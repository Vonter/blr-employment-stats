"""Fetch e-Karmika registration certificates incrementally."""

from __future__ import annotations

import argparse
import csv
import logging
import os
import re
import shutil
import sys
import threading
import time
from collections import Counter
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from common import (
    CIRCLE_WARD_MAP,
    CertificateKey,
    certificate_keys,
    lapsed_registrations,
    load_json,
    raw_pdf_keys,
    registration_ids,
    save_json,
)

BASE_URL = (
    "https://www.ekarmika.karnataka.gov.in/ekarmika/"
    "UserCredentials/CertificateView.aspx"
)
POST_HEADERS = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Origin": "https://www.ekarmika.karnataka.gov.in",
    "Referer": BASE_URL,
}
NOT_FOUND = "Certificate not found"
STATE_FILE = Path("debug/fetch_state.json")
LOG_FILE = Path("debug/debug.log")
COVERAGE_FILE = Path("debug/fetch_failures.csv")

CATEGORIES = ("CE", "S")
CURRENT_YEAR = time.localtime().tm_year
EARLIEST_YEAR = 1990  # the data starts at 2000; backfill probes below that floor
MAX_SEQ = 9999
# Gap that ends a year's sequence, counted over the shared CE/S counter. The
# longest gap seen in the published data is 53, so this leaves a wide margin.
MAX_CONSECUTIVE_MISSING = 60
NEGATIVE_TTL = timedelta(hours=24)
INVALID_TTL = timedelta(days=30)
AUDIT_WIDTH = 120  # sequence numbers probed beyond a frontier per audit
AUDIT_BUDGET = 200  # audits per run
AUDIT_INTERVAL = timedelta(days=7)
PARALLEL = 3
DELAY = 0.5  # seconds between requests, across all workers
TIMEOUT = (10, 60)  # connect, read
CIRCUIT_BREAKER = 10  # consecutive failures that abort a run
MIN_FREE_GB = 50

logger = logging.getLogger(__name__)


def now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


def with_parts(entries: dict, count: int) -> dict:
    """Entries whose slash-separated name has `count` parts."""
    return {name: value for name, value in entries.items()
            if len(name.split("/")) == count}


class FetchState:
    """Small fetch-only cache, saved atomically as JSON."""

    def __init__(self, path: Path = STATE_FILE) -> None:
        self.path = path
        self.data = load_json(path, {})
        migrated = False
        for name, default in ("frontiers", {}), ("checks", {}), ("audits", {}):
            self.data.setdefault(name, default)
        # Frontiers and audits are now keyed by the shared CE/S counter, and
        # refreshes no longer round-robin. Drop keys left by the old layout.
        self.data.pop("refresh_cursor", None)
        self.data["frontiers"] = with_parts(self.data["frontiers"], 2)
        self.data["audits"] = with_parts(self.data["audits"], 3)
        # Older state treated invalid portal PDFs as immediately retryable,
        # causing every maintenance run to download the same bad responses.
        for entry in self.data["checks"].values():
            if entry.get("status") == "invalid" and not entry.get("retry"):
                checked = entry.get("checked")
                try:
                    retry = datetime.fromisoformat(checked) + INVALID_TTL
                except (TypeError, ValueError):
                    retry = now() + INVALID_TTL
                entry["retry"] = retry.isoformat()
                migrated = True
        self.lock = threading.RLock()
        self.dirty = int(migrated)

    def touch(self) -> None:
        self.dirty += 1
        if self.dirty >= 1000:
            self.flush()

    def flush(self) -> None:
        with self.lock:
            if self.dirty:
                save_json(self.path, self.data)
                self.dirty = 0

    def _found(self, key: CertificateKey) -> None:
        name = f"{key.circle}/{key.ward}"
        if [key.year, key.seq] > self.data["frontiers"].get(name, [0, 0]):
            self.data["frontiers"][name] = [key.year, key.seq]
        self.data["checks"].pop(key.number, None)

    def add(self, keys: Iterable[CertificateKey]) -> None:
        with self.lock:
            for key in keys:
                self._found(key)
            self.touch()

    def frontiers(self) -> dict[tuple[int, int], int]:
        """Latest year seen per circle/ward."""
        with self.lock:
            names = list(self.data["frontiers"].items())
        result = {}
        for name, (year, _seq) in names:
            circle, ward = name.split("/")
            result[(int(circle), int(ward))] = year
        return result

    def check(self, key: CertificateKey) -> dict:
        with self.lock:
            return self.data["checks"].get(key.number, {})

    def forget(self, key: CertificateKey) -> None:
        """Remove a cached result so an explicit request reaches the portal."""
        with self.lock:
            if self.data["checks"].pop(key.number, None) is not None:
                self.touch()

    def retry_due(self, key: CertificateKey) -> bool:
        entry = self.check(key)
        return not entry.get("retry") or entry["retry"] <= now().isoformat()

    def backoff(self, key: CertificateKey) -> timedelta:
        attempts = self.check(key).get("attempts", 0)
        return timedelta(minutes=min(2 ** min(attempts, 8), 360))

    def record(
        self,
        key: CertificateKey,
        status: str,
        error: str | None = None,
        retry_after: timedelta | None = None,
    ) -> None:
        if status == "found":
            self.add([key])
            return
        with self.lock:
            previous = self.data["checks"].get(key.number, {})
            self.data["checks"][key.number] = {
                "status": status,
                "error": error,
                "checked": now().isoformat(),
                "retry": (now() + retry_after).isoformat() if retry_after else None,
                "attempts": previous.get("attempts", 0) + 1,
            }
            self.touch()

    def retry_keys(
        self,
        statuses: frozenset[str] = frozenset(("transient", "invalid")),
    ) -> list[CertificateKey]:
        """Due failed requests.

        Expected ``missing`` probes are excluded: a complete backfill contains
        more than a million of them and retrying all of those is not a useful
        failure-recovery operation. Sweeps probe missing numbers again when
        needed after their negative-cache TTL.
        """
        with self.lock:
            entries = list(self.data["checks"].items())
        return [
            key
            for number, entry in entries
            if entry.get("status") in statuses
            and (key := CertificateKey.parse(number))
            and self.retry_due(key)
        ]

    def audit_band(self, group: tuple[int, int, int], latest: int) -> range | None:
        """Unexplored sequence numbers past a frontier, if this group is due."""
        name = "/".join(map(str, group))
        with self.lock:
            entry = self.data["audits"].get(name, {})
        checked = entry.get("checked")
        if checked and datetime.fromisoformat(checked) + AUDIT_INTERVAL > now():
            return None
        start = max(latest + MAX_CONSECUTIVE_MISSING + 1, entry.get("limit", 0) + 1)
        if start > MAX_SEQ:
            return None
        return range(start, min(start + AUDIT_WIDTH, MAX_SEQ + 1))

    def complete_audit(self, group: tuple[int, int, int], limit: int) -> None:
        name = "/".join(map(str, group))
        with self.lock:
            previous = self.data["audits"].get(name, {}).get("limit", 0)
            self.data["audits"][name] = {
                "limit": max(limit, previous),
                "checked": now().isoformat(),
            }
            self.touch()


class PortalUnavailable(RuntimeError):
    pass


class RateLimiter:
    def __init__(self, interval: float) -> None:
        self.interval = max(0.0, interval)
        self.next_request = 0.0
        self.requests = 0
        self.lock = threading.Lock()

    def wait(self) -> None:
        with self.lock:
            current = time.monotonic()
            slot = max(current, self.next_request)
            self.next_request = slot + self.interval
            self.requests += 1
        if slot > current:
            time.sleep(slot - current)


class CircuitBreaker:
    def __init__(self, threshold: int) -> None:
        self.threshold = threshold
        self.failures = 0
        self.lock = threading.Lock()

    def success(self) -> None:
        with self.lock:
            self.failures = 0

    def failure(self, message: str) -> None:
        with self.lock:
            self.failures += 1
            if self.failures >= self.threshold:
                raise PortalUnavailable(
                    f"Portal unavailable after {self.failures} failures: {message}"
                )


class PortalClient:
    """One reusable WebForms session."""

    def __init__(self, limiter: RateLimiter, breaker: CircuitBreaker) -> None:
        self.limiter = limiter
        self.breaker = breaker
        self.viewstate: tuple[str, str] | None = None
        self.session = requests.Session()
        self.session.mount(
            "https://",
            HTTPAdapter(
                max_retries=Retry(
                    total=3,
                    backoff_factor=0.8,
                    status_forcelist=(429, 500, 502, 503, 504),
                    allowed_methods=frozenset(("GET", "POST")),
                    respect_retry_after_header=True,
                )
            ),
        )

    def request(self, method: str, url: str, **kwargs) -> requests.Response:
        self.limiter.wait()
        try:
            response = self.session.request(method, url, timeout=TIMEOUT, **kwargs)
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            self.breaker.failure(str(exc))
            raise

    def prime(self) -> None:
        response = self.request("GET", BASE_URL, headers={"Referer": BASE_URL})
        self.viewstate = viewstate_of(soup(response.text))
        if not all(self.viewstate):
            raise RuntimeError("Portal response contains no WebForms state")

    def search(self, key: CertificateKey) -> tuple[bool, str]:
        if self.viewstate is None:
            self.prime()
        data = certificate_form(key.number, *self.viewstate)
        data["ctl00$MainContent$btn_Search"] = "View"
        page = soup(self.post(data).text)
        viewstate = viewstate_of(page)
        self.viewstate = viewstate if all(viewstate) else None

        found, status = search_result(page)
        if found or status == NOT_FOUND:
            self.breaker.success()
        else:
            self.breaker.failure(status)
        return found, status

    def view(self, key: CertificateKey) -> requests.Response:
        if self.viewstate is None:
            raise RuntimeError("Search state was lost")
        data = certificate_form(
            key.number,
            *self.viewstate,
            "ctl00$MainContent$GRD_EstDetails$ctl02$lbtn_view",
        )
        response = self.post(data, stream=True)
        self.viewstate = None
        return response

    def post(self, data: dict[str, str], **kwargs) -> requests.Response:
        return self.request("POST", BASE_URL, data=data, headers=POST_HEADERS, **kwargs)


def viewstate_of(page: BeautifulSoup) -> tuple[str, str]:
    def value(element_id: str) -> str:
        element = page.find("input", id=element_id)
        return element.get("value", "") if element else ""

    return value("__VIEWSTATE"), value("__VIEWSTATEGENERATOR")


def certificate_form(
    number: str, viewstate: str, generator: str, event_target: str = ""
) -> dict[str, str]:
    return {
        "__EVENTTARGET": event_target,
        "__EVENTARGUMENT": "",
        "__VIEWSTATE": viewstate,
        "__VIEWSTATEGENERATOR": generator,
        "ctl00$username": "",
        "ctl00$password1": "",
        "ctl00$MainContent$txt_cerficate": number,
    }


def search_result(page: BeautifulSoup) -> tuple[bool, str]:
    """Whether the search hit, plus the portal's message when it did not."""
    if NOT_FOUND.lower() in page.get_text(" ", strip=True).lower():
        return False, NOT_FOUND
    grid = page.find(id=re.compile(r"GRD_EstDetails", re.IGNORECASE))
    if grid and any(row.find("td") for row in grid.find_all("tr")):
        return True, "found"
    messages = page.find_all(
        ("span", "label", "div"), id=re.compile(r"lbl|msg|err|alert", re.IGNORECASE)
    )
    message = next((node.get_text(" ", strip=True) for node in messages if node.text), "")
    return False, message or "unrecognized portal response"


def pdf_link(response: requests.Response) -> str | None:
    """URL of the PDF behind a viewer page, or None if this response is the PDF."""
    if (
        "application/pdf" in response.headers.get("Content-Type", "")
        or response.content[:4] == b"%PDF"
    ):
        return None
    page = soup(response.text)
    tag = page.find(
        ("iframe", "embed"), src=re.compile(r"\.pdf", re.IGNORECASE)
    ) or page.find("a", href=re.compile(r"\.pdf", re.IGNORECASE))
    source = tag and (tag.get("src") or tag.get("href"))
    if not source:
        raise ValueError("certificate view contains no PDF")
    return urljoin(BASE_URL, source)


def save_pdf(response: requests.Response, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".part")
    try:
        with temporary.open("wb") as handle:
            first = True
            for chunk in response.iter_content(1024 * 1024):
                if not chunk:
                    continue
                if first and chunk[:4] != b"%PDF":
                    raise ValueError("response is not a PDF")
                first = False
                handle.write(chunk)
            if first:
                raise ValueError("empty PDF response")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def holds_certificate(path: Path, key: CertificateKey | None = None) -> bool:
    # Imported lazily: the parser pulls in pandas and pdfplumber.
    import pdfplumber

    from parse import extract_registration, is_certificate

    try:
        # Every page, as the parser reads it: the signature that marks a
        # non-certificate can sit past the first page.
        with pdfplumber.open(path) as document:
            text = "\n".join(page.extract_text() or "" for page in document.pages)
        if not is_certificate(text):
            return False
        printed = extract_registration(text)["registration_no"]
        # The path remains authoritative when a scan's number is unreadable,
        # but a different readable number is a wrong portal response.
        return key is None or printed is None or printed == key.number
    except Exception:  # noqa: BLE001 - PDF backends vary.
        return False


class Runner:
    """Runs fetch tasks over a thread pool, tracking what has been found."""

    def __init__(
        self,
        state: FetchState,
        known: set[str],
        min_free_gb: float = MIN_FREE_GB,
    ) -> None:
        self.state = state
        self.known = known
        self.seen: dict[tuple[int, int, int], int] = {}
        for key in certificate_keys(known):
            self._mark(key)
        self.limiter = RateLimiter(DELAY)
        self.breaker = CircuitBreaker(CIRCUIT_BREAKER)
        self.local = threading.local()
        self.lock = threading.Lock()
        self.stats: Counter[str] = Counter()
        self.audit_slots = AUDIT_BUDGET
        self.min_free_gb = min_free_gb

    def _mark(self, key: CertificateKey) -> None:
        self.known.add(key.number)
        self.seen[key.group] = max(key.seq, self.seen.get(key.group, 0))

    def client(self) -> PortalClient:
        if not hasattr(self.local, "client"):
            self.local.client = PortalClient(self.limiter, self.breaker)
        return self.local.client

    def is_found(self, key: CertificateKey) -> bool:
        return key.number in self.known or key.path().exists()

    def fetch(self, key: CertificateKey, force: bool = False) -> str:
        status = self._fetch(key, force)
        with self.lock:
            self.stats[status] += 1
            if status == "found":
                self._mark(key)
        logger.info("FETCH %s status=%s", key.number, status)
        return status

    def _fetch(self, key: CertificateKey, force: bool) -> str:
        """Download one certificate. `force` re-fetches over an existing PDF."""
        path = key.path()
        if not force:
            if path.exists():
                self.state.add([key])
                return "found"
            if not self.state.retry_due(key):
                return self.state.check(key).get("status", "missing")

        client = self.client()
        try:
            found, status = client.search(key)
            if not found:
                if status != NOT_FOUND:
                    self.state.record(key, "transient", status, self.state.backoff(key))
                    return "transient"
                if force:
                    # The portal lost it; keep the copy already downloaded.
                    self.state.record(key, "found")
                    return "transient"
                self.state.record(key, "missing", status, NEGATIVE_TTL)
                return "missing"

            response = client.view(key)
            link = pdf_link(response)
            if link:
                response = client.request(
                    "GET", link, headers={"Referer": BASE_URL}, stream=True
                )

            free_gb = shutil.disk_usage(".").free / (1024**3)
            if free_gb < self.min_free_gb:
                raise RuntimeError(f"only {free_gb:.1f} GB free")

            destination = path.with_suffix(".refresh.pdf") if force else path
            save_pdf(response, destination)
            if not holds_certificate(destination, key):
                destination.unlink(missing_ok=True)
                self.state.record(
                    key,
                    "found" if force else "invalid",
                    f"not a registration certificate{f': {link}' if link else ''}",
                    INVALID_TTL if not force else None,
                )
                return "transient" if force else "invalid"
            if force:
                os.replace(destination, path)
        except PortalUnavailable:
            raise
        except (requests.RequestException, RuntimeError, ValueError) as exc:
            client.viewstate = None
            self.state.record(
                key,
                "found" if force else "transient",
                str(exc),
                self.state.backoff(key),
            )
            logger.warning("%s — %s", key.number, exc)
            return "transient"

        self.state.record(key, "found")
        logger.info("SAVED %s", path)
        return "found"

    def probe(self, group: tuple[int, int, int], seq: int) -> set[str]:
        """Status of one sequence number in every category it could hold."""
        circle, ward, year = group
        return {
            "found" if self.is_found(key) else self.fetch(key)
            for key in (CertificateKey(circle, ward, category, seq, year)
                        for category in CATEGORIES)
        }

    def sweep(self, group: tuple[int, int, int], start: int) -> None:
        """Fetch consecutive sequence numbers until the year runs out.

        CE and S share the counter, so a number only widens the gap when it was
        issued in neither category.
        """
        missing = 0
        for seq in range(start, MAX_SEQ + 1):
            statuses = self.probe(group, seq)
            if "found" in statuses:
                missing = 0
            elif statuses == {"missing"}:
                missing += 1
                if missing >= MAX_CONSECUTIVE_MISSING:
                    return
            else:
                logger.warning("PAUSE %s at %s after %s", group, seq, statuses)
                return

    def audit(self, group: tuple[int, int, int]) -> None:
        """Probe past the frontier for certificates hidden behind a long gap."""
        band = self.state.audit_band(group, self.seen.get(group, 0))
        if not band or not self.take_audit_slot():
            return
        if all(self.probe(group, seq) <= {"found", "missing"} for seq in band):
            self.state.complete_audit(group, band.stop - 1)

    def take_audit_slot(self) -> bool:
        with self.lock:
            if self.audit_slots <= 0:
                return False
            self.audit_slots -= 1
            return True

    def extend(self, item: tuple[tuple[int, int], int]) -> None:
        """Catch a circle/ward up from its latest year to today.

        Years before the frontier are settled and are only revisited by
        `--backfill`.
        """
        (circle, ward), latest_year = item
        for year in range(latest_year, CURRENT_YEAR + 1):
            group = (circle, ward, year)
            self.sweep(group, self.seen.get(group, 0) + 1)
            self.audit(group)

    def backfill(self, group: tuple[int, int, int]) -> None:
        self.sweep(group, 1)
        self.audit(group)

    def retry(self, key: CertificateKey) -> str:
        # An invalid PDF is on disk and has to be overwritten to be replaced.
        invalid = self.state.check(key).get("status") == "invalid"
        if invalid and key.path().exists() and not holds_certificate(key.path(), key):
            # A past version could cache the portal's generic SOP manual under
            # a certificate filename. It must not protect that file as a known
            # good copy during the forced-refresh failure path.
            key.path().unlink()
        return self.fetch(key, force=invalid and key.path().exists())

    def refresh(self, key: CertificateKey) -> str:
        # Forcing only matters over an existing PDF; without one this is a
        # plain fetch, which is how a clone without the raw archive catches up.
        if not key.path().exists():
            # An explicit refresh must bypass even a recent negative-cache hit.
            self.state.forget(key)
        return self.fetch(key, force=key.path().exists())

    def run(self, tasks: Iterable, function) -> None:
        errors = []
        with ThreadPoolExecutor(max_workers=PARALLEL) as pool:
            futures = {pool.submit(function, task): task for task in tasks}
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as exc:  # noqa: BLE001 - report after workers stop.
                    logger.error("%s failed: %s", futures[future], exc)
                    errors.append(exc)
        if errors:
            raise RuntimeError(f"{len(errors)} worker(s) failed") from errors[0]

    def report(self) -> None:
        summary = " ".join(f"{name}={count}" for name, count in sorted(self.stats.items()))
        logger.info(
            "Run summary: requests=%s%s",
            self.limiter.requests,
            f" {summary}" if summary else "",
        )


def setup_logging() -> None:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            RotatingFileHandler(LOG_FILE, maxBytes=10_000_000, backupCount=3),
            logging.StreamHandler(sys.stdout),
        ],
    )


def write_failure_report(
    state: FetchState,
    path: Path = COVERAGE_FILE,
) -> None:
    """Publish unresolved fetches without the much larger expected-missing cache."""
    fields = ("registration_no", "status", "attempts", "checked", "retry", "error")
    rows = [
        {"registration_no": number, **{field: entry.get(field) for field in fields[1:]}}
        for number, entry in sorted(state.data["checks"].items())
        if entry.get("status") in {"transient", "invalid"}
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        with temporary.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def backfill_groups(known: set[str]) -> list[tuple[int, int, int]]:
    """Every mapped or observed circle/ward, across all years.

    The seed map is hand-maintained and has drifted from the portal before, so
    observed pairs are unioned in to keep a stale entry from hiding a ward.
    """
    pairs = {
        (circle, ward)
        for circle, wards in CIRCLE_WARD_MAP.items()
        for ward in wards
    }
    pairs.update((key.circle, key.ward) for key in certificate_keys(known))
    return [
        (circle, ward, year)
        for circle, ward in sorted(pairs)
        for year in range(CURRENT_YEAR, EARLIEST_YEAR - 1, -1)
    ]


def refreshable() -> list[CertificateKey]:
    return certificate_keys(lapsed_registrations())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch e-Karmika labour registration certificates. Without "
        "options, extends each known sequence to the current year.",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--backfill",
        action="store_true",
        help=f"sweep every circle/ward from {CURRENT_YEAR} back to "
        f"{EARLIEST_YEAR}, then re-fetch every lapsed certificate",
    )
    mode.add_argument(
        "--retry",
        action="store_true",
        help="retry transient and invalid certificate fetches that are due again",
    )
    mode.add_argument(
        "--index",
        action="store_true",
        help="rescan the raw archive into the fetch state, then exit",
    )
    mode.add_argument(
        "--certificate",
        dest="certificates",
        action="append",
        type=certificate_argument,
        metavar="REGISTRATION_NO",
        help="refetch one certificate from the live portal; may be repeated",
    )
    parser.add_argument(
        "--min-free-gb",
        type=float,
        default=MIN_FREE_GB,
        help=f"abort downloads below this much free disk space (default: {MIN_FREE_GB})",
    )
    return parser.parse_args()


def certificate_argument(value: str) -> CertificateKey:
    key = CertificateKey.parse(value)
    if not key or key.category not in CATEGORIES:
        raise argparse.ArgumentTypeError(
            "expected office/ward/CE-or-S/sequence/year"
        )
    return key


def main() -> None:
    args = parse_args()
    setup_logging()

    state = FetchState()
    known = registration_ids(Path("data/employment.parquet"))
    known |= registration_ids(Path("debug/employment.parquet"))
    state.add(certificate_keys(known))
    if args.index or not state.data.get("raw_indexed"):
        archived = []
        for key, path in raw_pdf_keys():
            if key.number in known or holds_certificate(path, key):
                archived.append(key)
            else:
                state.record(
                    key,
                    "invalid",
                    "non-certificate PDF already present in raw archive",
                    INVALID_TTL,
                )
                logger.warning("IGNORED %s — raw PDF is not a certificate", key.number)
        state.add(archived)
        known.update(key.number for key in archived)
        state.data["raw_indexed"] = now().isoformat()
        state.touch()

    runner = Runner(state, known, min_free_gb=args.min_free_gb)
    try:
        if args.index:
            return
        if args.certificates:
            runner.run(args.certificates, runner.refresh)
        elif args.retry:
            runner.run(state.retry_keys(), runner.retry)
        elif args.backfill:
            runner.run(backfill_groups(known), runner.backfill)
            runner.run(refreshable(), runner.refresh)
        else:
            runner.run(state.frontiers().items(), runner.extend)
    finally:
        runner.report()
        state.flush()
        write_failure_report(state)


if __name__ == "__main__":
    main()
