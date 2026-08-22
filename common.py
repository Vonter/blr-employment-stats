"""Common pipeline helpers."""

from __future__ import annotations

import json
import os
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

RAW_ROOT = Path("raw")


@dataclass(frozen=True, order=True)
class CertificateKey:
    """A registration number, split into its parts.

    `circle` and `ward` are published as `office_number` and `ward_number`; the
    circle is the labour office that issued the certificate.
    """

    circle: int
    ward: int
    category: str
    seq: int
    year: int

    @property
    def number(self) -> str:
        return f"{self.circle}/{self.ward}/{self.category}/{self.seq:04d}/{self.year}"

    @property
    def group(self) -> tuple[int, int, int]:
        """The sequence this certificate is numbered within.

        CE and S share one counter per circle, ward and year: a number issued
        in one category is never reissued in the other.
        """
        return self.circle, self.ward, self.year

    def path(self, root: Path = RAW_ROOT) -> Path:
        return (
            root / str(self.circle) / str(self.ward) / self.category
            / str(self.year) / f"{self.seq:04d}.pdf"
        )

    @classmethod
    def parse(cls, value: str) -> CertificateKey | None:
        try:
            circle, ward, category, seq, year = value.split("/")
            return cls(int(circle), int(ward), category, int(seq), int(year))
        except (AttributeError, TypeError, ValueError):
            return None

    @classmethod
    def from_path(cls, path: Path) -> CertificateKey | None:
        try:
            return cls(
                int(path.parts[-5]), int(path.parts[-4]), path.parts[-3],
                int(path.stem), int(path.parts[-2]),
            )
        except (IndexError, ValueError):
            return None


def raw_pdf_keys(root: Path = RAW_ROOT) -> Iterator[tuple[CertificateKey, Path]]:
    for path in root.glob("*/*/*/*/*.pdf"):
        if key := CertificateKey.from_path(path):
            yield key, path


def certificate_keys(values: Iterable[str]) -> list[CertificateKey]:
    """Keys for the registration numbers that are well formed."""
    return [key for value in values if (key := CertificateKey.parse(value))]


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return default


def save_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        temporary.write_text(json.dumps(value, separators=(",", ":"), sort_keys=True))
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def atomic_parquet(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        frame.to_parquet(temporary, index=False)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def registration_ids(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return set(
        pd.read_parquet(path, columns=["registration_no"])["registration_no"]
        .dropna()
        .astype(str)
    )


def dataset(name: str) -> Path:
    """Prefer the parser's internal output, falling back to the published file."""
    internal = Path("debug") / f"{name}.parquet"
    return internal if internal.exists() else Path("data") / f"{name}.parquet"


def lapsed_registrations() -> list[str]:
    """Registrations whose coverage has run out, most recently lapsed first.

    A certificate still valid, or already renewed past today, cannot have an
    unseen renewal on the portal, so re-fetching it would find nothing new. A
    recent lapse is the likeliest to have been renewed since it was last seen.
    """
    employment = dataset("employment")
    if not employment.exists():
        return []
    frame = pd.read_parquet(employment, columns=["registration_no", "valid_upto"])
    covered = frame.dropna(subset=["registration_no"]).set_index("registration_no")
    columns = [covered["valid_upto"]]

    renewals = dataset("renewals")
    if renewals.exists():
        latest = (
            pd.read_parquet(renewals, columns=["registration_no", "renewal_date"])
            .groupby("registration_no")["renewal_date"]
            .max()
        )
        columns.append(latest.reindex(covered.index))

    coverage = pd.concat(columns, axis=1).max(axis=1)
    lapsed = coverage[coverage < pd.Timestamp.now()].sort_values(ascending=False)
    return lapsed.index.astype(str).tolist()


# Bootstrap seed for `fetch.py --backfill`; corrected against circle/ward pairs
# observed in the published dataset. Backfill unions this with the pairs already
# present in the data, so a stale entry cannot silently exclude a ward.
CIRCLE_WARD_MAP: dict[int, list[int]] = {
    1: [43, 44, 66, 67, 68, 76],
    2: [41, 70, 71],
    3: [61, 62, 63, 93],
    4: [78, 91, 92],
    5: [86, 113, 149, 150],
    6: [75, 99, 106, 107, 108],
    7: [94],
    8: [111, 115, 116],
    9: [109, 110],
    10: [74, 100, 101, 102, 105],
    11: [138, 139, 140, 142, 154],
    12: [103, 104, 123, 124, 125, 126],
    13: [166, 167, 168, 169, 170],
    14: [143, 146, 147, 152, 153],
    15: [51, 52, 53, 54, 55, 82, 84, 85],
    16: [95, 96, 97, 98, 121, 122],
    17: [118, 119, 145],
    18: [188, 192, 193],
    19: [80, 88, 89, 90, 112, 114],
    20: [109, 120],
    21: [148, 151],
    22: [172, 173, 174, 175],
    23: [27, 28, 29, 50],
    24: [3, 4, 8, 9, 11],
    25: [17, 36, 37, 38, 42],
    26: [141, 157, 158, 160, 161, 162],
    27: [14, 40],
    28: [184, 194, 196, 197],
    29: [54, 83],
    30: [69, 72, 73],
    31: [20, 34, 35, 46],
    32: [57, 58, 59, 79],
    33: [127, 128, 132, 133, 134, 135, 136, 137],
    34: [155, 156, 163, 164, 165],
    35: [144, 171, 176],
    36: [25, 26, 52],
    37: [51, 56, 81, 87],
    38: [189, 190, 191],
    39: [177, 178, 179, 187, 195],
    40: [12, 13, 15, 39],
    41: [111, 117],
    42: [24, 30, 31, 49],
    43: [23, 32, 47, 48, 60],
    44: [1, 2, 5, 6],
    45: [10, 16, 18, 19],
    46: [7, 21, 22, 33],
    47: [45, 64, 65, 77],
    48: [129, 130, 131, 159, 198],
    49: [180, 181, 182, 183, 185, 186],
}
