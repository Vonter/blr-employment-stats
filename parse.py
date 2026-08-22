"""Parse downloaded certificate PDFs into datasets."""

from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import pdfplumber

from common import (
    CertificateKey,
    atomic_parquet,
    load_json,
    raw_pdf_keys,
    registration_ids,
    save_json,
)

RAW_DIR = Path("raw")
DATA_DIR = Path("data")
DEBUG_DIR = Path("debug")
FAILURE_FILE = DEBUG_DIR / "parse_failures.json"
# Amendments are kept internally only; the rest are published.
DATASETS = ("employment", "renewals", "amendments")
PUBLISHED = ("employment", "renewals")
NON_CERTIFICATE_SIGNATURES = (
    "ONLINE SERVICES PROCEDURE/SOP/Checklist",
    "NEW REGISTRATION WORK-FLOW",
)
CRITICAL_FIELDS = ("registration_no", "establishment_name", "registration_date")
TAIL_COLUMNS = ["reg_year", "ward_number", "office_number", "office"]
INT_COLUMNS = [
    "employees_male", "employees_female", "employees_total", "reg_year", "year",
]
CERTIFICATE_COLUMNS = [
    "registration_no", "office_number", "ward_number", "establishment_type", "reg_year",
    "establishment_name", "employer_name", "nature_of_business", "address", "phone",
    "fax", "email", "employees_male", "employees_female", "employees_total",
    "registration_fee_rs", "fee_transaction_no", "fee_payment_date",
    "registration_date", "valid_upto", "office",
]
DROP_COLUMNS = {
    "employment": ["employer_name", "phone", "fax", "email", "fee_transaction_no"],
    "renewals": ["fee_amount_rs", "challan_no", "challan_date", "valid_upto", "office"],
}
# Deduplication keys and sort column per dataset.
MERGE_KEYS = {
    "employment": (["registration_no"], "registration_date"),
    "renewals": (["registration_no", "year"], "renewal_date"),
    "amendments": (["registration_no", "sl_no", "section"], None),
}


def internal(name: str) -> Path:
    return DEBUG_DIR / f"{name}.parquet"


def published(name: str) -> Path:
    return DATA_DIR / f"{name}.parquet"


def is_kannada_line(text: str) -> bool:
    return any("ಀ" <= c <= "೿" for c in text) or bool(re.search(r"\(cid:\d+\)", text))


def extract_english(block: str, skip_labels: set[str] | None = None) -> str:
    """English lines from a mixed Kannada/English block, skipping exact label strings."""
    skip = skip_labels or set()
    lines = (line.strip() for line in block.splitlines())
    return " ".join(line for line in lines
                    if line and not is_kannada_line(line) and line not in skip)


def extract_until_kannada(block: str) -> str:
    """English lines from a block, stopping at the first Kannada line."""
    lines: list[str] = []
    for line in block.splitlines():
        line = line.strip()
        if not line:
            continue
        if is_kannada_line(line):
            break
        lines.append(line)
    return " ".join(lines).strip()


def clean_cell(s: str | None) -> str | None:
    """Strip Kannada lines from a table cell and normalize whitespace."""
    if s is None:
        return None
    lines = [line.strip() for line in s.splitlines()
             if line.strip() and not is_kannada_line(line.strip())]
    return " ".join(lines).strip() or None


def parse_date(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.strptime(s.strip(), "%d-%m-%Y")  # noqa: DTZ007
    except ValueError:
        return None


def parse_date_slash(s: str | None) -> datetime | None:
    """Parse DD/MM/YYYY, falling back to DD-MM-YYYY."""
    if not s:
        return None
    try:
        return datetime.strptime(s.strip(), "%d/%m/%Y")  # noqa: DTZ007
    except ValueError:
        return parse_date(s)


def normalized_match(pattern: str, text: str, flags: int = 0) -> str | None:
    match = re.search(pattern, text, flags)
    return " ".join(match.group(1).split()) if match else None


def extract_registration(text: str) -> dict:
    match = re.search(r"(\d+\s*/\s*\d+\s*/\s*[A-Z]+\s*/\s*\d+\s*/\s*\d{4})", text)
    registration_no = re.sub(r"\s+", "", match.group(1)) if match else None
    if not registration_no:
        return {
            "registration_no": None,
            "office_number": None,
            "ward_number": None,
            "establishment_type": None,
            "reg_year": None,
        }

    office, ward, category, _, year = registration_no.split("/")
    return {
        "registration_no": registration_no,
        "office_number": office,
        "ward_number": ward,
        "establishment_type": category,
        "reg_year": int(year),
    }


def extract_establishment(text: str) -> dict:
    employer = re.search(
        r"Name of the Employer\.\s*\n(.*?)(?=Nature of Business)",
        text,
        re.DOTALL,
    )
    if employer:
        employer_name = extract_english(employer.group(1)) or None
    else:
        employer_name = normalized_match(r"Name of the Employer\.\s+(.+)", text)

    nature = re.search(
        r"Nature of Business\s+(.+?)(?=\nPostal Address of the)",
        text,
        re.DOTALL,
    )
    if nature:
        nature_of_business = extract_until_kannada(nature.group(1))
    else:
        nature_of_business = normalized_match(r"Nature of Business\s+(.+)", text)

    address = re.search(
        r"Postal Address of the\s+(.*?)(?=Telephone / Mobile No\.)",
        text,
        re.DOTALL,
    )
    return {
        "establishment_name": normalized_match(r"Name of the Establishment\s+(.+)", text),
        "employer_name": employer_name,
        "nature_of_business": nature_of_business,
        "address": (
            extract_english(address.group(1), skip_labels={"Establishment"})
            if address else None
        ),
    }


def extract_contact(text: str) -> dict:
    phone = re.search(r"Telephone / Mobile No\.\s+(\d+)", text)
    fax_match = re.search(r"Fax\s+\([^)]*\)\s*\n(.+?)\nE-Mail", text)
    fax = fax_match.group(1).strip() if fax_match else ""
    email = re.search(r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b", text)
    return {
        "phone": phone.group(1) if phone else None,
        "fax": fax if fax and not is_kannada_line(fax) else None,
        "email": email.group(0) if email else None,
    }


def extract_employment(text: str) -> dict:
    # Some certificates populate only one of the three cells. Preserve every
    # printed value without treating a blank cell as zero or inferring a total.
    match = re.search(
        r"M\s*:\s*(\d*)[^\n]*?F\s*:\s*(\d*)[^\n]*?Total\s*:\s*(\d*)",
        text,
    )
    values = (
        (int(value) if value else None for value in match.groups())
        if match else (None, None, None)
    )
    return dict(zip(
        ("employees_male", "employees_female", "employees_total"),
        values,
    ))


FEE_FIELDS = ("registration_fee_rs", "fee_transaction_no", "fee_payment_date")
FEE_LABEL = r"Registration Fee Paid[ \t]*R[ \t]*s\.?"
# ':\s' skips the colon inside the '(cid:NNNN)' glyph markers on the same line.
FEE_BLOCK = re.compile(
    r"([\d,]+\.\d{2})[^\n]*?:\s([^\n]*)\n[^\n]*No\. & Date[^\n]*\n"
    rf"[^\n]*?{FEE_LABEL}[ \t]*([^\n]*)"
)
# Two-column layouts interleave the columns, putting the label before its values.
FEE_INLINE = re.compile(rf"{FEE_LABEL}[ \t]*([\d,]+\.\d{{2}})[ \t]*([^\n]*)")


def extract_fee(text: str) -> dict:
    """Fee, transaction number and payment date from the fee block.

    The transaction number may be blank, and the payment date may wrap onto the
    line below after either its day or its month.
    """
    if match := FEE_BLOCK.search(text):
        amount, tail, continuation = match.groups()
    elif match := FEE_INLINE.search(text):
        amount, tail, continuation = *match.groups(), ""
    else:
        return dict.fromkeys(FEE_FIELDS)
    transaction, _, day = tail.rpartition(",")
    return {
        "registration_fee_rs": float(amount.replace(",", "")),
        "fee_transaction_no": transaction.strip() or None,
        "fee_payment_date": parse_date(day.strip() + continuation.strip()),
    }


CID_GLYPH = re.compile(r"\(cid:\d+\)")
OFFICE_CIRCLE = (
    re.compile(r"Labour Inspector[ ,-]*[Cc]ircle[ ,-]*(\d+)"),  # '... - Circle 15'
    re.compile(r"Labour Inspector[ ,-]*[Cc]ircle *\n[^\n]*?(\d+)"),  # number wraps
    re.compile(r"(\d+)(?:st|nd|rd|th) +Circle"),  # '20th Circle, Karmika Bhavana'
)
OFFICE_LINE = re.compile(r'Office of[^"\n]*Labour Inspector[^"\n]*')


def extract_office(header: str) -> str | None:
    """Issuing office from the certificate header.

    Layouts differ in where the circle number lands: alongside the office name,
    on the Kannada line below a bare 'circle', or in the postal address.
    """
    text = CID_GLYPH.sub("", header)
    for pattern in OFFICE_CIRCLE:
        if match := pattern.search(text):
            return f"Office of Senior Labour Inspector - Circle {match.group(1)}"
    line = OFFICE_LINE.search(text)
    return " ".join(line.group(0).split()) if line else None


def extract_validity(text: str) -> dict:
    match = re.search(
        r"On\s+(\d{2}-\d{2}-\d{4})\.?\s+The\s+Registration\s+is valid upto\s+(\d{2}-\d{2}-\d{4})",
        text,
        re.DOTALL,
    )
    return {
        "registration_date": parse_date(match.group(1)) if match else None,
        "valid_upto": parse_date(match.group(2)) if match else None,
    }


def extract_fields(text: str, header: str) -> dict:
    """Extract one registration-certificate record.

    `header` is the first page carrying text; renewal and amendment pages repeat
    the office line and would otherwise overwrite the issuing office.
    """
    text = re.sub(r"(\d{2}-\d{2})-\s*\n\s*(\d{4})", r"\1-\2", text)
    fields: dict = {"office": extract_office(header)}
    for extractor in (
        extract_registration,
        extract_establishment,
        extract_contact,
        extract_employment,
        extract_fee,
        extract_validity,
    ):
        fields.update(extractor(text))
    return fields


def is_certificate(text: str) -> bool:
    return (
        not any(sig in text for sig in NON_CERTIFICATE_SIGNATURES)
        and "Name of the Establishment" in text
        and "Registration Fee Paid" in text
    )


def _parse_fee_challan(cell: str) -> tuple[float | None, str | None, datetime | None]:
    """Parse the renewal fee cell: 'AMOUNT, CHALLAN_NO, DATE' (challan may line-wrap)."""
    parts = cell.split(",")
    amount = challan_no = challan_date = None

    if parts:
        try:
            amount = float(re.sub(r"[^\d.]", "", parts[0]))
        except ValueError:
            pass
    if len(parts) >= 2:
        challan_raw = ",".join(parts[1:-1]) if len(parts) > 2 else parts[1]
        challan_no = re.sub(r"\s+", "", challan_raw) or None
    if len(parts) >= 3:
        challan_date = parse_date_slash(parts[-1].strip())

    return amount, challan_no, challan_date


def _parse_renewal_table(table: list, registration_no: str) -> list[dict]:
    """Extract renewal records from a table that contains a Renewal section."""
    cols: dict[str, int | None] = dict.fromkeys(
        ["year", "renewal_date", "employees", "fee", "expiry", "office"])

    for row in table:
        cells = [clean_cell(c) for c in row]
        if any(c and "Year" in c for c in cells):
            for i, c in enumerate(cells):
                if not c:
                    continue
                if "Year" in c and cols["year"] is None:
                    cols["year"] = i
                elif "Renewal" in c and "Date" in c:
                    cols["renewal_date"] = i
                elif "persons" in c or "employed" in c:
                    cols["employees"] = i
                elif "Challan" in c or "Fee paid" in c:
                    cols["fee"] = i
                elif "expiry" in c:
                    cols["expiry"] = i
                elif "Signature" in c or "Inspector" in c:
                    cols["office"] = i
            break

    if cols["year"] is None:
        return []

    records: list[dict] = []
    pending: list[dict] = []

    for row in table:
        if cols["year"] >= len(row):
            continue

        def get(key: str, row: list = row) -> str | None:
            col = cols[key]
            return clean_cell(row[col]) if col is not None and col < len(row) else None

        year_cell = get("year")
        if year_cell and re.match(r"^\d{4}$", year_cell.strip()):
            emp_raw = get("employees")
            try:
                employees_total = int(emp_raw) if emp_raw else None
            except ValueError:
                employees_total = None

            fee_raw = row[cols["fee"]] if cols["fee"] is not None and cols["fee"] < len(row) else None
            fee_amount, challan_no, challan_date = (
                _parse_fee_challan(fee_raw) if fee_raw else (None, None, None))

            office_val = get("office")
            office = office_val if office_val and "Office" in office_val else None

            rec = {
                "registration_no": registration_no,
                "year": int(year_cell),
                "renewal_date": parse_date(get("renewal_date")),
                "employees_total": employees_total,
                "fee_amount_rs": fee_amount,
                "challan_no": challan_no,
                "challan_date": challan_date,
                "valid_upto": parse_date(get("expiry")),
                "office": office,
            }
            records.append(rec)
            if office is None:
                pending.append(rec)

        elif pending:
            for cell in row:
                cv = clean_cell(cell)
                if cv and "Office" in cv:
                    for r in pending:
                        r["office"] = cv
                    pending.clear()
                    break

    return records


def _parse_amendment_table(table: list, registration_no: str) -> list[dict]:
    """Extract amendment records from a table."""
    if not table:
        return []

    ncols = max(len(row) for row in table)
    if ncols >= 13:
        ci = {"sl": 0, "type": 1, "section": 3, "before": 5, "after": 7,
              "date_sub": 9, "date_appr": 11, "office": 12}
    else:
        ci = {"sl": 0, "type": 1, "section": 2, "before": 3, "after": 4,
              "date_sub": 5, "date_appr": 6, "office": 7}

    def get(row: list, key: str) -> str | None:
        i = ci[key]
        return clean_cell(row[i]) if i < len(row) else None

    start = 0
    for i, row in enumerate(table):
        sl_raw = get(row, "sl")
        if sl_raw and "Sl." in sl_raw:
            start = i + 1
            break
        if sl_raw and re.match(r"^\d+$", sl_raw) and int(sl_raw) < 1000:
            start = i
            break

    records: list[dict] = []
    pending: list[dict] = []
    sl_no = amd_type = date_sub = date_appr = office = None

    def apply_office(off: str) -> None:
        for r in pending:
            if r["office"] is None:
                r["office"] = off
        pending.clear()

    def add_record(row: list) -> None:
        section = get(row, "section")
        if not section:
            return
        rec = {
            "registration_no": registration_no,
            "sl_no": sl_no,
            "type": amd_type,
            "section": section,
            "before_amendment": get(row, "before"),
            "after_amendment": get(row, "after"),
            "date_submission": date_sub,
            "date_approval": date_appr,
            "office": office,
        }
        records.append(rec)
        if office is None:
            pending.append(rec)

    for row in table[start:]:
        sl_raw = get(row, "sl")

        if sl_raw and re.match(r"^\d+$", sl_raw.strip()) and int(sl_raw) < 1000:
            sl_no = int(sl_raw)
            amd_type = get(row, "type") or "Amendment"
            date_sub = parse_date(get(row, "date_sub"))
            date_appr = parse_date(get(row, "date_appr"))
            office_val = get(row, "office")
            if office_val and "Office" in office_val:
                office = office_val
                apply_office(office)
            else:
                office = None
            add_record(row)

        elif sl_no is not None:
            office_val = get(row, "office")
            if office_val and "Office" in office_val:
                office = office_val
                apply_office(office)
                continue
            add_record(row)

    return records


def extract_supplementary(
    pdf: pdfplumber.PDF,
    page_texts: list[str],
    registration_no: str,
) -> tuple[list[dict], list[dict]]:
    """Extract supplementary records from relevant pages."""
    renewals: list[dict] = []
    amendments: list[dict] = []

    for page, text in zip(pdf.pages, page_texts):
        if "Renewal" not in text and "Amendment" not in text:
            continue
        for table in page.extract_tables():
            if not table:
                continue
            table_text = " ".join(str(cell or "") for row in table for cell in row)
            if "Renewal" in table_text and "Registration No." in table_text:
                # Keyed by path, not by the number printed in the table: the two
                # disagree on a few scans, which would orphan the renewal.
                renewals.extend(_parse_renewal_table(table, registration_no))
            if "Amendment" in text or "Amendment" in table_text:
                amendments.extend(_parse_amendment_table(table, registration_no))

    return renewals, amendments


def parse_document(
    pdf_path: Path,
    key: CertificateKey,
) -> tuple[dict, list[dict], list[dict], str | None]:
    """Read a PDF once and extract every dataset, or explain why it was rejected."""
    def rejected(reason: str) -> tuple[dict, list, list, str]:
        return {}, [], [], reason

    try:
        with pdfplumber.open(pdf_path) as pdf:
            page_texts = [page.extract_text() or "" for page in pdf.pages]
            text = "\n".join(page_texts)
            if not is_certificate(text):
                return rejected("not a registration certificate")
            header = next((page for page in page_texts if page.strip()), "")
            record = extract_fields(text, header)
            renewals, amendments = extract_supplementary(pdf, page_texts, key.number)
    except Exception as e:  # noqa: BLE001 - PDF backends raise heterogeneous errors.
        print(f"ERROR {pdf_path}: {e}", file=sys.stderr)
        return rejected("text extraction failed")

    if record["registration_no"] is None:
        # Unreadable on some scans; the path it was fetched under is authoritative.
        record.update(extract_registration(key.number))
    if missing := [field for field in CRITICAL_FIELDS if record.get(field) is None]:
        return rejected(f"missing field(s): {', '.join(missing)}")
    if record["registration_no"] != key.number:
        return rejected(
            f"PDF contains {record['registration_no']}, expected {key.number}")
    return record, renewals, amendments, None


# Mapping of BBMP (old) ward number to ward name, derived from:
# https://raw.githubusercontent.com/Vonter/city-officials/refs/heads/main/static/blr/boundaries_bbmp_wards_old.json
WARD_NAMES = {
    "1": "Kempegowda Ward",
    "2": "Chowdeswari Ward",
    "3": "Atturu",
    "4": "Yelahanka Satellite Town",
    "5": "Jakkuru",
    "6": "Thanisandra",
    "7": "Byatarayanapura",
    "8": "Kodigehalli",
    "9": "Vidyaranyapura",
    "10": "Dodda Bommasandra",
    "11": "Kuvempu Nagar",
    "12": "Shettihalli",
    "13": "Mallasandra",
    "14": "Bagalakunte",
    "15": "T Dasarahalli",
    "16": "Jalahalli",
    "17": "J P Park",
    "18": "Radhakrishna Temple Ward",
    "19": "Sanjaya Nagar",
    "20": "Ganga Nagar",
    "21": "Hebbala",
    "22": "Vishwanath Nagenahalli",
    "23": "Nagavara",
    "24": "HBR Layout",
    "25": "Horamavu",
    "26": "Ramamurthy Nagar",
    "27": "Banasavadi",
    "28": "Kammanahalli",
    "29": "Kacharkanahalli",
    "30": "Kadugondanahalli",
    "31": "Kushal Nagar",
    "32": "Kaval Bairasandra",
    "33": "Manorayanapalya",
    "34": "Gangenahalli",
    "35": "Aramane Nagara",
    "36": "Mattikere",
    "37": "Yeshwanthpura",
    "38": "HMT Ward",
    "39": "Chokkasandra",
    "40": "Dodda Bidarakallu",
    "41": "Peenya Industrial Area",
    "42": "Lakshmi Devi Nagar",
    "43": "Nandini Layout",
    "44": "Marappana Palya",
    "45": "Malleswaram",
    "46": "Jayachamarajendra Nagar",
    "47": "Devara Jeevanahalli",
    "48": "Muneshwara Nagar",
    "49": "Lingarajapura",
    "50": "Benniganahalli",
    "51": "Vijnanapura",
    "52": "K R Puram",
    "53": "Basavanapura",
    "54": "Hudi",
    "55": "Devasandra",
    "56": "A Narayanapura",
    "57": "C V Raman Nagar",
    "58": "New Tippasandara",
    "59": "Maruthi Seva Nagar",
    "60": "Sagayarapuram",
    "61": "S K Garden",
    "62": "Ramaswamy Palya",
    "63": "Jayamahal",
    "64": "Rajamahal Guttahalli",
    "65": "Kadu Malleshwar Ward",
    "66": "Subramanya Nagar",
    "67": "Nagapura",
    "68": "Mahalakshimpuram",
    "69": "Laggere",
    "70": "Rajagopal Nagar",
    "71": "Hegganahalli",
    "72": "Herohalli",
    "73": "Kottegepalya",
    "74": "Shakthi Ganapathi Nagar",
    "75": "Shankar Matt",
    "76": "Gayithri Nagar",
    "77": "Dattatreya Temple",
    "78": "Pulikeshinagar",
    "79": "Sarvagna Nagar",
    "80": "Hoysala Nagar",
    "81": "Vijnana Nagar",
    "82": "Garudachar Playa",
    "83": "Kadugodi",
    "84": "Hagadur",
    "85": "Dodda Nekkundi",
    "86": "Marathahalli",
    "87": "HAL Airport",
    "88": "Jeevanbhima Nagar",
    "89": "Jogupalya",
    "90": "Halsoor",
    "91": "Bharathi Nagar",
    "92": "Shivaji Nagar",
    "93": "Vasanth Nagar",
    "94": "Gandhinagar",
    "95": "Subhash Nagar",
    "96": "Okalipuram",
    "97": "Dayananda Nagar",
    "98": "Prakash Nagar",
    "99": "Rajaji Nagar",
    "100": "Basaveshwara Nagar",
    "101": "Kamakshipalya",
    "102": "Vrisabhavathi Nagar",
    "103": "Kaveripura",
    "104": "Govindaraja Nagar",
    "105": "Agrahara Dasarahalli",
    "106": "Dr. Raj Kumar Ward",
    "107": "Shivanagara",
    "108": "Sriramamandir",
    "109": "Chickpete",
    "110": "Sampangiram Nagar",
    "111": "Shantala Nagar",
    "112": "Domlur",
    "113": "Konena Agrahara",
    "114": "Agaram",
    "115": "Vannarpet",
    "116": "Nilasandra",
    "117": "Shanthi Nagar",
    "118": "Sudham Nagara",
    "119": "Dharmaraya Swamy Temple",
    "120": "Cottonpete",
    "121": "Binnipete",
    "122": "Kempapura Agrahara",
    "123": "Vijayanagar",
    "124": "Hosahalli",
    "125": "Marenahalli",
    "126": "Maruthi Mandir ward",
    "127": "Mudalapalya",
    "128": "Nagarabhavi",
    "129": "Jnana Bharathi ward",
    "130": "Ullalu",
    "131": "Nayandahalli",
    "132": "Attiguppe",
    "133": "Hampi Nagar",
    "134": "Bapuji Nagar",
    "135": "Padarayanapura",
    "136": "Jagajivanaramnagar",
    "137": "Rayapuram",
    "138": "Chalavadipalya",
    "139": "K R Market",
    "140": "Chamrajapet",
    "141": "Azad Nagar",
    "142": "Sunkenahalli",
    "143": "Vishveshwara Puram",
    "144": "Siddapura",
    "145": "Hombegowda Nagara",
    "146": "Lakkasandra",
    "147": "Adugodi",
    "148": "Ejipura",
    "149": "Varthuru",
    "150": "Bellanduru",
    "151": "Koramangala",
    "152": "Suddagunte Palya",
    "153": "Jayanagar",
    "154": "Basavanagudi",
    "155": "Hanumanth Nagar",
    "156": "Srinagar",
    "157": "Gali Anjenaya Temple ward",
    "158": "Deepanjali Nagar",
    "159": "Kengeri",
    "160": "Rajarajeshwari Nagar",
    "161": "Hosakerehalli",
    "162": "Girinagar",
    "163": "Katriguppe",
    "164": "Vidyapeeta ward",
    "165": "Ganesh Mandir ward",
    "166": "Karisandra",
    "167": "Yediyur",
    "168": "Pattabhiram Nagar",
    "169": "Byrasandra",
    "170": "Jayanagar East",
    "171": "Gurappanapalya",
    "172": "Madivala",
    "173": "Jakkasandra",
    "174": "HSR Layout",
    "175": "Bommanahalli",
    "176": "BTM Layout",
    "177": "J P Nagar",
    "178": "Sarakki",
    "179": "Shakambari Nagar",
    "180": "Banashankari Temple ward",
    "181": "Kumaraswamy Layout",
    "182": "Padmanabha Nagar",
    "183": "Chikkalsandra",
    "184": "Uttarahalli",
    "185": "Yelchenahalli",
    "186": "Jaraganahalli",
    "187": "Puttenahalli",
    "188": "Bilekhalli",
    "189": "Hongasandra",
    "190": "Mangammanapalya",
    "191": "Singasandra",
    "192": "Begur",
    "193": "Arakere",
    "194": "Gottigere",
    "195": "Konankunte",
    "196": "Anjanapura",
    "197": "Vasanthpura",
    "198": "Hemmigepura",
}


def add_ward_names(df: pd.DataFrame) -> pd.DataFrame:
    """Insert the old BBMP ward name after its number."""
    ward_name = df["ward_number"].astype("string").str.strip().map(WARD_NAMES)
    df = df.drop(columns="ward_name", errors="ignore")
    df.insert(df.columns.get_loc("ward_number") + 1, "ward_name", ward_name)

    unmapped = df.loc[
        ward_name.isna() & df["ward_number"].notna(),
        "ward_number",
    ].unique()
    if len(unmapped):
        print(f"  Warning: {len(unmapped)} ward(s) without a name: "
              f"{', '.join(map(str, sorted(unmapped)))}", file=sys.stderr)
    return df


def normalize_certificate_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Upgrade parser output created with the previous schema."""
    df = df.rename(columns={"circle": "office_number", "ward": "ward_number"})
    return df[[column for column in CERTIFICATE_COLUMNS if column in df]]


def load_existing(name: str) -> pd.DataFrame:
    """The dataset's internal copy, falling back to the published one."""
    for path in internal(name), published(name):
        if path.exists():
            frame = pd.read_parquet(path)
            return normalize_certificate_columns(frame) if name == "employment" else frame
    return pd.DataFrame()


def merge_records(
    existing: pd.DataFrame,
    records: list[dict],
    touched: set[str],
    dedup_cols: list[str],
    sort_col: str | None,
) -> pd.DataFrame:
    if not existing.empty:
        existing = existing[
            ~existing["registration_no"].astype(str).isin(touched)
        ]
    combined = pd.concat([existing, pd.DataFrame(records)], ignore_index=True)
    if not combined.empty:
        # A row without a key can never be matched, updated or joined again.
        combined = combined[combined["registration_no"].notna()]
        dedup = [column for column in dedup_cols if column in combined]
        if dedup:
            combined = combined.drop_duplicates(subset=dedup, keep="last")
        if sort_col and sort_col in combined:
            combined = combined.sort_values(
                sort_col, ascending=False, na_position="last"
            )
    return combined


def reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    leading = [column for column in ("registration_no", "registration_date", "ward_name")
               if column in df]
    if leading:
        df = df[leading + [column for column in df if column not in leading]]

    tail = [column for column in TAIL_COLUMNS if column in df]
    return df[[column for column in df if column not in tail] + tail]


def publish(df: pd.DataFrame, name: str) -> None:
    """Write a published Parquet file and zipped CSV."""
    if df.empty:
        return

    if name == "employment":
        df = normalize_certificate_columns(df)
    if "ward_number" in df:
        df = add_ward_names(df)
    df = df.drop(columns=DROP_COLUMNS.get(name, []), errors="ignore")
    df = reorder_columns(df)

    for col in INT_COLUMNS:
        if col in df:
            df[col] = df[col].astype("Int64")

    DATA_DIR.mkdir(exist_ok=True)
    parquet_out = published(name)
    atomic_parquet(df, parquet_out)
    csv_out = parquet_out.with_suffix(".csv.zip")
    temporary = csv_out.with_suffix(csv_out.suffix + ".tmp")
    try:
        df.to_csv(
            temporary,
            index=False,
            date_format="%Y-%m-%d",
            compression={"method": "zip", "archive_name": f"{name}.csv"},
        )
        os.replace(temporary, csv_out)
    finally:
        temporary.unlink(missing_ok=True)
    print(f"Wrote {parquet_out} and {csv_out} ({len(df)} rows, {len(df.columns)} columns).")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Parse Karnataka labour registration PDFs into Parquet files.",
    )
    parser.add_argument("--reparse-all", action="store_true",
                        help="reparse every raw PDF, not just new or changed ones")
    parser.add_argument("--allow-removals", action="store_true",
                        help="allow a published registration to disappear")
    return parser.parse_args()


def parse_files(
    candidates: list[tuple[CertificateKey, Path]],
) -> tuple[dict[str, list[dict]], set[str], list[tuple[CertificateKey, Path, str]]]:
    """Parse each candidate PDF into per-dataset records."""
    records: dict[str, list[dict]] = {name: [] for name in DATASETS}
    touched: set[str] = set()
    failures: list[tuple[CertificateKey, Path, str]] = []

    for key, pdf_path in candidates:
        print(f"  {pdf_path}")
        record, renewals, amendments, reason = parse_document(pdf_path, key)
        if reason:
            failures.append((key, pdf_path, reason))
            continue
        records["employment"].append(record)
        records["renewals"].extend(renewals)
        records["amendments"].extend(amendments)
        touched.add(key.number)

    return records, touched, failures


def report_failures(parsed: int, failures: list[tuple[CertificateKey, Path, str]]) -> None:
    if not failures:
        return
    print(f"\n{len(failures)} of {parsed} PDF(s) had extraction issues:", file=sys.stderr)
    for _key, path, reason in failures:
        print(f"  - {path}: {reason}", file=sys.stderr)


def newer(source: Path, target: Path) -> bool:
    return source.exists() and (
        not target.exists() or source.stat().st_mtime_ns > target.stat().st_mtime_ns
    )


def check_removals(certificates: pd.DataFrame, allow: bool) -> None:
    """Refuse to drop registrations that are already published."""
    kept = set(certificates["registration_no"].dropna().astype(str))
    missing = registration_ids(published("employment")) - kept
    if missing and not allow:
        raise RuntimeError(
            f"Refusing to remove {len(missing)} published registration(s); "
            "restore their raw PDFs or use --allow-removals"
        )


def republish(allow_removals: bool) -> None:
    frames = {name: pd.read_parquet(internal(name)) for name in PUBLISHED
              if internal(name).exists()}
    check_removals(frames["employment"], allow_removals)
    for name, frame in frames.items():
        publish(frame, name)


def main() -> None:
    args = parse_args()
    pdf_files = list(raw_pdf_keys(RAW_DIR))
    if not pdf_files:
        raise SystemExit(
            f"No PDFs found under {RAW_DIR}. Run 'python3 fetch.py --index' to seed "
            "the fetch state from the published data, then 'python3 fetch.py'."
        )

    source = internal("employment")
    if not source.exists():
        source = published("employment")
    existing_ids = registration_ids(source)
    baseline = source.stat().st_mtime_ns if source.exists() else 0
    retry_ids = set(load_json(FAILURE_FILE, []))
    available_ids = {key.number for key, _path in pdf_files}
    retained_retry_ids = retry_ids & available_ids
    if retained_retry_ids != retry_ids:
        save_json(FAILURE_FILE, sorted(retained_retry_ids))
        retry_ids = retained_retry_ids

    candidates = [
        (key, path)
        for key, path in pdf_files
        if args.reparse_all
        or key.number not in existing_ids
        or key.number in retry_ids
        or path.stat().st_mtime_ns > baseline
    ]
    if not candidates:
        stale = internal("employment").exists() and any(
            newer(internal(name), published(name)) for name in PUBLISHED
        )
        if stale:
            republish(args.allow_removals)
        else:
            print("No new or changed PDFs. Outputs unchanged.")
        return

    records, touched, failures = parse_files(candidates)
    save_json(FAILURE_FILE, sorted(key.number for key, _path, _reason in failures))
    if not touched:
        report_failures(len(candidates), failures)
        return

    frames = {
        name: merge_records(load_existing(name), records[name], touched, *MERGE_KEYS[name])
        for name in DATASETS
    }
    # Supplementary rows have to join; a few scans print a registration number
    # that resolves to a different certificate on the portal.
    final_ids = set(frames["employment"]["registration_no"].dropna().astype(str))
    for name in ("renewals", "amendments"):
        frame = frames[name]
        if "registration_no" in frame:
            frames[name] = frame[frame["registration_no"].isin(final_ids)]

    # Certificates last: their file is the baseline for the next incremental run.
    for name in reversed(DATASETS):
        atomic_parquet(frames[name], internal(name))

    check_removals(frames["employment"], args.allow_removals)
    for name in PUBLISHED:
        publish(frames[name], name)
    report_failures(len(candidates), failures)


if __name__ == "__main__":
    main()
