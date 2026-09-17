#!/usr/bin/env python3
"""
NCD Listing Enrichment - Browser App (Streamlit)
=================================================
Drop in the three monthly exchange files (BSE additions, NSE additions, and the
BSE/NSE common list). The app reads them whatever shape they arrive in, removes
the cross-exchange duplication, enriches every ISIN via the NSDL India Bond Info
public API, and produces a formatted, downloadable workbook plus an on-screen
dashboard.

Only private placements are analysed: public-issue sheets are skipped on
read, and any row NSDL records as a public issue is dropped as a backstop.

Only four columns are required in the source files - Company Name, Listing Date,
ISIN and Capital Listed. Headers, header positions, sheet names and column order
may all change from month to month; everything else on the output sheet is
derived from the ISIN via NSDL.

Output workbook:
    1. Enriched_Data   - deduplicated rows + exchange flag + NSDL enrichment
    2. Summary         - formula-driven totals by exchange, issue type, rating,
                         issuer type, industry, day and top issuers
    3. Charts          - native Excel charts built on the Summary figures
    4. Top_50_Issuers  - ranked by amount raised (live COUNTIFS/SUMIFS formulas)
    5. Reconciliation  - what the dedup removed and any cross-file conflicts
    6. Exceptions      - ISINs whose API lookups failed or returned empty data

Setup (one-time):
    pip install -r requirements.txt

Run:
    streamlit run ncd_app.py

Notes:
  * A JSON cache (nsdl_cache.json, saved next to this file) stores every
    successful API response - re-processing the same ISINs is instant.
  * Fetching runs 6 ISINs in parallel (~40-60s for 200 fresh ISINs).
"""

import io
import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from pathlib import Path

import pandas as pd

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------
BASE = "https://www.indiabondinfo.nsdl.com/bds-service/v1/public"
ENDPOINTS = {
    "isin":       BASE + "/isins?isin={isin}",
    "ratings":    BASE + "/bdsinfo/credit-ratings?isin={isin}",
    "instrument": BASE + "/bdsinfo/instruments?isin={isin}",
}
CACHE_FILE = Path(__file__).parent / "nsdl_cache.json"
MAX_WORKERS = 6          # parallel ISINs; keep modest to avoid throttling
MAX_RETRIES = 3
TIMEOUT = 20

# Canonical output column names. The four source columns are renamed to these
# on ingest, so nothing downstream depends on what the exchanges called them.
COL_ISIN = "ISIN"
COL_NAME = "Company Name"
COL_DATE = "Listing Date"
COL_AMT = "Capital Listed (Rs. Crores)"

# A re-issue is an ISIN that was allotted well before this month's listing.
# Validated at 100% on BSE July (114 rows) and 97.9% on NSE July (48 rows);
# any threshold from 7 to 20 days gives identical results on that data.
REISSUE_GAP_DAYS = 15

ISIN_RE = re.compile(r"^IN[A-Z0-9]{10}$")


# --------------------------------------------------------------------------
# TLS trust store
# --------------------------------------------------------------------------
def repair_ca_bundle_env():
    """Drop CA-bundle env vars that point at files which no longer exist.

    Some installers (PostgreSQL 18, for one) set CURL_CA_BUNDLE machine-wide to
    a path inside their own tree. If that tree is moved or removed, every
    `requests` call on the machine dies with 'Could not find a suitable TLS CA
    certificate bundle' before a single byte goes out. A broken pointer is worse
    than no pointer - unset it so requests falls back to certifi.
    """
    broken = []
    for var in ("REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE", "SSL_CERT_FILE"):
        path = os.environ.get(var)
        if path and not Path(path).is_file():
            os.environ.pop(var, None)
            broken.append(f"{var}={path}")
    return broken


# --------------------------------------------------------------------------
# Payload validation
# --------------------------------------------------------------------------
def is_empty_payload(d):
    """True when the API returned a JSON body with no usable data
    (every top-level value is null/empty) - treated as a failed lookup."""
    if not isinstance(d, dict) or not d:
        return True
    return all(v in (None, "", [], {}) or (isinstance(v, str) and not v.strip())
               for v in d.values())


# --------------------------------------------------------------------------
# Fetch layer: parallel, cached, thread-safe
# --------------------------------------------------------------------------
class Fetcher:
    def __init__(self):
        import requests
        self._requests = requests
        self._local = threading.local()          # one Session per thread
        self._lock = threading.Lock()
        self.cache = {}
        if CACHE_FILE.exists():
            try:
                raw = json.loads(CACHE_FILE.read_text())
                # Scrub previously-cached empty payloads so they get retried
                self.cache = {k: v for k, v in raw.items() if not is_empty_payload(v)}
            except Exception:
                self.cache = {}
        self._dirty = 0

    def _session(self):
        if not hasattr(self._local, "s"):
            s = self._requests.Session()
            s.headers.update({
                "User-Agent": "Mozilla/5.0 (compatible; NCD-research-script)",
                "Accept": "application/json",
            })
            self._local.s = s
        return self._local.s

    def save_cache(self):
        with self._lock:
            CACHE_FILE.write_text(json.dumps(self.cache))
            self._dirty = 0

    def get(self, kind, isin):
        """Parsed JSON for (kind, isin), or None on failure/empty payload."""
        key = f"{kind}:{isin}"
        with self._lock:
            if key in self.cache:
                return self.cache[key]

        url = ENDPOINTS[kind].format(isin=isin)
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = self._session().get(url, timeout=TIMEOUT)
                if r.status_code == 200:
                    data = r.json()
                    if is_empty_payload(data):
                        return None              # 200 but no usable data
                    with self._lock:
                        self.cache[key] = data
                        self._dirty += 1
                        if self._dirty >= 25:    # periodic save
                            CACHE_FILE.write_text(json.dumps(self.cache))
                            self._dirty = 0
                    return data
                elif r.status_code in (429, 502, 503):
                    time.sleep(2 * attempt)
                else:
                    return None
            except Exception:
                time.sleep(2 * attempt)
        return None

    def fetch_isin(self, isin):
        """All three endpoints for one ISIN (runs inside a worker thread)."""
        return isin, {
            "isin":       self.get("isin", isin),
            "ratings":    self.get("ratings", isin),
            "instrument": self.get("instrument", isin),
        }


# --------------------------------------------------------------------------
# Extraction helpers
# --------------------------------------------------------------------------
AGENCY_PATTERNS = [
    (r"\bCRISIL\b",        "CRISIL"),
    (r"\bICRA\b",          "ICRA"),
    (r"\bCARE\b",          "CARE"),
    (r"INDIA\s+RATINGS?\b", "India Ratings"),
    (r"\bFITCH\b",         "India Ratings"),
    (r"\bACUITE\b",        "Acuite"),
    (r"\bBRICKWORK\b",     "Brickwork"),
    (r"\bINFOMERICS\b",    "Infomerics"),
    (r"\bACER\b",          "ACER"),
    (r"\bSMERA\b",         "SMERA"),
]


def short_agency(name):
    up = (name or "").upper()
    for pat, short in AGENCY_PATTERNS:
        if re.search(pat, up):
            return short
    return name.title() if name and name.isupper() else name


def extract_issuer(data):
    if not data:
        return None, None, None
    t = (data.get("issuerTypeOwner") or "").strip() or None
    ind = (data.get("basicIndusrty") or data.get("basicIndustry") or "").strip() or None
    name = (data.get("issuerName") or "").strip() or None
    return t, ind, name


def extract_ratings(data):
    if not data:
        return None
    parts = []
    for r in (data.get("currentRatings") or []):
        agency = short_agency((r.get("creditRatingAgencyName") or "").strip())
        rating = (r.get("currentRating") or "").strip()
        outlook = (r.get("outlook") or "").strip()
        if not rating:
            continue
        s = f"{agency}: {rating}" if agency else rating
        if outlook and outlook != "-":
            s += f" ({outlook})"
        parts.append(s)
    if parts:
        return "; ".join(parts)
    flag = (data.get("instrumentRateFlag") or "").strip()
    return flag or None


def _instrument_block(data):
    try:
        return data["instrumentsVo"]["instruments"] or {}
    except (KeyError, TypeError):
        return {}


def extract_category(data):
    cat = _instrument_block(data).get("category")
    return cat.strip() if isinstance(cat, str) and cat.strip() not in ("-", "") else None


# NSDL has no coupon field. The rate is the opening token of the instrument
# description ("10.25% SECURED RATED LISTED..."); non-fixed-rate paper says so
# in words instead. Matches the source file's own rate column on 110/114 rows.
COUPON_PCT_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*%")
FLOATING_RE = re.compile(r"RESET RATE|FLOATING|FLOATER|\bRR\b")
MLD_RE = re.compile(r"MARKET[\s-]?LINKED|\bMLDS?\b|\bPP-?MLD\b")
ZERO_RE = re.compile(r"ZERO[\s-]?COUPON|\bZCB\b")


def extract_coupon(isin_data, instrument_data):
    """Rate of interest: a float for fixed-rate paper, else a text label.

    NSDL leaves the rate out entirely for some issuers, in which case this
    returns None rather than guessing - a blank cell is honest, a wrong rate
    is not.
    """
    blk = _instrument_block(instrument_data)
    desc = blk.get("instrumentDesc") or ""
    sec_type = (isin_data or {}).get("secType") or ""
    m = COUPON_PCT_RE.match(desc)
    if m:
        return float(m.group(1))
    # The category is the surer signal for structured paper: an MLD's
    # description names its underlying ("NIFTY 50 SECURED RATED...") rather
    # than calling itself market linked.
    blob = f"{desc} {sec_type} {blk.get('category') or ''}".upper()
    if ZERO_RE.search(blob):
        return "Zero Coupon"
    if MLD_RE.search(blob):
        return "Market Linked"
    if FLOATING_RE.search(blob):
        return "Floating Rate"
    # Last resort: NSDL's short security name usually embeds the rate,
    # e.g. "REC LIMITED SR 223B 7.46 BD 30JU28 FVRS1LAC".
    m = re.search(r"\b(\d{1,2}\.\d{1,4})\b", sec_type)
    return float(m.group(1)) if m else None


def _nsdl_date(value):
    """NSDL serves dd-mm-yyyy, with '-' for 'not applicable'."""
    if not isinstance(value, str) or value.strip() in ("", "-"):
        return None
    try:
        return datetime.strptime(value.strip(), "%d-%m-%Y")
    except ValueError:
        return None


def extract_dates(instrument_data):
    """(original allotment date, redemption date, is_perpetual)."""
    blk = _instrument_block(instrument_data)
    perpetual = str(blk.get("perpetualInNature") or "").strip().lower() == "yes"
    return _nsdl_date(blk.get("allotmentDate")), _nsdl_date(blk.get("redemptionDate")), perpetual


def derive_type_of_issue(listing_date, allotment_date):
    """New Issue vs Re-Issue.

    NSDL reports an ISIN's *original* allotment date, not the current tranche's.
    So a listing that sits well after allotment is a further issue on paper that
    already exists - a re-issue. On new issues the two dates are days apart.
    """
    if allotment_date is None or listing_date is None:
        return None
    if pd.isna(listing_date) or pd.isna(allotment_date):
        return None
    gap = (pd.Timestamp(listing_date).normalize() - pd.Timestamp(allotment_date).normalize()).days
    return "Re-Issue" if gap > REISSUE_GAP_DAYS else "New Issue"


# Best (highest) rating held across agencies, for the summary charts.
GRADE_ORDER = ["AAA", "AA+", "AA", "AA-", "A+", "A", "A-",
               "BBB+", "BBB", "BBB-", "BB+", "BB", "BB-",
               "B+", "B", "B-", "C", "D"]
# Longest token first, or "AA-" would match as "AA" and "A-" as "A".
GRADE_RE = re.compile("|".join(re.escape(g)
                               for g in sorted(GRADE_ORDER, key=len, reverse=True)))
# Structured-paper qualifiers that sit in front of the grade. Left in place the
# "D" of "PP-MLD" reads as a default rating, which is the opposite of the truth.
GRADE_PREFIX_RE = re.compile(r"^(?:PROVISIONAL|PROV\.?|PP-?MLD|MLD|IND|CE|SO)+")


def best_grade(rating_str):
    """The strongest grade an instrument holds across agencies, or None."""
    if not isinstance(rating_str, str) or not rating_str.strip():
        return None
    found = []
    for part in rating_str.split(";"):
        # Drop the agency prefix and the outlook so stray letters can't match.
        txt = part.split(":")[-1]
        txt = re.sub(r"\(.*?\)", "", txt).upper().replace(" ", "")
        txt = GRADE_PREFIX_RE.sub("", txt)
        # Anchored: once the agency and qualifiers are gone the grade is the
        # first thing left. Searching anywhere would find the "A" in "Rated",
        # which is NSDL's flag for "an agency rated this" - not a grade.
        m = GRADE_RE.match(txt)
        if m:
            found.append(m.group(0))
    return min(found, key=GRADE_ORDER.index) if found else None


# --------------------------------------------------------------------------
# Issuer name normalisation
# --------------------------------------------------------------------------
SUFFIX_MAP = [
    (r"\bLTD\.?\b", "LIMITED"),
    (r"\bPVT\.?\b", "PRIVATE"),
    (r"\bPRIVTE\b", "PRIVATE"),
    (r"\bCORPN\.?\b", "CORPORATION"),
]


def clean_name(name):
    if not isinstance(name, str) or not name.strip():
        return ""
    s = name.upper().strip()
    s = s.replace("&", " AND ")
    s = re.sub(r"[*™®]", "", s)
    s = re.sub(r"[^\w\s\-\.]", " ", s)
    for pat, rep in SUFFIX_MAP:
        s = re.sub(pat, rep, s)
    s = re.sub(r"\.\s*", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    keep_upper = {"NHPC", "REC", "LIC", "PNB", "IIFL", "HDB", "NIIF", "SMFG",
                  "MAS", "RDC", "ESAF", "NCD", "SDI", "REIT", "INVIT",
                  "NBFC", "L", "T", "A", "K", "R", "AK", "II", "III", "IV"}
    keep_lower = {"AND", "OF", "FOR", "THE", "IN"}
    words = []
    for i, w in enumerate(s.split()):
        if w in keep_upper:
            words.append(w)
        elif w in keep_lower and i > 0:
            words.append(w.lower())
        else:
            words.append(w.capitalize())
    return " ".join(words)


# ==========================================================================
# Ingestion: read whatever shape the exchanges send this month
# ==========================================================================
NAME_KEYWORDS = ("COMPANY", "ISSUER", "ENTITY", "SECURITY NAME", "NAME OF")
LISTING_KW_STRONG = ("LISTING", "LISTED")
LISTING_KW_WEAK = ("ADMISSION", "TRADING", "EFFECTIVE")
AMT_KEYWORDS = ("CAPITAL", "ISSUE SIZE", "AMOUNT", "RAISED", "VALUE",
                "CRORE", "CRS", "CR.", "LAKH", "LAC")
LAKH_KEYWORDS = ("LAKH", "LAC")
SERIAL_KEYWORDS = ("SR", "S.NO", "SNO", "SERIAL", "#")
SKIP_SHEET_KEYWORDS = ("PIVOT", "CHART", "SUMMARY", "NOTES")


def _is_isin(value):
    return isinstance(value, str) and bool(ISIN_RE.match(value.strip().upper()))


def _isin_fraction(series):
    vals = [v for v in series if v is not None and not (isinstance(v, float) and pd.isna(v))]
    if not vals:
        return 0.0
    return sum(_is_isin(str(v)) for v in vals) / len(vals)


def _numeric_fraction(series):
    vals = [v for v in series if v is not None and not (isinstance(v, float) and pd.isna(v))]
    if not vals:
        return 0.0
    return sum(isinstance(v, (int, float)) and not isinstance(v, bool) for v in vals) / len(vals)


def _date_fraction(series):
    vals = [v for v in series if v is not None and not (isinstance(v, float) and pd.isna(v))]
    if not vals:
        return 0.0
    return sum(isinstance(v, (datetime, date, pd.Timestamp)) for v in vals) / len(vals)


def _looks_like_serial(series):
    """1, 2, 3, ... - a row counter, not an amount."""
    nums = [v for v in series if isinstance(v, (int, float)) and not isinstance(v, bool)
            and not pd.isna(v)]
    if len(nums) < 2:
        return False
    return all(float(n).is_integer() for n in nums) and nums[0] <= 2 and \
        all(b - a == 1 for a, b in zip(nums, nums[1:]))


def _header_match(columns, keywords):
    return [c for c in columns if any(k in str(c).upper() for k in keywords)]


def frame_from_raw(raw):
    """Turn a headerless sheet dump into a DataFrame with real column names.

    The header row is found relative to the data rather than assumed to be row 1,
    so a title row, a blank row, or a shifted table all still parse.
    """
    first_data = None
    for r in range(len(raw)):
        if any(_is_isin(str(v)) for v in raw.iloc[r] if v is not None):
            first_data = r
            break
    if first_data is None:
        return None                              # no ISINs here - not a data sheet

    header_row = None
    for r in range(first_data - 1, -1, -1):
        cells = [v for v in raw.iloc[r] if isinstance(v, str) and v.strip()]
        if len(cells) >= 2:
            header_row = r
            break

    if header_row is None:
        df = raw.iloc[first_data:].reset_index(drop=True)
        df.columns = [f"Column {i + 1}" for i in range(df.shape[1])]
    else:
        names, seen = [], {}
        for i, v in enumerate(raw.iloc[header_row]):
            n = str(v).strip() if v is not None and str(v).strip() else f"Column {i + 1}"
            seen[n] = seen.get(n, 0) + 1
            names.append(n if seen[n] == 1 else f"{n} ({seen[n]})")
        df = raw.iloc[header_row + 1:].reset_index(drop=True)
        df.columns = names
    return df


def detect_columns(df):
    """Locate the four mandatory columns. Returns (mapping, lakh_flag, warnings).

    Header text is tried first, then the values themselves, so a renamed column
    is still found as long as it holds the right kind of data.
    """
    warnings = []
    cols = list(df.columns)

    # --- ISIN: always identified by value, never by header ---
    scored = sorted(((_isin_fraction(df[c]), c) for c in cols), reverse=True)
    if not scored or scored[0][0] < 0.5:
        return None, False, ["no column of ISIN-shaped values"]
    isin_col = scored[0][1]
    taken = {isin_col}

    # --- Company name ---
    cands = [c for c in _header_match(cols, NAME_KEYWORDS) if c not in taken]
    if not cands:
        cands = [c for c in cols if c not in taken
                 and _numeric_fraction(df[c]) < 0.2 and _date_fraction(df[c]) < 0.2
                 and df[c].notna().sum() > 0]
        if cands:
            warnings.append(f"company name guessed from values: '{cands[0]}'")
    if not cands:
        return None, False, ["no company-name column"]
    name_col = cands[0]
    taken.add(name_col)

    # --- Listing date ---
    date_cols = [c for c in cols if c not in taken and _date_fraction(df[c]) > 0.5]
    if not date_cols:
        return None, False, ["no listing-date column"]
    cands = _header_match(date_cols, LISTING_KW_STRONG) or         _header_match(date_cols, LISTING_KW_WEAK)
    if cands:
        date_col = cands[0]
    else:
        date_col = _pick_listing_date(df, date_cols)
        warnings.append(f"listing date guessed as '{date_col}' "
                        f"(no column named 'Listing Date')")
    taken.add(date_col)

    # --- Capital listed ---
    cands = [c for c in _header_match(cols, AMT_KEYWORDS)
             if c not in taken
             and not _header_match([c], SERIAL_KEYWORDS)
             and not _looks_like_serial(df[c])
             and _numeric_fraction(df[c]) > 0]
    if not cands:
        cands = [c for c in cols if c not in taken
                 and _numeric_fraction(df[c]) > 0.5
                 and not _looks_like_serial(df[c])
                 and not _header_match([c], SERIAL_KEYWORDS)]
        if cands:
            warnings.append(f"capital column guessed from values: '{cands[0]}'")
    if not cands:
        return None, False, ["no capital/amount column"]
    # A sheet may carry the same figure in lakhs and in crores - prefer crores.
    in_crores = [c for c in cands if not _header_match([c], LAKH_KEYWORDS)]
    amt_col = in_crores[0] if in_crores else cands[0]
    is_lakh = bool(_header_match([amt_col], LAKH_KEYWORDS))
    if is_lakh:
        warnings.append(f"'{amt_col}' is in lakhs - converted to crores")

    return {COL_ISIN: isin_col, COL_NAME: name_col,
            COL_DATE: date_col, COL_AMT: amt_col}, is_lakh, warnings


def _pick_listing_date(df, date_cols):
    """Choose the listing date among several unlabelled date columns.

    A listing sits on or after allotment and long before redemption, so drop
    the far-future column (maturity) and keep the latest of what remains.
    """
    medians = {}
    for c in date_cols:
        med = pd.to_datetime(df[c], errors="coerce").median()
        if pd.notna(med):
            medians[c] = med
    if not medians:
        return date_cols[0]
    earliest = min(medians.values())
    near = {c: m for c, m in medians.items()
            if (m - earliest).days <= 365} or medians
    return max(near, key=near.get)


def classify_source(filename):
    """Which exchange file is this? COMMON must be tested first - the common
    file's own name contains both 'BSE' and 'NSE'."""
    up = filename.upper()
    if "COMMON" in up:
        return "COMMON"
    if "BSE" in up and "NSE" not in up:
        return "BSE"
    if "NSE" in up and "BSE" not in up:
        return "NSE"
    if "BSE" in up:
        return "BSE"
    if "NSE" in up:
        return "NSE"
    return "UNKNOWN"


def is_public_issue_sheet(sheet_name):
    """'Public' / 'Public Issue' sheets hold public issues; 'PPDI' / 'Private'
    hold private placements. Only the latter are analysed."""
    return "PUBLIC" in sheet_name.upper()


def read_source_file(file_bytes, filename, source):
    """Parse every data sheet in one workbook into a tidy frame.

    Returns (frame, per-sheet report). Sheets with no ISIN column - pivots,
    charts, 'NIL' placeholders - are skipped and reported, not treated as errors.
    """
    report, parts = [], []
    try:
        xls = pd.ExcelFile(io.BytesIO(file_bytes))
    except Exception as e:
        return pd.DataFrame(), [{"File": filename, "Sheet": "-", "Rows": 0,
                                 "Status": f"unreadable: {e}"}]

    for sheet in xls.sheet_names:
        base = {"File": filename, "Sheet": sheet}
        if any(k in sheet.upper() for k in SKIP_SHEET_KEYWORDS):
            report.append({**base, "Rows": 0, "Status": "skipped (pivot/summary sheet)"})
            continue
        if is_public_issue_sheet(sheet):
            report.append({**base, "Rows": 0,
                           "Status": "excluded (public issues - private placements only)"})
            continue
        try:
            raw = xls.parse(sheet, header=None, dtype=object)
        except Exception as e:
            report.append({**base, "Rows": 0, "Status": f"unreadable: {e}"})
            continue

        df = frame_from_raw(raw)
        if df is None:
            report.append({**base, "Rows": 0, "Status": "skipped (no ISINs found)"})
            continue

        mapping, is_lakh, warns = detect_columns(df)
        if mapping is None:
            report.append({**base, "Rows": 0,
                           "Status": "SKIPPED - " + "; ".join(warns)})
            continue

        out = df[[mapping[COL_ISIN], mapping[COL_NAME],
                  mapping[COL_DATE], mapping[COL_AMT]]].copy()
        out.columns = [COL_ISIN, COL_NAME, COL_DATE, COL_AMT]
        out[COL_ISIN] = out[COL_ISIN].astype(str).str.strip().str.upper()
        out = out[out[COL_ISIN].str.match(ISIN_RE)]          # drops Total / NIL rows
        out[COL_DATE] = pd.to_datetime(out[COL_DATE], errors="coerce")
        out[COL_AMT] = pd.to_numeric(out[COL_AMT], errors="coerce")
        if is_lakh:
            out[COL_AMT] = out[COL_AMT] / 100.0
        out = out[out[COL_AMT].notna()]
        out[COL_NAME] = out[COL_NAME].astype(str).str.strip()
        out["Exchange"] = source

        parts.append(out)
        report.append({**base, "Rows": len(out),
                       "Status": "; ".join(warns) if warns else "ok",
                       "ISIN col": mapping[COL_ISIN], "Name col": mapping[COL_NAME],
                       "Date col": mapping[COL_DATE], "Capital col": mapping[COL_AMT]})

    frame = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(
        columns=[COL_ISIN, COL_NAME, COL_DATE, COL_AMT, "Exchange"])
    return frame, report


# ==========================================================================
# Dedup: the common file is redundant with BSE and NSE by construction
# ==========================================================================
# Rs. 10,000 expressed in crores. Below this the two exchanges agree and the
# difference is float noise; above it, a digit was genuinely transposed.
AMOUNT_CONFLICT_CR = 0.001


def _amount_tolerance(a, b):
    """Exchanges occasionally transpose a digit (502.4151 vs 502.4115)."""
    return abs(a - b) <= max(0.05, 0.001 * max(abs(a), abs(b)))


def consolidate(bse, nse, common):
    """Merge the three files into one row per real listing event.

    An ISIN can legitimately appear several times in one exchange's file - a
    further issue on the same paper later in the month - so rows are matched
    across files pair by pair on amount, never collapsed on ISIN alone.
    """
    audit = []
    kept = []

    all_isins = sorted(set(bse[COL_ISIN]) | set(nse[COL_ISIN]) | set(common[COL_ISIN]))
    for isin in all_isins:
        b_rows = bse[bse[COL_ISIN] == isin].to_dict("records")
        n_rows = nse[nse[COL_ISIN] == isin].to_dict("records")
        c_rows = common[common[COL_ISIN] == isin].to_dict("records")
        n_unmatched = list(range(len(n_rows)))

        for b in b_rows:
            # Pair this BSE listing with the closest unclaimed NSE listing.
            best, best_diff = None, None
            for k in n_unmatched:
                diff = abs(float(b[COL_AMT]) - float(n_rows[k][COL_AMT]))
                if _amount_tolerance(float(b[COL_AMT]), float(n_rows[k][COL_AMT])) and \
                        (best_diff is None or diff < best_diff):
                    best, best_diff = k, diff
            if best is None:
                kept.append({**b, "Exchange": "BSE"})
                continue

            n = n_rows[best]
            n_unmatched.remove(best)
            notes = []
            amount = float(b[COL_AMT])
            # Where BSE and NSE disagree, the common file is the tie-breaker.
            match_c = next((c for c in c_rows
                            if _amount_tolerance(float(c[COL_AMT]), amount)), None)
            if best_diff > AMOUNT_CONFLICT_CR:
                chosen = float(match_c[COL_AMT]) if match_c else amount
                notes.append(f"capital differs across exchanges "
                             f"(BSE {b[COL_AMT]:,.4f} / NSE {n[COL_AMT]:,.4f}); "
                             f"used {chosen:,.4f}")
                amount = chosen
                audit.append({"ISIN": isin, "Issue": "Capital mismatch",
                              "Detail": notes[-1]})
            listing = min(pd.Timestamp(b[COL_DATE]), pd.Timestamp(n[COL_DATE]))
            if pd.Timestamp(b[COL_DATE]) != pd.Timestamp(n[COL_DATE]):
                detail = (f"BSE {pd.Timestamp(b[COL_DATE]):%d-%m-%Y} vs "
                          f"NSE {pd.Timestamp(n[COL_DATE]):%d-%m-%Y}; used the earlier")
                notes.append("listing date differs across exchanges; used the earlier")
                audit.append({"ISIN": isin, "Issue": "Listing date mismatch",
                              "Detail": detail})
            kept.append({**b, COL_AMT: amount, COL_DATE: listing, "Exchange": "Both",
                         "Data Notes": "; ".join(notes)})

        for k in n_unmatched:
            kept.append({**n_rows[k], "Exchange": "NSE"})

        # The common file should be a strict subset of BSE and NSE. Anything in
        # it that we could not pair is either missing upstream or a real listing
        # only the common file carries - keep it and say so.
        for c in c_rows:
            paired = any(_amount_tolerance(float(c[COL_AMT]), float(r[COL_AMT]))
                         for r in kept if r[COL_ISIN] == isin and r["Exchange"] == "Both")
            if paired:
                continue
            in_b = any(_amount_tolerance(float(c[COL_AMT]), float(r[COL_AMT])) for r in b_rows)
            in_n = any(_amount_tolerance(float(c[COL_AMT]), float(r[COL_AMT])) for r in n_rows)
            missing = [x for x, ok in (("BSE", in_b), ("NSE", in_n)) if not ok]
            audit.append({"ISIN": isin, "Issue": "In common file only",
                          "Detail": f"not found in the {' and '.join(missing)} file"
                                    f" (Rs. {float(c[COL_AMT]):,.4f} cr)"})
            if not in_b and not in_n:
                kept.append({**c, "Exchange": "Both",
                             "Data Notes": "present only in the common file"})

    out = pd.DataFrame(kept)
    if out.empty:
        return out, pd.DataFrame(columns=["ISIN", "Issue", "Detail"])
    if "Data Notes" not in out.columns:
        out["Data Notes"] = ""
    out["Data Notes"] = out["Data Notes"].fillna("")
    out = out.sort_values([COL_DATE, COL_NAME, COL_ISIN]).reset_index(drop=True)
    return out, pd.DataFrame(audit, columns=["ISIN", "Issue", "Detail"])


# ==========================================================================
# Core pipeline
# ==========================================================================
OUTPUT_ORDER = [COL_NAME, COL_ISIN, COL_DATE, COL_AMT, "Exchange",
                "Type of Issue", "Rate of Interest", "Original Allotment Date",
                "Maturity Date", "Type of Issuer", "Industry of Issuer",
                "Credit Rating(s)", "Best Rating", "Category of Instrument",
                "Clean Issuer Name", "Data Notes"]


def run_pipeline(frames, progress_cb=None):
    """frames: {"BSE": df, "NSE": df, "COMMON": df} as returned by read_source_file."""
    empty = pd.DataFrame(columns=[COL_ISIN, COL_NAME, COL_DATE, COL_AMT, "Exchange"])
    bse = frames.get("BSE", empty)
    nse = frames.get("NSE", empty)
    common = frames.get("COMMON", empty)
    if bse.empty and nse.empty and common.empty:
        raise ValueError("None of the uploaded files contained usable ISIN rows.")

    df, audit = consolidate(bse, nse, common)

    unique_isins = sorted(df[COL_ISIN].unique())
    fetcher = Fetcher()
    results = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [pool.submit(fetcher.fetch_isin, i) for i in unique_isins]
        for n, fut in enumerate(as_completed(futures), 1):
            isin, payloads = fut.result()
            results[isin] = payloads
            if progress_cb:
                progress_cb(n, len(unique_isins))
    fetcher.save_cache()

    # Backstop for the sheet-level exclusion: if a future file mixes public
    # issues into a private sheet, or renames the public sheet, NSDL's own
    # mode-of-issue field still catches them.
    public = [i for i in unique_isins
              if str(_instrument_block(results[i]["instrument"]).get("modeOfIssue") or "")
              .strip().lower() == "public issue"]
    if public:
        dropped = df[df[COL_ISIN].isin(public)]
        audit = pd.concat([audit, pd.DataFrame(
            [{"ISIN": r[COL_ISIN], "Issue": "Excluded - public issue",
              "Detail": f"NSDL records this as a public issue; removed from a "
                        f"private-placement sheet ({r[COL_NAME]}, "
                        f"Rs. {r[COL_AMT]:,.2f} cr)"}
             for _, r in dropped.iterrows()])], ignore_index=True)
        df = df[~df[COL_ISIN].isin(public)].reset_index(drop=True)
        unique_isins = [i for i in unique_isins if i not in set(public)]
        if df.empty:
            raise ValueError("Every row was a public issue - nothing left to analyse.")

    enrich, exceptions = {}, []
    for isin in unique_isins:
        p = results[isin]
        t, ind, nsdl_name = extract_issuer(p["isin"])
        allot, maturity, perpetual = extract_dates(p["instrument"])
        enrich[isin] = dict(
            type_of_issuer=t, industry=ind, nsdl_name=nsdl_name,
            ratings=extract_ratings(p["ratings"]),
            category=extract_category(p["instrument"]),
            coupon=extract_coupon(p["isin"], p["instrument"]),
            allotment=allot, maturity=maturity, perpetual=perpetual)
        failed = [lbl for lbl, key in [("issuer details", "isin"),
                                       ("credit ratings", "ratings"),
                                       ("instrument", "instrument")]
                  if p[key] is None]
        if failed:
            src = df.loc[df[COL_ISIN] == isin, COL_NAME].iloc[0]
            exceptions.append((isin, src, failed))

    def col(key):
        return df[COL_ISIN].map(lambda i: (enrich.get(i) or {}).get(key))

    df["Type of Issuer"] = col("type_of_issuer")
    df["Industry of Issuer"] = col("industry")
    df["Credit Rating(s)"] = col("ratings")
    # The strongest grade across agencies. Stored as its own column because the
    # Summary counts it with COUNTIFS - parsing "CRISIL: AAA (Stable); ..." is
    # not something a worksheet formula can do reliably.
    df["Best Rating"] = df["Credit Rating(s)"].map(best_grade)
    df["Category of Instrument"] = col("category")
    df["Rate of Interest"] = col("coupon")
    df["Original Allotment Date"] = pd.to_datetime(col("allotment"), errors="coerce")
    df["Maturity Date"] = pd.to_datetime(col("maturity"), errors="coerce")
    df["Type of Issue"] = [
        derive_type_of_issue(ld, al)
        for ld, al in zip(df[COL_DATE], df["Original Allotment Date"])
    ]

    perp = col("perpetual").fillna(False).astype(bool)
    df.loc[perp, "Data Notes"] = (df.loc[perp, "Data Notes"]
                                  .map(lambda s: "; ".join(filter(None, [s, "perpetual"]))))

    nsdl_names = col("nsdl_name")
    df["Clean Issuer Name"] = [
        clean_name(n) if isinstance(n, str) and n.strip() else clean_name(src)
        for n, src in zip(nsdl_names, df[COL_NAME])
    ]

    df = df[[c for c in OUTPUT_ORDER if c in df.columns]]
    top50 = (df.groupby("Clean Issuer Name")[COL_AMT].sum()
               .sort_values(ascending=False).head(50).index.tolist())
    return df, top50, exceptions, audit


# Chart field names must stay free of "." and "[]" - Vega-Lite parses those as
# field paths. The units live in AMT_FIELD_LABEL, used only for display.
AMT_FIELD = "Amount"
AMT_FIELD_LABEL = "Amount (Rs. Crores)"


def build_summary(df):
    """Breakdowns used by both the dashboard and the Summary sheet."""
    def by(field):
        g = (df.groupby(df[field].fillna("Not reported"))
               .agg(Issuances=(COL_ISIN, "size"), Amount=(COL_AMT, "sum"))
               .sort_values("Amount", ascending=False).reset_index())
        g.columns = [field, "Issuances", AMT_FIELD]
        return g

    out = {"Exchange": by("Exchange")}
    out["Type of Issue"] = by("Type of Issue")
    out["Industry of Issuer"] = by("Industry of Issuer")
    out["Type of Issuer"] = by("Type of Issuer")

    gdf = df.assign(Grade=df["Best Rating"].fillna("Unrated"))
    g = (gdf.groupby("Grade").agg(Issuances=(COL_ISIN, "size"), Amount=(COL_AMT, "sum"))
         .reset_index())
    g["_ord"] = g["Grade"].map(lambda x: GRADE_ORDER.index(x) if x in GRADE_ORDER else 99)
    out["Credit Rating"] = g.sort_values("_ord").drop(columns="_ord")

    daily = (df.groupby(df[COL_DATE].dt.date)
               .agg(Issuances=(COL_ISIN, "size"), Amount=(COL_AMT, "sum"))
               .reset_index())
    daily.columns = ["Listing Date", "Issuances", AMT_FIELD]
    out["Daily"] = daily

    issuers = (df.groupby("Clean Issuer Name")
                 .agg(Issuances=(COL_ISIN, "size"), Amount=(COL_AMT, "sum"))
                 .sort_values("Amount", ascending=False).reset_index())
    issuers.columns = ["Issuer", "Issuances", AMT_FIELD]
    out["Issuers"] = issuers
    return out


# --------------------------------------------------------------------------
# Excel output (writes to a BytesIO stream for the download button)
# --------------------------------------------------------------------------
def unique_issuer_ratings(series):
    """Aggregate an issuer's per-ISIN rating strings into a deduplicated
    'Agency: Grade' list (outlooks stripped so they don't create near-dupes)."""
    seen, out = set(), []
    for s in series.dropna():
        for part in str(s).split(";"):
            p = re.sub(r"\s*\([^)]*\)\s*$", "", part.strip())
            p = re.sub(r"\s+", " ", p)      # NSDL spacing variants ('AA+' vs 'AA +')
            if p and p not in seen:
                seen.add(p)
                out.append(p)
    return "; ".join(out)


def _cellval(v):
    """openpyxl cannot serialise NaN/NaT/pd.NA - hand it None instead."""
    if v is None or v is pd.NaT:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    return v


def _add_charts_sheet(wb, ws_s, positions):
    """Native Excel charts mirroring the dashboard, fed from the Summary sheet.

    Referencing Summary (instead of pasting images) keeps the charts editable
    and restylable in Excel, and they redraw if someone edits a Summary figure.
    """
    from openpyxl.chart import BarChart, Reference
    from openpyxl.chart.shapes import GraphicalProperties
    from openpyxl.drawing.line import LineProperties
    from openpyxl.styles import Font

    ws = wb.create_sheet("Charts")
    ws["A1"] = "NCD Listings - Charts"
    ws["A1"].font = Font(name="Arial", size=13, bold=True, color="1F4E79")
    ws["A2"] = "Amounts in Rs. crores. Source figures are on the Summary sheet."
    ws["A2"].font = Font(name="Arial", size=9, italic=True, color="808080")

    def chart(title, horizontal=False, width=16.0, height=7.5, max_rows=None,
              date_axis=False):
        hdr, first, last = positions[title]
        if last < first:
            return None                   # empty table - nothing to plot
        if max_rows:
            last = min(last, first + max_rows - 1)
        ch = BarChart()
        ch.type = "bar" if horizontal else "col"
        ch.title = title
        ch.style = 10
        ch.legend = None
        ch.width, ch.height = width, height
        ch.add_data(Reference(ws_s, min_col=3, min_row=hdr, max_row=last),
                    titles_from_data=True)
        ch.set_categories(Reference(ws_s, min_col=1, min_row=first, max_row=last))
        ch.series[0].graphicalProperties.solidFill = "1F4E79"
        ch.series[0].graphicalProperties.line.solidFill = "1F4E79"
        ch.gapWidth = 60
        ch.y_axis.numFmt = "#,##0"
        # openpyxl 3.1 marks axes as deleted by default; recent Excel honours it
        # and draws charts with no axis labels at all.
        ch.x_axis.delete = False
        ch.y_axis.delete = False
        # Give the title its own space; by default Excel lays it over the bars.
        ch.title.overlay = False
        # Default gridlines render as heavy black rules - soften them.
        ch.y_axis.majorGridlines.spPr = GraphicalProperties(
            ln=LineProperties(solidFill="D9D9D9"))
        if date_axis:
            ch.x_axis.number_format = "dd-mmm"
        if horizontal:
            # Bar charts plot the first category at the bottom; flip so the
            # largest issuer/industry reads first, as on the dashboard. Flipping
            # also drags the value axis to the top unless it is re-anchored.
            ch.x_axis.scaling.orientation = "maxMin"
            ch.y_axis.crosses = "max"
        return ch

    layout = [
        ("Daily Listing Activity", "A4",  dict(width=33.0, date_axis=True)),
        ("Exchange",               "A20", {}),
        ("Credit Rating",          "K20", {}),
        ("Type of Issue",          "A36", {}),
        ("Type of Issuer",         "K36", {}),
        ("Top 15 Issuers",         "A52", dict(horizontal=True, width=33.0, height=11.0)),
        ("Industry of Issuer",     "A75", dict(horizontal=True, width=33.0, height=10.0,
                                               max_rows=12)),
    ]
    for title, anchor, opts in layout:
        ch = chart(title, **opts)
        if ch is not None:
            ws.add_chart(ch, anchor)
    return ws


def build_workbook_bytes(df, top_n, exceptions, audit, summary):
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Border, Side, Alignment
    from openpyxl.utils import get_column_letter

    HDR_FILL = PatternFill("solid", fgColor="1F4E79")
    ALT_FILL = PatternFill("solid", fgColor="DDEBF7")
    HDR_FONT = Font(name="Arial", size=10, bold=True, color="FFFFFF")
    BODY_FONT = Font(name="Arial", size=10)
    TITLE_FONT = Font(name="Arial", size=13, bold=True, color="1F4E79")
    THIN = Side(style="thin", color="BFBFBF")
    BORDER = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)
    CENTER = Alignment(horizontal="center", vertical="center")
    LEFT = Alignment(horizontal="left", vertical="center")

    wb = Workbook()

    # ---------------- Sheet 1: Enriched_Data ----------------
    ws = wb.active
    ws.title = "Enriched_Data"
    ws.append(list(df.columns))
    for row in df.itertuples(index=False, name=None):
        ws.append([_cellval(v) for v in row])

    ncols, nrows = df.shape[1], df.shape[0] + 1
    date_cols = {i + 1 for i, c in enumerate(df.columns) if "Date" in str(c)}
    num_fmt_cols = {i + 1 for i, c in enumerate(df.columns) if "Crores" in str(c)}
    for j in range(1, ncols + 1):
        c = ws.cell(row=1, column=j)
        c.font, c.fill, c.border, c.alignment = HDR_FONT, HDR_FILL, BORDER, CENTER
    for i in range(2, nrows + 1):
        for j in range(1, ncols + 1):
            c = ws.cell(row=i, column=j)
            c.font, c.border = BODY_FONT, BORDER
            if j in date_cols and c.value is not None and not isinstance(c.value, str):
                c.number_format = "DD-MM-YYYY"
                c.alignment = CENTER
            elif j in num_fmt_cols:
                c.number_format = "#,##0.00"
            elif isinstance(c.value, (int, float)):
                c.alignment = CENTER
            else:
                c.alignment = LEFT
    for j, colname in enumerate(df.columns, 1):
        max_len = max([len(str(colname))] +
                      [len(str(v)) for v in df.iloc[:, j - 1].head(200)])
        ws.column_dimensions[get_column_letter(j)].width = min(max(10, max_len + 2), 45)
    ws.freeze_panes = "C2"
    ws.auto_filter.ref = f"A1:{get_column_letter(ncols)}{nrows}"

    clean_col_letter = get_column_letter(list(df.columns).index("Clean Issuer Name") + 1)
    amt_col_letter = get_column_letter(list(df.columns).index(COL_AMT) + 1)

    # ---------------- Sheet 2: Summary ----------------
    ws_s = wb.create_sheet("Summary")
    ws_s["A1"] = "NCD Listings - Summary"
    ws_s["A1"].font = TITLE_FONT
    # Every figure on this sheet is a formula over Enriched_Data, so filling in
    # a blank Type of Issuer (or correcting any value) there flows straight
    # through to these totals and to the Charts sheet built on them. Whole-
    # column references keep rows appended below the data in scope; the
    # ISIN "<>" condition stops the empty rows beneath from counting as blanks.
    def data_col(name):
        return f"Enriched_Data!${get_column_letter(list(df.columns).index(name) + 1)}:$" \
               f"{get_column_letter(list(df.columns).index(name) + 1)}"

    isin_rng, amt_rng = data_col(COL_ISIN), data_col(COL_AMT)
    name_rng = data_col("Clean Issuer Name")
    ws_s["A2"] = (f'=TEXT(COUNTA({isin_rng})-1,"#,##0")&" listings | Rs. "'
                  f'&TEXT(SUM({amt_rng}),"#,##0.00")&" crores | "'
                  f'&TEXT(SUMPRODUCT(({name_rng}<>"")/COUNTIF({name_rng},{name_rng}&""))-1,"#,##0")'
                  f'&" issuers | generated {datetime.now():%d-%m-%Y %H:%M}"')
    ws_s["A2"].font = Font(name="Arial", size=9, italic=True, color="808080")

    # title -> (table of starting categories, Enriched_Data column it counts,
    #           label used for blank cells or None, extra categories to always list)
    specs = [
        ("Exchange", summary["Exchange"], "Exchange", None, []),
        ("Type of Issue", summary["Type of Issue"], "Type of Issue", "Not reported",
         ["New Issue", "Re-Issue"]),
        ("Credit Rating", summary["Credit Rating"], "Best Rating", "Unrated", []),
        ("Type of Issuer", summary["Type of Issuer"], "Type of Issuer", "Not reported",
         ["Non PSU", "Public Sector Undertaking (PSU)"]),
        ("Industry of Issuer", summary["Industry of Issuer"], "Industry of Issuer",
         "Not reported", []),
        ("Daily Listing Activity", summary["Daily"], COL_DATE, None, []),
        ("Top 15 Issuers", summary["Issuers"].head(15), "Clean Issuer Name", None, []),
    ]
    positions = {}                       # title -> (header row, first data row, last data row)
    ITALIC = Font(name="Arial", size=10, italic=True, color="808080")
    BOLD = Font(name="Arial", size=10, bold=True)

    row = 4
    for title, tbl, field, blank_label, always in specs:
        rng = data_col(field)
        ws_s.cell(row=row, column=1, value=title).font = Font(
            name="Arial", size=11, bold=True, color="1F4E79")
        row += 1
        hdr = row
        for j, h in enumerate(tbl.columns, 1):
            label = AMT_FIELD_LABEL if h == AMT_FIELD else str(h)
            c = ws_s.cell(row=row, column=j, value=label)
            c.font, c.fill, c.border, c.alignment = HDR_FONT, HDR_FILL, BORDER, CENTER
        row += 1

        cats = [v for v in tbl.iloc[:, 0].tolist() if v != blank_label]
        cats += [a for a in always if a not in cats]
        if blank_label:
            cats.append(blank_label)     # always last, so it's easy to watch shrink
        first = row
        for cat in cats:
            is_blank = blank_label is not None and cat == blank_label
            crit = '""' if is_blank else f"$A{row}"
            label = cat.to_pydatetime() if isinstance(cat, pd.Timestamp) else cat
            if isinstance(label, date) and not isinstance(label, datetime):
                label = datetime(label.year, label.month, label.day)
            ws_s.cell(row=row, column=1, value=label)
            ws_s.cell(row=row, column=2,
                      value=f'=COUNTIFS({isin_rng},"<>",{rng},{crit})')
            ws_s.cell(row=row, column=3,
                      value=f'=SUMIFS({amt_rng},{isin_rng},"<>",{rng},{crit})')
            for j in range(1, 4):
                c = ws_s.cell(row=row, column=j)
                c.font, c.border = BODY_FONT, BORDER
            ws_s.cell(row=row, column=2).alignment = CENTER
            ws_s.cell(row=row, column=3).number_format = "#,##0.00"
            if isinstance(label, datetime):
                ws_s.cell(row=row, column=1).number_format = "DD-MM-YYYY"
                ws_s.cell(row=row, column=1).alignment = LEFT
            row += 1
        last = row - 1
        positions[title] = (hdr, first, last)

        if title not in ("Daily Listing Activity", "Top 15 Issuers"):
            # Anything typed into Enriched_Data that isn't one of the rows above
            # lands here instead of silently vanishing from the totals.
            ws_s.cell(row=row, column=1, value="Other (not listed above)").font = ITALIC
            ws_s.cell(row=row, column=2,
                      value=f'=COUNTA({isin_rng})-1-SUM(B{first}:B{last})').font = ITALIC
            ws_s.cell(row=row, column=3,
                      value=f'=SUM({amt_rng})-SUM(C{first}:C{last})').font = ITALIC
            ws_s.cell(row=row, column=2).alignment = CENTER
            ws_s.cell(row=row, column=3).number_format = "#,##0.00"
            row += 1
            ws_s.cell(row=row, column=1, value="Total").font = BOLD
            ws_s.cell(row=row, column=2, value=f"=SUM(B{first}:B{row - 1})").font = BOLD
            ws_s.cell(row=row, column=3, value=f"=SUM(C{first}:C{row - 1})").font = BOLD
            ws_s.cell(row=row, column=2).alignment = CENTER
            ws_s.cell(row=row, column=3).number_format = "#,##0.00"
            for j in range(1, 4):
                ws_s.cell(row=row, column=j).border = Border(
                    top=Side(style="thin"), bottom=Side(style="double"))
            row += 1
        row += 1
    for j, w in zip(range(1, 4), [40, 14, 24]):
        ws_s.column_dimensions[get_column_letter(j)].width = w

    # ---------------- Sheet 3: Charts ----------------
    _add_charts_sheet(wb, ws_s, positions)

    # ---------------- Sheet 4: Top_50_Issuers ----------------
    ws2 = wb.create_sheet("Top_50_Issuers")
    ws2["A1"] = "Top 50 Issuers by Amount Raised"
    ws2["A1"].font = TITLE_FONT
    ws2["A3"] = ("Note: Counts, amounts, Type and Industry are live formulas against the "
                 "Enriched_Data sheet (COUNTIFS/SUMIFS and INDEX/MATCH first-occurrence on "
                 "'Clean Issuer Name'; amounts include re-issues; blank = not populated in "
                 "NSDL's records). Credit Rating(s) is the deduplicated set of Agency: Grade "
                 "pairs across all the issuer's ISINs (outlooks omitted), computed at generation.")
    ws2["A3"].font = Font(name="Arial", size=8, italic=True, color="808080")

    ratings_map = (df.groupby("Clean Issuer Name")["Credit Rating(s)"]
                     .apply(unique_issuer_ratings).to_dict())

    headers = ["Rank", "Issuer", "Type of Issuer", "Industry of Issuer",
               "Credit Rating(s)", "No. of Issuances",
               "Amount Raised (Rs. Crores)", "% of Total"]
    hdr_row = 5
    for j, h in enumerate(headers, 1):
        c = ws2.cell(row=hdr_row, column=j, value=h)
        c.font, c.fill, c.border, c.alignment = HDR_FONT, HDR_FILL, BORDER, CENTER

    type_col_letter = get_column_letter(list(df.columns).index("Type of Issuer") + 1)
    ind_col_letter = get_column_letter(list(df.columns).index("Industry of Issuer") + 1)
    data_range = f"Enriched_Data!${clean_col_letter}$2:${clean_col_letter}${nrows}"
    amt_range = f"Enriched_Data!${amt_col_letter}$2:${amt_col_letter}${nrows}"
    type_range = f"Enriched_Data!${type_col_letter}$2:${type_col_letter}${nrows}"
    ind_range = f"Enriched_Data!${ind_col_letter}$2:${ind_col_letter}${nrows}"
    first_data, last_data = hdr_row + 1, hdr_row + len(top_n)
    for i, issuer in enumerate(top_n, 0):
        r = first_data + i
        ws2.cell(row=r, column=1, value=i + 1)
        ws2.cell(row=r, column=2, value=issuer)
        ws2.cell(row=r, column=3,
                 value=(f'=IFERROR(IF(INDEX({type_range},MATCH($B{r},{data_range},0))="","",'
                        f'INDEX({type_range},MATCH($B{r},{data_range},0))),"")'))
        ws2.cell(row=r, column=4,
                 value=(f'=IFERROR(IF(INDEX({ind_range},MATCH($B{r},{data_range},0))="","",'
                        f'INDEX({ind_range},MATCH($B{r},{data_range},0))),"")'))
        rc = ws2.cell(row=r, column=5, value=ratings_map.get(issuer, ""))
        rc.alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)
        ws2.cell(row=r, column=6, value=f'=COUNTIFS({data_range},$B{r})')
        ws2.cell(row=r, column=7, value=f'=SUMIFS({amt_range},{data_range},$B{r})')
        ws2.cell(row=r, column=8, value=f'=G{r}/SUM({amt_range})')
        for j in range(1, 9):
            c = ws2.cell(row=r, column=j)
            c.font, c.border = BODY_FONT, BORDER
            if i % 2 == 1:
                c.fill = ALT_FILL
        ws2.cell(row=r, column=1).alignment = CENTER
        ws2.cell(row=r, column=3).alignment = CENTER
        ws2.cell(row=r, column=6).alignment = CENTER
        ws2.cell(row=r, column=7).number_format = "#,##0.00"
        ws2.cell(row=r, column=8).number_format = "0.0%"

    tr = last_data + 1
    ws2.cell(row=tr, column=2, value=f"Total (Top {len(top_n)})")
    ws2.cell(row=tr, column=6, value=f"=SUM(F{first_data}:F{last_data})")
    ws2.cell(row=tr, column=7, value=f"=SUM(G{first_data}:G{last_data})")
    ws2.cell(row=tr, column=8, value=f"=SUM(H{first_data}:H{last_data})")
    for j in range(1, 9):
        c = ws2.cell(row=tr, column=j)
        c.font = Font(name="Arial", size=10, bold=True)
        c.border = Border(top=Side(style="double"), bottom=THIN, left=THIN, right=THIN)
    ws2.cell(row=tr, column=7).number_format = "#,##0.00"
    ws2.cell(row=tr, column=8).number_format = "0.0%"
    for j, w in zip(range(1, 9), [7, 46, 14, 32, 42, 15, 22, 10]):
        ws2.column_dimensions[get_column_letter(j)].width = w
    ws2.freeze_panes = f"A{first_data}"

    # ---------------- Sheet 5: Reconciliation ----------------
    ws4 = wb.create_sheet("Reconciliation")
    ws4["A1"] = "Cross-file reconciliation"
    ws4["A1"].font = TITLE_FONT
    ws4["A2"] = ("Rows the common file duplicated have been removed. Anything below is a "
                 "disagreement between the three source files that was resolved "
                 "automatically - review if the amounts matter.")
    ws4["A2"].font = Font(name="Arial", size=8, italic=True, color="808080")
    ws4.append([])
    ws4.append(["ISIN", "Issue", "Detail"])
    for j in range(1, 4):
        c = ws4.cell(row=4, column=j)
        c.font, c.fill, c.border, c.alignment = HDR_FONT, HDR_FILL, BORDER, CENTER
    if len(audit):
        for _, r in audit.iterrows():
            ws4.append([_cellval(v) for v in r])
    else:
        ws4.append(["-", "No conflicts", "The three files agreed on every shared ISIN"])
    for row_cells in ws4.iter_rows(min_row=5):
        for c in row_cells:
            c.font, c.border = BODY_FONT, BORDER
            c.alignment = LEFT
    for j, w in zip(range(1, 4), [16, 28, 80]):
        ws4.column_dimensions[get_column_letter(j)].width = w

    # ---------------- Sheet 6: Exceptions ----------------
    ws3 = wb.create_sheet("Exceptions")
    ws3.append(["ISIN", "Company Name (as per source file)", "Failed / Empty Lookups"])
    for j in range(1, 4):
        c = ws3.cell(row=1, column=j)
        c.font, c.fill, c.border, c.alignment = HDR_FONT, HDR_FILL, BORDER, CENTER
    if exceptions:
        for isin, name, failed in exceptions:
            ws3.append([isin, name, ", ".join(failed)])
    else:
        ws3.append(["-", "All ISINs resolved successfully", "-"])
    for row_cells in ws3.iter_rows(min_row=2):
        for c in row_cells:
            c.font, c.border = BODY_FONT, BORDER
    for j, w in zip(range(1, 4), [16, 50, 30]):
        ws3.column_dimensions[get_column_letter(j)].width = w

    # openpyxl writes formulas without cached results; force a full recalc on
    # open so the Summary, Charts and Top 50 all show numbers immediately.
    from openpyxl.workbook.properties import CalcProperties
    wb.calculation = CalcProperties(fullCalcOnLoad=True)

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.getvalue()


# ==========================================================================
# Streamlit UI
# ==========================================================================
ACCENT = "#1F4E79"
EXCHANGE_COLORS = ["#1F4E79", "#2E86AB", "#8AB8D8"]


def _bar(data, x, y, color=None, horizontal=False, height=280, fmt=",.0f"):
    """One bar chart. Category labels stay horizontal and un-truncated - a
    rotated, clipped issuer name is worse than no chart at all."""
    import altair as alt
    if horizontal:
        enc_x = alt.X(f"{y}:Q", title=None,
                      axis=alt.Axis(format="~s", tickCount=5))
        enc_y = alt.Y(f"{x}:N", sort="-x", title=None,
                      axis=alt.Axis(labelLimit=260))
    else:
        enc_x = alt.X(f"{x}:N", sort="-y", title=None,
                      axis=alt.Axis(labelAngle=0, labelLimit=150))
        enc_y = alt.Y(f"{y}:Q", title=None,
                      axis=alt.Axis(format="~s", tickCount=5))
    enc = dict(x=enc_x, y=enc_y,
               tooltip=[alt.Tooltip(f"{x}:N"),
                        alt.Tooltip(f"{y}:Q", title="Rs. crores", format=fmt)])
    if color:
        enc["color"] = alt.Color(f"{color}:N", legend=None,
                                 scale=alt.Scale(range=EXCHANGE_COLORS))
    else:
        enc["color"] = alt.value(ACCENT)
    return alt.Chart(data).mark_bar(cornerRadius=2).encode(**enc).properties(height=height)


def render_dashboard(df, summary):
    import altair as alt
    import streamlit as st

    total = df[COL_AMT].sum()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Listings", f"{len(df):,}")
    c2.metric("Capital listed", f"Rs. {total:,.0f} cr")
    c3.metric("Issuers", f"{df['Clean Issuer Name'].nunique():,}")
    both = int((df["Exchange"] == "Both").sum())
    c4.metric("Listed on both", f"{both:,}",
              help="Counted once here; the common file's duplicate rows were removed.")

    st.markdown("##### Listing activity")
    daily = summary["Daily"].copy()
    daily["Listing Date"] = pd.to_datetime(daily["Listing Date"])
    st.altair_chart(
        alt.Chart(daily).mark_bar(cornerRadius=2, color=ACCENT).encode(
            x=alt.X("Listing Date:T", title=None,
                    axis=alt.Axis(format="%d %b", labelAngle=0, tickCount=8)),
            y=alt.Y(f"{AMT_FIELD}:Q", title="Rs. crores",
                    axis=alt.Axis(format="~s")),
            tooltip=[alt.Tooltip("Listing Date:T", format="%d %b %Y"),
                     alt.Tooltip("Issuances:Q"),
                     alt.Tooltip(f"{AMT_FIELD}:Q", title="Rs. crores", format=",.2f")],
        ).properties(height=220), width="stretch")

    left, right = st.columns(2)
    with left:
        st.markdown("##### By exchange")
        st.altair_chart(_bar(summary["Exchange"], "Exchange", AMT_FIELD,
                             color="Exchange", height=240), width="stretch")
        st.markdown("##### New issues vs re-issues")
        st.altair_chart(_bar(summary["Type of Issue"], "Type of Issue",
                             AMT_FIELD, height=240), width="stretch")
    with right:
        st.markdown("##### By credit rating")
        st.altair_chart(_bar(summary["Credit Rating"], "Grade",
                             AMT_FIELD, height=240), width="stretch")
        st.markdown("##### By type of issuer")
        st.altair_chart(_bar(summary["Type of Issuer"], "Type of Issuer",
                             AMT_FIELD, height=240), width="stretch")

    st.markdown("##### Top 15 issuers")
    st.altair_chart(_bar(summary["Issuers"].head(15), "Issuer",
                         AMT_FIELD, horizontal=True, height=380,
                         fmt=",.2f"), width="stretch")

    st.markdown("##### By industry")
    st.altair_chart(_bar(summary["Industry of Issuer"].head(12), "Industry of Issuer",
                         AMT_FIELD, horizontal=True, height=320,
                         fmt=",.2f"), width="stretch")


def _table_format(st):
    """Render dates as dates and crores to two places in the on-screen table."""
    cfg = {COL_AMT: st.column_config.NumberColumn(format="%.2f")}
    for c in (COL_DATE, "Original Allotment Date", "Maturity Date"):
        cfg[c] = st.column_config.DateColumn(format="DD-MM-YYYY")
    return cfg


def main():
    import streamlit as st

    st.set_page_config(page_title="NCD Data Analysis", page_icon="📊", layout="wide")
    st.markdown("<h1 style='text-align:center;'>NCD Data Analysis</h1>",
                unsafe_allow_html=True)
    st.markdown("<p style='text-align:center;opacity:0.7;'>Private placements</p>",
                unsafe_allow_html=True)

    broken = repair_ca_bundle_env()
    if broken:
        st.caption("Ignored a broken CA-bundle setting on this machine "
                   f"({', '.join(broken)}) and fell back to certifi.")

    uploads = st.file_uploader(
        "Upload the BSE, NSE and common files (any order, any sheet layout)",
        type=["xlsx", "xls"], accept_multiple_files=True)

    if not uploads:
        cached = len(json.loads(CACHE_FILE.read_text())) if CACHE_FILE.exists() else 0
        st.info("Drop in this month's three files. **Private placements only** - "
                "public-issue sheets are skipped. Only **Company Name, Listing Date, "
                "ISIN and Capital Listed** need to be present - headers, sheet names "
                "and column order can change freely. Everything else is looked up from "
                "NSDL using the ISIN."
                + (f"\n\nLocal cache holds {cached:,} previously fetched API responses."
                   if cached else ""))
        return

    # --- classify each upload, let the user correct a wrong guess ---
    st.markdown("##### Files")
    roles, cols = {}, st.columns(min(3, len(uploads)))
    options = ["BSE", "NSE", "COMMON", "Ignore"]
    for i, up in enumerate(uploads):
        guess = classify_source(up.name)
        with cols[i % len(cols)]:
            roles[up.name] = st.selectbox(
                up.name, options,
                index=options.index(guess) if guess in options else 0,
                key=f"role_{i}")

    # --- parse ---
    frames, reports = {}, []
    for up in uploads:
        role = roles[up.name]
        if role == "Ignore":
            continue
        frame, report = read_source_file(up.getvalue(), up.name, role)
        reports.extend(report)
        if not frame.empty:
            frames[role] = (pd.concat([frames[role], frame], ignore_index=True)
                            if role in frames else frame)

    rep_df = pd.DataFrame(reports)
    total_rows = sum(len(f) for f in frames.values())
    with st.expander(f"Sheets read - {total_rows:,} source rows "
                     f"across {len(rep_df)} sheet(s)", expanded=total_rows == 0):
        st.dataframe(rep_df, hide_index=True)
        st.caption("Column detection is by content first, header text second. "
                   "Check the mapping above if a month's format changes.")

    if total_rows == 0:
        st.error("No usable ISIN rows found. Confirm the files carry Company Name, "
                 "Listing Date, ISIN and Capital Listed columns.")
        return

    missing = [r for r in ("BSE", "NSE", "COMMON") if r not in frames]
    if missing:
        st.warning(f"No rows read for: {', '.join(missing)}. "
                   "Proceeding with what was uploaded.")

    # Streamlit reruns the whole script on every interaction, and a button is
    # only True on the run straight after its click - so anything drawn under
    # `if st.button(...)` vanishes on the next rerun. Build once, keep the
    # result in session state, and draw from there. The signature ties the
    # stored result to these exact files and roles, so changing the upload
    # clears it instead of showing stale numbers.
    signature = tuple((up.name, up.size, roles[up.name]) for up in uploads)

    if st.button("Build consolidated NCD sheet", type="primary"):
        prog = st.progress(0.0, text="Fetching from NSDL...")

        def cb(done, total):
            prog.progress(done / total, text=f"Fetching from NSDL... {done}/{total} ISINs")

        t0 = time.time()
        try:
            df, top50, exceptions, audit = run_pipeline(frames, progress_cb=cb)
        except ValueError as e:
            prog.empty()
            st.session_state.pop("result", None)
            st.error(str(e))
            return
        prog.progress(1.0, text=f"Done in {time.time() - t0:.0f}s")
        summary = build_summary(df)
        st.session_state["result"] = dict(
            signature=signature, df=df, exceptions=exceptions, audit=audit,
            summary=summary, removed=total_rows - len(df),
            xlsx=build_workbook_bytes(df, top50, exceptions, audit, summary),
            file_name=f"NCD_Enriched_{datetime.now():%Y-%m-%d}.xlsx")

    result = st.session_state.get("result")
    if not result or result["signature"] != signature:
        return
    df, exceptions, audit, summary = (result["df"], result["exceptions"],
                                      result["audit"], result["summary"])

    st.success(f"{len(df):,} listings after removing {result['removed']:,} duplicated "
               f"row(s) carried by the common file.")
    # on_click="ignore": the file downloads in the browser and the page stays
    # exactly as it is - no rerun.
    st.download_button("\u2b07 Download enriched workbook (.xlsx)", data=result["xlsx"],
                       file_name=result["file_name"],
                       mime=("application/vnd.openxmlformats-officedocument"
                             ".spreadsheetml.sheet"),
                       type="primary", on_click="ignore")

    tabs = st.tabs(["Dashboard", "Enriched data", "Top 50 issuers",
                    f"Reconciliation ({len(audit)})",
                    f"Exceptions ({len(exceptions)})"])
    with tabs[0]:
        render_dashboard(df, summary)
    with tabs[1]:
        # Rate of Interest mixes numbers with labels ("Floating Rate"), which
        # Arrow can't hold in one column. Show it as text on screen only; the
        # workbook keeps the numbers numeric.
        rate = df["Rate of Interest"].map(lambda v: "" if v is None or pd.isna(v) else str(v))
        st.dataframe(df.assign(**{"Rate of Interest": rate}), height=480,
                     hide_index=True, column_config=_table_format(st))
    with tabs[2]:
        top_df = (df.groupby("Clean Issuer Name")
                  .agg(Type_of_Issuer=("Type of Issuer", "first"),
                       Industry=("Industry of Issuer", "first"),
                       Credit_Ratings=("Credit Rating(s)", unique_issuer_ratings),
                       Issuances=(COL_AMT, "size"),
                       Amount_Rs_Crores=(COL_AMT, "sum"))
                  .sort_values("Amount_Rs_Crores", ascending=False).head(50)
                  .reset_index().rename(columns={"Clean Issuer Name": "Issuer"}))
        top_df.index += 1
        st.dataframe(top_df, height=480)
    with tabs[3]:
        if len(audit):
            st.caption("Disagreements between the three files, resolved automatically, "
                       "and any public issues removed from private-placement sheets.")
            st.dataframe(audit, hide_index=True)
        else:
            st.success("The three files agreed on every shared ISIN.")
    with tabs[4]:
        if exceptions:
            st.caption("NSDL has no record for these ISINs - typically securitised "
                       "debt instruments (PTCs, trusts) rather than NCDs. Source "
                       "columns are kept; enriched columns stay blank.")
            st.dataframe(pd.DataFrame(
                [(i, n, ", ".join(f)) for i, n, f in exceptions],
                columns=["ISIN", "Company Name", "Failed lookups"]), hide_index=True)
        else:
            st.success("Every ISIN resolved against NSDL.")


if __name__ == "__main__":
    main()
