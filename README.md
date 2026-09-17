# NCD Data Analysis

Consolidates the three monthly NCD listing files (BSE additions, NSE additions,
and the BSE/NSE common list) into one enriched, deduplicated workbook, with a
dashboard in the browser.

**Private placements only.** Public-issue sheets (`Public Issue` / `Public`) are
skipped on read. As a backstop, any row NSDL records as a public issue is also
dropped and logged on the `Reconciliation` sheet.

## Run

```
pip install -r requirements.txt
streamlit run ncd_app.py
```

Drop in the three files in any order. Filenames are used to guess which is
which; correct the guess in the dropdown if needed.

## What the source files must contain

Only four columns, on any sheet, under any header text, in any order:

| Field | Recognised by |
|---|---|
| ISIN | the values themselves (`IN` + 10 chars) - header text is never used |
| Company Name | header keyword, else the first non-numeric, non-date column |
| Listing Date | header keyword, else the latest non-maturity date column |
| Capital Listed | header keyword, else the numeric column that isn't a row counter |

Handled automatically: header rows that aren't row 1, title and blank rows,
trailing `Total` / `NIL` / source-note rows, pivot and chart sheets, amounts
given in lakhs, and `PPDI` / `Private` sheet names (public-issue sheets are excluded).
The detected mapping is shown in the app - check it when a format changes.

## What gets added

Everything below is looked up from the ISIN via the NSDL India Bond Info public
API, so it survives any change to the source layout:

- **Exchange** - BSE / NSE / Both, from the dedup
- **Type of Issue** - New Issue or Re-Issue, derived from the gap between the
  listing date and NSDL's original allotment date
- **Rate of Interest**, **Maturity Date**, **Original Allotment Date**
- **Type of Issuer**, **Industry of Issuer**, **Credit Rating(s)**,
  **Category of Instrument**, **Clean Issuer Name**

Note that NSDL reports an ISIN's *original* allotment date, not the current
tranche's, so on re-issues that column is the first allotment, not this month's.

## Deduplication

Every ISIN in the common file also appears in both the BSE and the NSE file, so
the common file is redundant and is used to break ties rather than as a source
of rows. Rows are matched across files pair by pair on amount, never collapsed
on ISIN alone - an ISIN can list twice in one month on the same exchange (a
further issue), and those are real, separate rows.

Where the files disagree - a different listing date or a transposed digit in the
amount - the conflict is resolved (earlier date; common file's figure) and
logged on the `Reconciliation` sheet.

## Output workbook

1. `Enriched_Data` - one row per listing, enriched
2. `Summary` - COUNTIFS/SUMIFS formulas over `Enriched_Data` by exchange,
   issue type, rating, issuer type, industry, day and top 15 issuers. Edit
   `Enriched_Data` (e.g. fill a blank Type of Issuer) and the Summary and Charts
   follow. Each breakdown ends with an *Other* row, which picks up any value you
   type that isn't already listed, and a *Total*. The rating breakdown counts the
   `Best Rating` column, so edit that column when changing a rating.
3. `Charts` - native, editable Excel charts mirroring the dashboard, drawn from
   the Summary figures
4. `Top_50_Issuers` - live COUNTIFS/SUMIFS formulas against sheet 1
5. `Reconciliation` - cross-file disagreements and any excluded public issues
6. `Exceptions` - ISINs NSDL has no record of (typically securitised debt
   instruments - PTCs and trusts - rather than NCDs)

## Cache

`nsdl_cache.json` (next to the app) stores every successful API response;
already-seen ISINs resolve instantly. Public data, no secrets.
