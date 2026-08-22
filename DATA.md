# blr-employment-stats

The source data is fetched from the
[e-Karmika portal, Department of Labour, Government of Karnataka](https://www.ekarmika.karnataka.gov.in/ekarmika/UserCredentials/CertificateView.aspx).

The dataset is published as a Parquet file and an equivalent zipped CSV:
- [employment.parquet](https://raw.githubusercontent.com/Vonter/blr-employment-stats/main/data/employment.parquet): Registration certificate
  details and reported employee counts, as Parquet
- [employment.csv.zip](https://raw.githubusercontent.com/Vonter/blr-employment-stats/main/data/employment.csv.zip): Registration certificate
  details and reported employee counts, as zipped CSV
- [renewals.parquet](https://raw.githubusercontent.com/Vonter/blr-employment-stats/main/data/renewals.parquet): Certificate renewals, as
  Parquet
- [renewals.csv.zip](https://raw.githubusercontent.com/Vonter/blr-employment-stats/main/data/renewals.csv.zip): Certificate renewals, as
  zipped CSV

View either Parquet file without downloading it:

- [employment](https://hyparam.github.io/demos/hyparquet/?key=https%3A%2F%2Fraw.githubusercontent.com%2FVonter%2Fblr-employment-stats%2Fmain%2Fdata%2Femployment.parquet)
- [renewals](https://hyparam.github.io/demos/hyparquet/?key=https%3A%2F%2Fraw.githubusercontent.com%2FVonter%2Fblr-employment-stats%2Fmain%2Fdata%2Frenewals.parquet)

The dataset is a snapshot of registration certificates issued under the
Karnataka Shops and Commercial Establishments Act, 1961 by the Department of
Labour, Government of Karnataka. It is not a measure of total employment, it
excludes unregistered workplaces, workers outside the Act's registered shops and
commercial establishments, and any activity absent from the certificates. The
e-Karmika portal handles registration and renewal of shops and commercial
establishments under the Act, since the 2010s onwards. Earlier manual records
were digitised and migrated, leading to some legacy source anomalies.

## Scope

The dataset covers the 198 wards in the BBMP ward system, served by 49 labour
offices. The time period is 1990 onwards.

## Data Dictionary

### `employment`

One row per registration certificate, sorted by `registration_date`, most
recent first. A certificate is not necessarily a unique real-world workplace
over all time, as establishments can be re-registered under a new number.

| Variable | Type | Description |
|----------|------|-------------|
| registration_no | string | Registration number in `office_number/ward_number/category/sequence/year` format |
| registration_date | date | Date of registration |
| ward_name | string | Older BBMP ward name, mapped from `ward_number` |
| establishment_type | string | `CE` for commercial establishment or `S` for shop |
| establishment_name | string | Name of the establishment |
| nature_of_business | string | Nature of business |
| address | string | Postal address of the establishment |
| employees_male | integer | Number of male employees printed on the certificate |
| employees_female | integer | Number of female employees printed on the certificate |
| employees_total | integer | Total employees printed on the certificate |
| registration_fee_rs | float | Registration fee printed on the certificate, in rupees |
| fee_payment_date | date | Registration fee payment date printed on the certificate |
| valid_upto | date | Date until which the registration is valid |
| reg_year | integer | Registration year from the registration number |
| ward_number | string | Older BBMP ward number |
| office_number | string | Labour office number |
| office | string | Issuing labour inspector office |

### `renewals`

One row per registration-certificate renewal term, uniquely keyed by
`registration_no` and `year`.

| Variable | Type | Description |
|----------|------|-------------|
| registration_no | string | Registration number of the renewed certificate |
| year | integer | Renewal term year covered by the row |
| renewal_date | date | Date the renewal was processed |
| employees_total | integer | Total employees printed for the renewal term |

A single renewal can cover a multi-year term, so `renewal_date` repeats across
that term's rows; `year` distinguishes them. `year` and the year of
`renewal_date` differ on many rows. A registration that lapses and is renewed
some years later has no rows for the intervening years. Only registrations with
at least one recorded renewal appear here.

## Notes

- Gaps or omissions in the list of establishments may be observed.
- The register does not establish that a listed establishment still operates.
  Certificate validity and renewal rows show recorded administrative coverage,
  not observed business activity.
- Source anomalies are present and retained as printed, without silent correction
  or imputation. These include registration fee dates in the distant past and future.
- `registration_date` can represent a later registration action or an original
  date on a re-issued legacy certificate. A few values fall outside the year in
  `registration_no`, precede 1990, follow `valid_upto`, or follow a renewal.
- `employees_total` is zero on some certificates. Where all three employee
  values are present, male plus female equals total.
- Fee, fee date, validity, registration date, and office values remain null
  where the source certificate omits them or text extraction failed to read them.
