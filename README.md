# blr-employment-stats

Tidy dataset of employee counts across establishments registered under the
Karnataka Shops and Commercial Establishments Act in Bengaluru.
Sourced from the Department of Labour's
[e-Karmika portal](https://www.ekarmika.karnataka.gov.in/ekarmika/UserCredentials/CertificateView.aspx)

View the raw data [here](https://hyparam.github.io/demos/hyparquet/?key=https%3A%2F%2Fraw.githubusercontent.com%2FVonter%2Fblr-employment-stats%2Fmain%2Fdata%2Femployment.parquet).

## Data

- [employment.parquet](https://raw.githubusercontent.com/Vonter/blr-employment-stats/main/data/employment.parquet): Registration certificate
  details and reported employee counts, as Parquet
- [employment.csv.zip](https://raw.githubusercontent.com/Vonter/blr-employment-stats/main/data/employment.csv.zip): Registration certificate
  details and reported employee counts, as zipped CSV
- [renewals.parquet](https://raw.githubusercontent.com/Vonter/blr-employment-stats/main/data/renewals.parquet): Certificate renewals, as
  Parquet
- [renewals.csv.zip](https://raw.githubusercontent.com/Vonter/blr-employment-stats/main/data/renewals.csv.zip): Certificate renewals, as
  zipped CSV

See [DATA.md](DATA.md) for the scope, schema and data dictionary.

## Scripts

- [fetch.py](fetch.py): Fetch raw certificates
- [parse.py](parse.py): Parse certificates into Parquet and compressed CSV datasets

Reusable and shared functionality live in [common.py](common.py).

## Generate

Install [uv](https://docs.astral.sh/uv/), then run:

```sh
uv sync
uv run fetch.py --index
uv run fetch.py
uv run parse.py
```

Fetching and parsing are incremental. The committed Parquet is the baseline.
Run any script with `--help` to list all the available options.

## License

This blr-employment-stats dataset is made available under the Open Database
License: http://opendatacommons.org/licenses/odbl/1.0/.
Some individual contents of the database are under copyright by the
Department of Labour, Government of Karnataka.

You are free:

* **To share**: To copy, distribute and use the database.
* **To create**: To produce works from the database.
* **To adapt**: To modify, transform and build upon the database.

As long as you:

* **Attribute**: You must attribute any public use of the database, or works produced from the database, in the manner specified in the ODbL. For any use or redistribution of the database, or works produced from it, you must make clear to others the license of the database and keep intact any notices on the original database.
* **Share-Alike**: If you publicly use any adapted version of this database, or works produced from an adapted database, you must also offer that adapted database under the ODbL.
* **Keep open**: If you redistribute the database, or an adapted version of it, then you may use technological measures that restrict the work (such as DRM) as long as you also redistribute a version without such measures.

## Credits

- [e-Karmika portal, Department of Labour, Government of Karnataka](https://www.ekarmika.karnataka.gov.in/ekarmika/UserCredentials/CertificateView.aspx)

## AI declaration

Components of this repository, including code and documentation, were written
with AI assistance.
