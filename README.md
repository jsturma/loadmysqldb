# mysqldbgen

`mysqldbgen` is a small MySQL test database generator/loader that uses `go-faker/faker` to create “realistic-ish” demo data.

It will:

- create the target database if it doesn’t exist
- create a small schema (`payments`, `buying_stats`, `products`, `accounts`) if missing
- insert a configurable number of rows using a configurable number of worker goroutines

## What it creates

Tables (created if missing):

- **`payments`**: `p_md5`, `p_account_uuid` (FK->accounts), `p_amount`, `p_epoch`
- **`buying_stats`**: user/product UUIDs, quantity, total amount, epoch
- **`products`**: UUID, name, authors, price
- **`accounts`**: UUID, username/email/password, created epoch, last login epoch

## Install / build

This repo includes a `go.mod`, so building is straightforward:

```bash
cd /path/to/loadmysqldb
go build -o mysqldbgen ./mysqldbgen.go
```

## Usage

Run with flags only:

```bash
./mysqldbgen \
  -host localhost \
  -port 3306 \
  -user root \
  -password root \
  -dbname mytestdb \
  -numWorkers 5 \
  -dbRecords2Process 10000
```

Run with YAML config (plus any flag overrides you want):

```bash
./mysqldbgen -config ./bin/example.yaml -dbname mytestdb -dbRecords2Process 5000
```

### CLI flags

All flags have defaults; `-config` is optional.

- **`-host`**: MySQL host (default `localhost`)
- **`-port`**: MySQL port (default `3306`)
- **`-user`**: admin user used to create the DB and connect (default `root`)
- **`-password`**: admin password (default `root`)
- **`-dbname`**: database to create/populate (default `mytestdb`)
- **`-config`**: path to YAML config file (default empty)
- **`-numWorkers`**: number of concurrent workers inserting rows (default `3`)
- **`-dbRecords2Process`**: number of “records” to process (default `100`)
- **`-pcentOutput`**: progress output every X% (default `10`)
- **`-minDays`**: minimum “account created” offset in seconds (default `259200` = 3 days)
- **`-maxDays`**: maximum “account created” offset in seconds (default `31536000` = 1 year)
- **`-delayLastLogin`**: random last-login delay in seconds (default `500`)
- **`-runOnlyFaker`**: generate fake data but do not write to DB (default `false`)

## YAML configuration

See `bin/example.yaml` for a starter config. Keys map directly to CLI flags:

```yaml
host: localhost
port: 3306
user: root
password: root
dbname: mytestdb
runOnlyFaker: false
numWorkers: 5
dbRecords2Process: 100
pcentOutput: 5
minDays: 259200
maxDays: 31536000
delayLastLogin: 500
```

Notes:

- Any CLI flag you pass will override YAML values (YAML is applied first, then flags win by virtue of being explicitly provided at runtime).
- `pcentOutput` affects how frequently progress is logged; it’s converted internally into a “records per log line” count.

## Continuous loader helper

There’s a small bash helper that repeatedly runs `mysqldbgen` against any `*.yaml` / `*.yml` files in the current directory, choosing a semi-random record count and sleeping between runs:

- Bash: `bin/tools/bash/loadmysqldb.bash`

Example:

```bash
cd /path/with/configs
cp /path/to/loadmysqldb/bin/example.yaml ./local.yaml
/path/to/loadmysqldb/mysqldbgen -config ./local.yaml -dbname mytestdb -dbRecords2Process 1000
```

Or (continuous loop):

```bash
cd /path/with/configs
/path/to/loadmysqldb/bin/tools/bash/loadmysqldb.bash mytestdb
```

## Requirements

- Go toolchain (to build from source)
- MySQL reachable with credentials that can create databases

