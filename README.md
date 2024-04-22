# Rust-CDC-Validator

## Overview

The `rust-cdc-validator` is a Rust-based utility designed to compare the state of a table in an Amazon RDS (PostgreSQL) database with the data migrated from Amazon RDS to Amazon S3 using AWS Database Migration Service (DMS). This tool is particularly useful for validating the consistency of data between the RDS database and the Parquet files stored in S3, especially when the S3 files are populated with change data capture (CDC) updates, since DMS validation is not yet supported with S3 as target.


## Features

- Import a snapshot of the CDC parquet data stored in AWS S3 with date-based folder partitioning in a locally deployed Postgres
- Specify a specific time range to replicate the S3 state on a Postgres
- Restore the RDS state from S3 in case of data loss
- Compare the state of a specific table in an Amazon RDS database with the data stored in Parquet files in the S3 bucket
- Identify differences at the row level by modifying the validated chunk size


## Prerequisites

- Your source DB is a PostgreSQL
- You have a running AWS DMS task in FULL LOAD + CDC Mode
- The target of the task is AWS S3 with:
    - Parquet formatted files
    - date-based folder partitioning
    - Additional columns of `Op` and `_dms_ingestion_timestamp` injected by DMS


## Usage

The tool provides two features for running it, which are `Inquire` and `Clap`.

### Using Clap
```shell
Usage: rust-cdc-validator validate [OPTIONS] --bucket-name <BUCKET_NAME> --s3-prefix <S3_PREFIX> --source-postgres-url <SOURCE_POSTGRES_URL> --target-postgres-url <TARGET_POSTGRES_URL> --table-names [<TABLE_NAMES>...] --start-date <START_DATE>

Options:
      --bucket-name <BUCKET_NAME>
          S3 Bucket name where the CDC files are stored
      --s3-prefix <S3_PREFIX>
          S3 Prefix where the files are stored Example: data/landing/rds/mydb
      --source-postgres-url <SOURCE_POSTGRES_URL>
          Url of the database to validate the CDC files Example: postgres://postgres:postgres@localhost:5432/mydb
      --target-postgres-url <TARGET_POSTGRES_URL>
          Url of the target database to import the parquet files Example: postgres://postgres:postgres@localhost:5432/mydb
      --database-schema <DATABASE_SCHEMA>
          Schema of database to validate against S3 files [default: public]
      --table-names [<TABLE_NAMES>...]
          List of table names to validate against S3 files
      --start-date <START_DATE>
          Start date to filter the Parquet files Example: 2024-02-14T10:00:00Z [default: 2024-02-14T10:00:00Z]
      --stop-date <STOP_DATE>
          Stop date to filter the Parquet files Example: 2024-02-14T10:00:00Z
      --chunk-size <CHUNK_SIZE>
          Datadiff chunk size [default: 1000]
      --max-connections <MAX_CONNECTIONS>
          Maximum connection pool size [default: 100]
      --start-position <START_POSITION>
          Datadiff start position [default: 0]
      --only-datadiff
          Run only the datadiff
      --only-snapshot
          Take only a snapshot from S3 to local DB
  -h, --help
          Print help
  -V, --version
          Print version

```

### Using Inquire
```shell
rust-cdc-validator --features="with-inquire"
```


## Example

- Spin up the local Postgres DB through Docker Compose:
```shell
docker-compose up

psql -h localhost -p 5438 -U postgres -d mydb
```

- Build and run the Rust tool
```shell
cargo fmt --all
cargo clippy --all

cargo build

RUST_LOG=rust_cdc_validator=info,rust_pgdatadiff=info cargo run --features="with-clap" validate --bucket-name my-bucket --s3-prefix prefix/path --source-postgres-url postgres://postgres:postgres@localhost:5432/mydb1 --target-postgres-url postgres://postgres:postgres@localhost:5438/mydb --database-schema public --table-name mytable --start-date 2024-02-14T10:00:00Z --chunk-size 100
```

For more debugging, you can enable Rust related logs by exporting the following:
```
export RUST_LOG=rust_cdc_validator=debug,rust_pgdatadiff=debug
```


## Crates

Some crates utilized that are worth mentioning:

- [polars](https://crates.io/crates/polars)
- [rust-pgdatadiff](https://crates.io/crates/rust-pgdatadiff)
- [tokio](https://crates.io/crates/tokio)
- [clap](https://crates.io/crates/clap)
- [inquire](https://crates.io/crates/inquire)
- [parquet](https://crates.io/crates/parquet)
- [sqlx](https://crates.io/crates/sqlx)


## License
This project is licensed under the MIT License 
