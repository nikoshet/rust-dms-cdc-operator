# Rust CDC-Validator

## Overview

The `rust-cdc-validator` is a Rust-based utility designed to compare the state of a table in an Amazon RDS (PostgreSQL) database with the data migrated from Amazon RDS to Amazon S3 using AWS Database Migration Service (DMS). This tool is particularly useful for validating the consistency of data between the RDS database and the Parquet files stored in S3, especially when the S3 files are populated with change data capture (CDC) updates, since DMS validation is not yet supported with S3 as target.


## Features

- Import a snapshot of the CDC parquet data stored in AWS S3 with date-based folder partitioning in a locally deployed Postgres
- Compare the state of a specific table in an Amazon RDS database with the data stored in Parquet files in the S3 bucket
- Identify differences at the row level by modifying chunk size


## Prerequisites

- Your source DB is a PostgreSQL
- You have a running AWS DMS task in FULL LOAD + CDC Mode
- The target of the task is AWS S3 with:
    - Parquet formatted files
    - date-based folder partitioning
    - Additional columns of `Op` and `_dms_ingestion_timestamp` injected by DMS


## Usage

```shell
Usage: rust-cdc-validator validate [OPTIONS] --bucket-name <BUCKET_NAME> --s3-prefix <S3_PREFIX> --postgres-url <POSTGRES_URL> --local-postgres-url <LOCAL_POSTGRES_URL> --table-name <TABLE_NAME> --start-date <START_DATE>

Options:
      --bucket-name <BUCKET_NAME>
          S3 Bucket name where the CDC files are stored
      --s3-prefix <S3_PREFIX>
          S3 Prefix where the files are stored Example: data/landing/rds/mydb
      --postgres-url <POSTGRES_URL>
          Url of the database to validate the CDC files Example: postgres://postgres:postgres@localhost:5432/mydb
      --local-postgres-url <LOCAL_POSTGRES_URL>
          Url of the local database to import the parquet files Example: postgres://postgres:postgres@localhost:5432/mydb
      --database-schema <DATABASE_SCHEMA>
          Schema of database to validate against S3 files [default: public]
      --table-name <TABLE_NAME>
          Table name to validate against S3 files
      --start-date <START_DATE>
          Start date to filter the Parquet files Example: 2024-02-14T10:00:00Z [default: 2024-02-14T10:00:00Z]
      --chunk-size <CHUNK_SIZE>
          Datadiff chunk size [default: 1000]
      --only-datadiff
          Run only the datadiff
      --only-snapshot
          Take only a snapshot from S3 to local DB
  -h, --help
          Print help
  -V, --version
          Print version

```


## Example

- Spin up the local Postgres DB through Docker Compose:
```shell
docker-compose up

psql -h localhost -p 5432 -U postgres -d mydb
```

- Build and run the Rust tool
```shell
cargo fmt --all
cargo clippy --all

cargo build

RUST_LOG=rust_cdc_validator=info,rust_pgdatadiff=info cargo run validate --bucket-name my-bucket --s3-prefix prefix/path --postgres-url postgres://postgres:postgres@localhost:5432/mydb1 --local-postgres-url postgres://postgres:postgres@localhost:5432/mydb2 --database-schema public --table-name mytable --start-date 2024-02-14T10:00:00Z --chunk-size 100
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
- [parquet](https://crates.io/crates/parquet)
- [sqlx](https://crates.io/crates/sqlx)


## License
This project is licensed under the MIT License 
