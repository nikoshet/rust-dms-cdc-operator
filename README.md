# Rust CDC-Validator

## Overview

The `rust-cdc-validator` is a Rust-based utility designed to compare the state of a table in an Amazon RDS (PostgreSQL) database with the data migrated from Amazon RDS to Amazon S3 using AWS Database Migration Service (DMS). This tool is particularly useful for validating the consistency of data between the RDS database and the Parquet files stored in S3, especially when the S3 files are populated with change data capture (CDC) updates.

## Features

- Compare the state of a specific table in an Amazon RDS database with the data stored in Parquet files in an S3 bucket.
- Identify differences at the row level.
- Flexible comparison options, including direct comparison using a local PostgreSQL database or in-memory Rust struct dictionary comparison.

## Usage
```
```

## License
This project is licensed under the MIT License 
