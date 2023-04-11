# dbase-cli

`dbase-cli` is a command-line tool that allows you to query dBase (`.dbf`) files using SQL. Powered by the [`dbase-rs`](https://github.com/tmontaigu/dbase-rs) library for dBase file parsing and [`datafusion`](https://github.com/apache/arrow-datafusion) as the query engine, `dbase-cli` provides a fast and easy way to query dBase files.

## Features 🌟

- Query dBase files with SQL
- Choose between CSV, TSV, DSV, o r table output formats
- Supports multiple SQL statements in a single query

## Installation 🔧

Ensure you have Rust installed on your system. If not, follow the instructions [here](https://www.rust-lang.org/tools/install) to install Rust.

Then, clone the repository:

```
git clone https://github.com/casperhart/dbase-cli.git
cd dbase-cli
```

Build and install the binary:

```
cargo build --release
cargo install --path .
```

## Usage 💻

Run a SQL query on a dBase file:

```
dbase-cli -e "CREATE EXTERNAL TABLE orders STORED AS dbase LOCATION '/path/to/ORDERS.DBF'; SELECT order_id, customer_name, order_date FROM orders LIMIT 1;" --output-format tsv > output.tsv
```

Or, run a SQL query from a file:

```
dbase-cli -f sample_query.sql --output-format tsv > output.tsv
```

Where `sample_query.sql` contains:

```
CREATE EXTERNAL TABLE orders STORED AS dbase LOCATION '/path/to/ORDERS.DBF';

SELECT order_id, customer_name, order_date
FROM orders
LIMIT 1;
```

## Output Formats 📄

- CSV (`--output-format csv`)
- TSV (`--output-format tsv`)
- DSV (`--output-format dsv --delimiter-for-dsv '|'`)
- Table (default) (`--output-format table`)

## Contributing 🤝

Contributions, issues, and feature requests are welcome! Feel free to check the [issues page](https://github.com/casperhart/dbase-cli/issues).

## Show your support ⭐

If you find this project useful, give it a ⭐️!
