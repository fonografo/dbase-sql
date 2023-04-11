use clap::{ArgGroup, Parser};
use csv::WriterBuilder;
use datafusion::arrow::error::Result;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::execution::context::SessionState;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::prelude::*;
use dbase::DbaseTableFactory;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
#[clap(group(
    ArgGroup::new("method")
        .required(true)
        .args(&["file", "execute"]),
))]
struct Args {
    /// Name of the person to greet
    #[arg(short = 'f')]
    file: Option<String>,

    /// Number of times to greet
    #[arg(short = 'e')]
    execute: Option<String>,

    #[arg(long)]
    output_format: Option<OutputFormatArg>,

    #[arg(long)]
    delimiter_for_dsv: Option<String>,
}

#[derive(clap::ValueEnum, Clone, Debug)]
enum OutputFormatArg {
    Csv,
    Tsv,
    Dsv,
    Table,
}

enum OutputFormat {
    Delimited(u8),
    Table,
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let args = Args::parse();

    if let Some(d) = &args.delimiter_for_dsv {
        if d.len() > 1 {
            panic!("delimiter must be a single byte")
        }
    }

    let cfg = RuntimeConfig::new();
    let env = RuntimeEnv::new(cfg).unwrap();
    let ses = SessionConfig::new();
    let mut state = SessionState::with_config_rt(ses, Arc::new(env));

    // add DbaseTableFactory to support "create external table stored as dbase" syntax
    state
        .table_factories_mut()
        .insert("DBASE".to_string(), Arc::new(DbaseTableFactory {}));

    let ctx = SessionContext::with_state(state);

    let mut query: String;

    if let Some(file) = args.file.as_deref() {
        let file = File::open(file)?;
        let reader = BufReader::new(file);
        query = String::new();

        for line in reader.lines() {
            let line = line?;
            query.push_str(&line);
            query.push(' ');
        }
    } else {
        query = args.execute.unwrap();
    }

    let statements: Vec<&str> = query
        .split(";")
        .filter(|statement| !statement.trim().is_empty())
        .collect();

    let output_format = match args.output_format {
        Some(c) => match c {
            OutputFormatArg::Csv => OutputFormat::Delimited(b','),
            OutputFormatArg::Tsv => OutputFormat::Delimited(b'\t'),
            OutputFormatArg::Dsv => match &args.delimiter_for_dsv {
                Some(s) => OutputFormat::Delimited(s.as_bytes()[0]),
                None => OutputFormat::Delimited(b'|'),
            },
            OutputFormatArg::Table => OutputFormat::Table,
        },
        None => OutputFormat::Table,
    };

    for statement in statements {
        let res = ctx.sql(statement).await?;

        match &output_format {
            OutputFormat::Delimited(s) => {
                let results = res.collect().await?;
                print_results(&results, *s).unwrap();
            }
            OutputFormat::Table => {
                if res.clone().collect().await?.len() > 0 {
                    res.show().await?;
                }
            }
        }
    }

    Ok(())
}

fn print_results(results: &[RecordBatch], delimiter: u8) -> std::io::Result<()> {
    let stdout = std::io::stdout();
    let mut handle = stdout.lock();
    let options = FormatOptions::default().with_display_error(true);

    if let Some(first_batch) = results.first() {
        let mut writer = WriterBuilder::new()
            .delimiter(delimiter)
            .from_writer(&mut handle);

        let formatters: Vec<ArrayFormatter>;
        // Write header
        let headers: Vec<String> = first_batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect();
        writer.write_record(headers)?;

        formatters = first_batch
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), &options))
            .collect::<Result<Vec<_>>>()
            .unwrap();

        for batch in results {
            for row in 0..batch.num_rows() {
                let mut record = csv::StringRecord::new();

                for col in 0..batch.num_columns() {
                    record.push_field(&formatters[col].value(row).to_string());
                }
                writer.write_record(&record).unwrap();
            }
        }
    }

    Ok(())
}
