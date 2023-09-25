use clap::{ArgGroup, Parser};
use datafusion::arrow::csv::writer::WriterBuilder;
use datafusion::arrow::error::Result;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionState;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::prelude::*;
use dbase::DbaseTableFactory;
use dirs::home_dir;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
#[clap(group(
    ArgGroup::new("method")
        .required(false)
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

    match (args.execute, args.file) {
        // query provided directly
        (Some(q), None) => {
            process_statements(&ctx, q.as_ref(), &output_format).await?;
        }
        // extract query from file
        (None, Some(file)) => {
            let file = File::open(file)?;
            let reader = BufReader::new(file);
            let mut query = String::new();

            for line in reader.lines() {
                let line = line?;
                query.push_str(&line);
                query.push(' ');
            }
            process_statements(&ctx, query.as_ref(), &output_format).await?;
        }
        // start repl and wait for input
        (None, None) => {
            repl(&ctx, &output_format).await.unwrap();
        }
        _ => unimplemented!(),
    }

    Ok(())
}

async fn process_statements(
    ctx: &SessionContext,
    query: &str,
    output_format: &OutputFormat,
) -> datafusion::error::Result<()> {
    let statements: Vec<&str> = query
        .split(';')
        .filter(|statement| !statement.trim().is_empty())
        .collect();

    for statement in statements {
        match process_statement(ctx, statement, output_format).await {
            Ok(_) => continue,
            Err(e) => {
                println!("{}", e);
                break;
            }
        }
    }
    Ok(())
}

async fn process_statement(
    ctx: &SessionContext,
    statement: &str,
    output_format: &OutputFormat,
) -> datafusion::error::Result<()> {
    let res = ctx.sql(statement).await?;

    match &output_format {
        OutputFormat::Delimited(s) => {
            let results = res.collect().await?;
            print_results(&results, *s).unwrap();
        }
        OutputFormat::Table => {
            // todo: don't collect the result twice
            if !res.clone().collect().await?.is_empty() {
                res.show().await?;
            }
        }
    }
    Ok(())
}

async fn repl(ctx: &SessionContext, output_format: &OutputFormat) -> rustyline::Result<()> {
    // `()` can be used when no completer is required
    let mut rl = DefaultEditor::new()?;
    let mut query: String = Default::default();

    let history_path = get_history_path();

    if rl.load_history(&history_path).is_err() {
        println!("No previous history.");
    }

    loop {
        let readline = rl.readline("dbase-sql> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str()).unwrap();
                query.push_str(&line);
                if query.ends_with(';') {
                    process_statements(ctx, &query, output_format)
                        .await
                        .expect("failed to process statements");
                    query = Default::default();
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    rl.save_history(&history_path).unwrap();

    Ok(())
}

fn print_results(results: &[RecordBatch], delimiter: u8) -> Result<()> {
    let stdout = std::io::stdout();
    let mut handle = stdout.lock();

    let mut writer = WriterBuilder::new()
        .with_delimiter(delimiter)
        .has_headers(true)
        .build(&mut handle);

    for batch in results {
        writer.write(batch)?;
    }

    Ok(())
}

fn get_history_path() -> String {
    let mut history_path = home_dir().unwrap();

    history_path.push(".dbase-sql");

    if !history_path.exists() {
        std::fs::create_dir(&history_path).unwrap();
    }

    history_path.push("history.txt");
    let history_path_str = history_path.to_str().unwrap();

    history_path_str.to_string()
}
