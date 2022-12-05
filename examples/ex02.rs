use arrow::csv::ReaderBuilder;
use clap::{Parser, ValueHint};
use parquet::{arrow::ArrowWriter, errors::ParquetError};
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"), author = "Dominik Moritz <domoritz@cmu.edu>")]
struct Opts {
    /// Input CSV file.
    #[clap(name = "CSV", value_parser, value_hint = ValueHint::AnyPath)]
    input: PathBuf,

    /// Output file.
    #[clap(name = "PARQUET", value_parser, value_hint = ValueHint::AnyPath)]
    output: PathBuf,

    /// File with Arrow schema in JSON format.
    #[clap(short = 's', long, value_parser, value_hint = ValueHint::AnyPath)]
    schema_file: Option<PathBuf>,

    /// The number of records to infer the schema from. All rows if not present. Setting max-read-records to zero will stop schema inference and all columns will be string typed.
    #[clap(long)]
    max_read_records: Option<usize>,

    /// Set whether the CSV file has headers
    #[clap(long)]
    header: Option<bool>,

    /// Set the CSV file's column delimiter as a byte character.
    #[clap(short, long, default_value = ",")]
    delimiter: char,
}

fn main() -> Result<(), ParquetError> {
    let data = vec![
        vec!["id,entry"],
        vec!["220101,john ran to the store"],
        vec!["220102,bill ate cupcakes"],
        vec!["220103,sue played in her sandbox"],
        vec!["0,a"],
        vec!["1,b"],
        vec!["2,c"],
    ];

    let data = data
        .iter()
        .map(|x| x.join(","))
        .collect::<Vec<_>>()
        .join("\n");
    let data = data.as_bytes();

    let mut cursor = std::io::Cursor::new(data);

    let opts: Opts = Opts::parse();

    let schema = match arrow::csv::reader::infer_file_schema(
        &mut cursor,
        opts.delimiter as u8,
        opts.max_read_records,
        opts.header.unwrap_or(true),
    ) {
        Ok((schema, _inferred_has_header)) => Ok(schema),
        Err(error) => Err(ParquetError::General(format!(
            "Error inferring schema: {}",
            error
        ))),
    }?;

    let schema_ref = Arc::new(schema);

    let builder = ReaderBuilder::new()
        .has_header(opts.header.unwrap_or(true))
        .with_delimiter(opts.delimiter as u8)
        .with_schema(schema_ref);

    let reader = builder.build(cursor)?;

    let output = File::create(opts.output)?;

    let mut writer = ArrowWriter::try_new(output, reader.schema(), None)?;

    for batch in reader {
        match batch {
            Ok(batch) => writer.write(&batch)?,
            Err(error) => return Err(error.into()),
        }
    }

    match writer.close() {
        Ok(_) => Ok(()),
        Err(error) => Err(error),
    }
}
