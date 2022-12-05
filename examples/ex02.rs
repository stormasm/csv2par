use arrow::csv::ReaderBuilder;
use clap::{Parser, ValueHint};
use parquet::{arrow::ArrowWriter, errors::ParquetError};
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"), author = "Dominik Moritz <domoritz@cmu.edu>")]
struct Opts {
    /// Output file.
    #[clap(name = "PARQUET", value_parser, value_hint = ValueHint::AnyPath)]
    output: PathBuf,
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

    let delimiter: char = ',';

    let schema =
        match arrow::csv::reader::infer_file_schema(&mut cursor, delimiter as u8, None, true) {
            Ok((schema, _inferred_has_header)) => Ok(schema),
            Err(error) => Err(ParquetError::General(format!(
                "Error inferring schema: {}",
                error
            ))),
        }?;

    let schema_ref = Arc::new(schema);

    let builder = ReaderBuilder::new()
        .has_header(true)
        .with_delimiter(delimiter as u8)
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
