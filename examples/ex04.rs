use arrow::csv::ReaderBuilder;
use parquet::{arrow::ArrowWriter, errors::ParquetError};
use std::fs::File;
use std::sync::Arc;

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

    parquet_file_writer(&data)
}

pub fn parquet_file_writer(csv: &str) -> Result<(), ParquetError> {
    let data = csv.as_bytes();
    let mut cursor = std::io::Cursor::new(data);

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

    let output = File::create("foo.parquet")?;

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
