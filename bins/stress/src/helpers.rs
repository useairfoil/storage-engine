use std::ops::RangeInclusive;

use bytesize::ByteSize;

pub fn parse_row_data_size_range(input: &str) -> Result<RangeInclusive<u64>, String> {
    let (start, end) = input.split_once("..").unwrap_or((input, input));
    let start = start.parse::<ByteSize>()?.as_u64();
    let end = end.parse::<ByteSize>()?.as_u64();

    if start > end {
        return Err("row data size range must be ascending".to_owned());
    }

    Ok(start..=end)
}

pub fn parse_batch_size_range(input: &str) -> Result<RangeInclusive<u64>, String> {
    let (start, end) = input.split_once("..").unwrap_or((input, input));
    let start = start.parse::<u64>().map_err(|error| error.to_string())?;
    let end = end.parse::<u64>().map_err(|error| error.to_string())?;

    if start == 0 || start > end {
        return Err("batch size range must be positive and ascending".to_owned());
    }

    Ok(start..=end)
}
