use super::FeatureMoments;

pub fn decode_kmeans_moments(
    bytes: &[u8],
    dimensions: usize,
) -> Result<(FeatureMoments, Vec<f64>), String> {
    let header = 8usize
        .checked_add(dimensions.checked_mul(24).ok_or("moments size overflow")?)
        .and_then(|size| size.checked_add(4))
        .ok_or("moments size overflow")?;
    if bytes.len() < header || &bytes[..4] != b"WFM1" {
        return Err("invalid iterative feature moments".to_string());
    }
    if u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize != dimensions {
        return Err("iterative feature moment dimensions mismatch".to_string());
    }
    let mut counts = Vec::with_capacity(dimensions);
    let mut sums = Vec::with_capacity(dimensions);
    let mut sum_squares = Vec::with_capacity(dimensions);
    let mut at = 8;
    for _ in 0..dimensions {
        counts.push(u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap()));
        sums.push(f64::from_le_bytes(
            bytes[at + 8..at + 16].try_into().unwrap(),
        ));
        sum_squares.push(f64::from_le_bytes(
            bytes[at + 16..at + 24].try_into().unwrap(),
        ));
        at += 24;
    }
    let rows = u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
    at += 4;
    let values = rows.checked_mul(dimensions).ok_or("sample size overflow")?;
    if bytes.len() != at + values * 8 {
        return Err("invalid iterative initialization sample".to_string());
    }
    let sample = bytes[at..]
        .chunks_exact(8)
        .map(|chunk| f64::from_le_bytes(chunk.try_into().unwrap()))
        .collect();
    Ok((
        FeatureMoments {
            counts,
            sums,
            sum_squares,
        },
        sample,
    ))
}

pub fn encode_kmeans_moments(moments: &FeatureMoments, sample: &[f64]) -> Vec<u8> {
    let dimensions = moments.counts.len();
    let mut wire = Vec::with_capacity(12 + dimensions * 24 + sample.len() * 8);
    wire.extend_from_slice(b"WFM1");
    wire.extend_from_slice(&(dimensions as u32).to_le_bytes());
    for column in 0..dimensions {
        wire.extend_from_slice(&moments.counts[column].to_le_bytes());
        wire.extend_from_slice(&moments.sums[column].to_le_bytes());
        wire.extend_from_slice(&moments.sum_squares[column].to_le_bytes());
    }
    wire.extend_from_slice(&((sample.len() / dimensions) as u32).to_le_bytes());
    for value in sample {
        wire.extend_from_slice(&value.to_le_bytes());
    }
    wire
}

pub fn kmeans_normalization(moments: &FeatureMoments) -> (Vec<f64>, Vec<f64>) {
    let means: Vec<f64> = moments
        .counts
        .iter()
        .enumerate()
        .map(|(column, count)| {
            if *count == 0 {
                0.0
            } else {
                moments.sums[column] / *count as f64
            }
        })
        .collect();
    let scales: Vec<f64> = moments
        .counts
        .iter()
        .enumerate()
        .map(|(column, count)| {
            if *count == 0 {
                return 0.0;
            }
            let variance = (moments.sum_squares[column] / *count as f64
                - means[column] * means[column])
                .max(0.0);
            if variance <= f64::EPSILON {
                0.0
            } else {
                variance.sqrt()
            }
        })
        .collect();
    (means, scales)
}

pub fn encode_kmeans_normalization(means: &[f64], scales: &[f64]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(8 + means.len() * 16);
    bytes.extend_from_slice(b"WKN1");
    bytes.extend_from_slice(&(means.len() as u32).to_le_bytes());
    for value in means.iter().chain(scales) {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}

pub fn decode_kmeans_normalization(
    bytes: &[u8],
    dimensions: usize,
) -> Result<(Vec<f64>, Vec<f64>), String> {
    if bytes.len() != 8 + dimensions * 16
        || &bytes[..4] != b"WKN1"
        || u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize != dimensions
    {
        return Err("invalid iterative normalization wire".to_string());
    }
    let values: Vec<f64> = bytes[8..]
        .chunks_exact(8)
        .map(|chunk| f64::from_le_bytes(chunk.try_into().unwrap()))
        .collect();
    let (means, scales) = values.split_at(dimensions);
    if values.iter().any(|value| !value.is_finite()) || scales.iter().any(|value| *value < 0.0) {
        return Err("invalid iterative normalization values".to_string());
    }
    Ok((means.to_vec(), scales.to_vec()))
}
