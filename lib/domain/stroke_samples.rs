//! Compact stroke samples (annotations#19, manager decision
//! 32a104ee983b48ca995a7e7820839020).
//!
//! A stroke stores its raw samples as `{"count": n, "data": "<base64url>"}`.
//! `data` is base64url without padding of these bytes:
//!
//! 1. byte 0: format version, 1;
//! 2. byte 1: flags, bit 0 when a pressure column follows, bit 1 for tilt;
//! 3. the columns x, y, t, then pressure and tilt when flagged, each with
//!    `count` values. Every value is the zigzag-encoded difference to the
//!    value before it in the same column (the first one to 0), written as an
//!    unsigned LEB128 varint.
//!
//! The units do not change: x and y in 1/100 page units, t in ms from the
//! stroke start, pressure 0 to 1000, tilt in milliradians. Nothing else may
//! follow the last column.
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};

/// The format version written in byte 0.
pub const VERSION: u8 = 1;
const PRESSURE: u8 = 1;
const TILT: u8 = 2;

/// Decoded sample columns. `pressure` and `tilt` have `x.len()` values or are
/// absent.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Columns {
    pub x: Vec<i64>,
    pub y: Vec<i64>,
    pub t: Vec<i64>,
    pub pressure: Option<Vec<i64>>,
    pub tilt: Option<Vec<i64>>,
}

impl Columns {
    pub fn len(&self) -> usize {
        self.x.len()
    }
    pub fn is_empty(&self) -> bool {
        self.x.is_empty()
    }
}

fn zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}
fn unzigzag(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

fn put(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Encodes the columns. All columns must have the same length.
pub fn encode(c: &Columns) -> serde_json::Value {
    let mut out = vec![VERSION, 0];
    if c.pressure.is_some() {
        out[1] |= PRESSURE;
    }
    if c.tilt.is_some() {
        out[1] |= TILT;
    }
    let mut column = |values: &[i64]| {
        let mut last = 0i64;
        for v in values {
            put(&mut out, zigzag(v.wrapping_sub(last)));
            last = *v;
        }
    };
    column(&c.x);
    column(&c.y);
    column(&c.t);
    if let Some(p) = &c.pressure {
        column(p);
    }
    if let Some(t) = &c.tilt {
        column(t);
    }
    serde_json::json!({"count": c.len(), "data": URL_SAFE_NO_PAD.encode(out)})
}

/// Why `data` cannot be read. The schema check reports it as
/// `invalid_samples` at the `data` path.
#[derive(Debug, PartialEq, Eq)]
pub struct Malformed;

/// Decodes `data` with `count` values per column.
pub fn decode(count: usize, data: &str) -> Result<Columns, Malformed> {
    let bytes = URL_SAFE_NO_PAD.decode(data).map_err(|_| Malformed)?;
    if bytes.len() < 2 || bytes[0] != VERSION || bytes[1] & !(PRESSURE | TILT) != 0 {
        return Err(Malformed);
    }
    let flags = bytes[1];
    let mut at = 2usize;
    let mut column = || -> Result<Vec<i64>, Malformed> {
        let mut values = Vec::with_capacity(count);
        let mut last = 0i64;
        for _ in 0..count {
            let mut v = 0u64;
            let mut shift = 0u32;
            loop {
                let b = *bytes.get(at).ok_or(Malformed)?;
                at += 1;
                if shift >= 64 || (shift == 63 && b > 1) {
                    return Err(Malformed);
                }
                v |= u64::from(b & 0x7f) << shift;
                if b & 0x80 == 0 {
                    break;
                }
                shift += 7;
            }
            last = last.checked_add(unzigzag(v)).ok_or(Malformed)?;
            values.push(last);
        }
        Ok(values)
    };
    let x = column()?;
    let y = column()?;
    let t = column()?;
    let pressure = if flags & PRESSURE != 0 {
        Some(column()?)
    } else {
        None
    };
    let tilt = if flags & TILT != 0 {
        Some(column()?)
    } else {
        None
    };
    if at != bytes.len() {
        return Err(Malformed);
    }
    Ok(Columns {
        x,
        y,
        t,
        pressure,
        tilt,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_and_known_bytes() {
        let c = Columns {
            x: vec![100, 105, 98],
            y: vec![0, -3, 70000],
            t: vec![0, 4, 8],
            pressure: Some(vec![500, 520, 1000]),
            tilt: None,
        };
        let v = encode(&c);
        assert_eq!(v["count"], 3);
        let data = v["data"].as_str().unwrap();
        assert_eq!(decode(3, data).unwrap(), c);
        // The shared test vector of both apps.
        let one = Columns {
            x: vec![1, 0],
            y: vec![-1, 63],
            t: vec![0, 8],
            pressure: None,
            tilt: None,
        };
        assert_eq!(encode(&one)["data"], "AQACAQGAAQAQ");
    }

    #[test]
    fn malformed_data_is_rejected() {
        let ok = encode(&Columns {
            x: vec![1],
            y: vec![2],
            t: vec![0],
            ..Default::default()
        });
        let data = ok["data"].as_str().unwrap();
        assert!(decode(2, data).is_err(), "too short");
        assert!(decode(0, data).is_err(), "trailing bytes");
        assert!(decode(1, "!!").is_err(), "not base64url");
        assert!(decode(1, &URL_SAFE_NO_PAD.encode([2, 0, 0, 0, 0])).is_err());
        assert!(decode(1, &URL_SAFE_NO_PAD.encode([1, 4, 0, 0, 0])).is_err());
    }
}
