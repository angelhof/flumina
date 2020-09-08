/*
    Utility functions
*/

use std::fmt::Debug;
use std::io;
use std::str::FromStr;
use std::time::{Duration, SystemTime};

// Related to time
pub fn time_since(t: SystemTime) -> Duration {
    // Note: this function may panic in case of clock drift
    t.elapsed().unwrap()
}
pub fn div_durations(d1: Duration, d2: Duration) -> u64 {
    ((d1.as_nanos() as f64) / (d2.as_nanos() as f64)) as u64
}
pub fn nanos_timestamp(t: SystemTime) -> u128 {
    // Note: this function may panic in case of clock drift
    t.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos()
}

// For stdin input
pub fn get_input<T>(msg: &str) -> T
where
    T: FromStr,
    <T as FromStr>::Err: Debug,
{
    println!("{}", msg);
    let mut input_text = String::new();
    io::stdin()
        .read_line(&mut input_text)
        .expect("failed to read from stdin");
    input_text.trim().parse::<T>().expect("not an integer")
}
