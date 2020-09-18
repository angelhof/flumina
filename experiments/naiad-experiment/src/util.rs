/*
    Utility functions
*/

use chrono;
use std::boxed::Box;
use std::fmt::Debug;
use std::fs::OpenOptions;
use std::io;
use std::io::prelude::*;
use std::str::FromStr;
use std::string::String;
use std::time::{Duration, SystemTime};
use std::vec::Vec;

/*
    Related to time
*/
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
pub fn current_datetime_str() -> String {
    let out = chrono::offset::Local::now().format("%Y-%m-%d-%H%M%S").to_string();
    println!("{:?}", out);
    out
}

/*
    For stdin input
*/
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

/*
    File handling
*/
pub fn vec_to_file<T>(v: Vec<T>, filename: &str) -> ()
where T: std::fmt::Debug {
    // This function may panic due to multiple reasons
    let mut file = OpenOptions::new()
        .create(true).write(true)
        .open(filename).unwrap();

    for item in v {
        writeln!(file, "{:?}", item).unwrap();
    }
}

/*
    String manipulation
*/
// Very bad function -- leaks the string memory :)
// Only use for e.g. command line arguments where it won't happen repeatedly.
pub fn string_to_static_str(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}
