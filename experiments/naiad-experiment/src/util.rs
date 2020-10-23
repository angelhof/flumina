/*
    Utility functions
*/

use chrono::offset::Local;
use rand::Rng;

use std::boxed::Box;
use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::io::{self, prelude::*, BufReader, Result};
use std::str::FromStr;
use std::string::String;
use std::thread;
use std::time::{Duration, SystemTime};
use std::vec::Vec;

/*
    Related to time
*/
pub fn time_since(t: SystemTime) -> Duration {
    // Note: this function may panic in case of clock drift
    t.elapsed().unwrap()
}
pub fn div_durations(d1: Duration, d2: Duration) -> u128 {
    ((d1.as_nanos() as f64) / (d2.as_nanos() as f64)) as u128
}
pub fn nanos_timestamp(t: SystemTime) -> u128 {
    // Note: this function may panic in case of clock drift
    t.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos()
}
pub fn current_datetime_str() -> String {
    let out = Local::now().format("%Y-%m-%d-%H%M%S").to_string();
    println!("Current Datetime: {:?}", out);
    out
}
pub fn sleep_for_secs(s: u64) {
    thread::sleep(Duration::from_secs(s));
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
    io::stdin().read_line(&mut input_text).expect("failed to read from stdin");
    input_text.trim().parse::<T>().expect("not an integer")
}

/*
    File handling
*/
pub fn vec_to_file<T>(v: Vec<T>, filename: &str)
where
    T: std::fmt::Debug,
{
    // This function may panic due to multiple reasons
    let mut file =
        OpenOptions::new().create(true).write(true).open(filename).unwrap();

    for item in v {
        writeln!(file, "{:?}", item).unwrap();
    }
}

// Run a closure for each line in a file
fn for_each_line_do<F>(filepath: &str, mut closure: F) -> Result<()>
where
    F: FnMut(usize, &str) -> Result<()>, // line number, line
{
    let file = File::open(filepath)?;
    let reader = BufReader::new(file);
    for (line_number, line) in reader.lines().enumerate() {
        closure(line_number, &line.unwrap())?;
    }
    Result::Ok(())
}

// From a file create a new one where each line is replaced using a given function
pub fn replace_lines_in_file<F>(
    in_filepath: &str,
    out_filepath: &str,
    closure: F,
) -> Result<()>
where
    F: Fn(usize, &str) -> String,
{
    let mut out_file =
        OpenOptions::new().create(true).write(true).open(out_filepath)?;
    for_each_line_do(in_filepath, move |line_number, line| {
        writeln!(out_file, "{}", closure(line_number, line))
    })
}

// Find a line in filepath equal to text and return the line number.
// Otherwise, return an error.
// Warning: line numbering starts from 0!
pub fn match_line_in_file(text: &str, filepath: &str) -> Result<usize> {
    let file = File::open(filepath)?;
    let reader = BufReader::new(file);
    for (line_number, line) in reader.lines().enumerate() {
        if line.unwrap() == text {
            return Result::Ok(line_number);
        }
    }
    Result::Err(io::Error::new(
        io::ErrorKind::Other,
        format!("text {} not found in file {}", text, filepath),
    ))
}

/*
    String manipulation
*/
// Very bad function -- leaks the string memory :)
// Only use for e.g. command line arguments where it won't happen repeatedly.
pub fn string_to_static_str(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

/*
    Random number generation
*/
pub fn rand_range(a: u64, b: u64) -> u64 {
    rand::thread_rng().gen_range(a, b)
}
pub fn rand_bool(p: f64) -> bool {
    rand::thread_rng().gen::<f64>() < p
}
