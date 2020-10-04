/*
    Configuration for running Timely experiments on EC2.
*/

use super::util::{match_line_in_file, replace_lines_in_file};

use std::process::Command;
use std::str;
use std::string::String;

const EC2_IP_FILE: &str = "hosts/ec2_local_ips.txt";
const EC2_HOST_TEMP_FILE: &str = "results/temp_ec2_hosts.txt";
const EC2_REGION: &str = "us-east-2";

pub fn get_ec2_ipv4() -> String {
    // example output: 172.31.27.62
    let output = Command::new("curl")
        .arg("http://169.254.169.254/latest/meta-data/local-ipv4")
        .output()
        .expect(
            "Couldn't get local ipv4 address with curl command; \
             are you running on an EC2 instance?",
        );
    if output.stdout.is_empty() {
        panic!(
            "Couldn't get local ipv4 address with curl command; \
             are you running on an EC2 instance?",
        )
    }
    String::from_utf8_lossy(&output.stdout).to_string()
}

pub fn get_ec2_host_port_str(ipv4: &str, port: u64) -> String {
    // example output: ip-172-31-27-62.us-east-2.compute.internal:4000
    let ipv4 = str::replace(ipv4, ".", "-");
    format!("ip-{}.{}.compute.internal:{}", ipv4, EC2_REGION, port)
}

pub fn get_ec2_node_number() -> u64 {
    match_line_in_file(&get_ec2_ipv4(), EC2_IP_FILE).unwrap_or_else(|err| {
        panic!(
            "failed to calculate ec2 node number from ip file {}: {}",
            EC2_IP_FILE, err
        )
    }) as u64
}

// Create/overwrite the file with EC2 host information, then return the new file.
pub fn prepare_ec2_host_file(port: u64) -> &'static str {
    replace_lines_in_file(EC2_IP_FILE, EC2_HOST_TEMP_FILE, |ipv4| {
        get_ec2_host_port_str(ipv4, port)
    })
    .unwrap();
    EC2_HOST_TEMP_FILE
}
