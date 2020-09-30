/*
    Configuration for running Timely experiments on EC2.
*/

use super::util::{match_line_in_file};

use std::process::Command;
use std::string::String;

const EC2_HOST_FILE: &str = "hosts/ec2_hosts.txt";
const EC2_REGION: &str = "us-east-2";
const EC2_PORT: u32 = 4000;

pub fn get_ec2_host_file() -> &'static str {
    EC2_HOST_FILE
}

fn get_ec2_ipv4() -> String {
    // example output: 172-31-27-62
    let output = Command::new("curl")
        .arg("http://169.254.169.254/latest/meta-data/local-ipv4")
        .output()
        .expect("Couldn't get local ipv4 address with curl command; \
                 are you running on an EC2 instance?");
    String::from_utf8_lossy(&output.stdout).to_string()
}

pub fn get_ec2_host_port_str() -> String {
    // example output: ip-172-31-27-62.us-east-2.compute.internal:4000
    let ipv4 = get_ec2_ipv4();
    format!("ip-{}.{}.compute.internal:{}", ipv4, EC2_REGION, EC2_PORT)
}

pub fn get_ec2_node_number() -> u64 {
    match_line_in_file(&get_ec2_host_port_str(), EC2_HOST_FILE)
        .expect(&format!(
            "failed to calculate ec2 node number from host file {}",
            EC2_HOST_FILE
        ))
}
