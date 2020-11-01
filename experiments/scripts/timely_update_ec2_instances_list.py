"""
Script to update the EC2 instance information using the AWS CLI.

NOTE: be sure to run this script from the naiad-experiment repo!
It won't work from another directory.
"""

import json
import subprocess

EC2_PRIVATE_IPS_PATH = "hosts/ec2_local_ips.txt"
EC2_SSH_HOSTS_PATH = "hosts/ssh_hosts.txt"
EC2_PARTIAL_SSH_HOSTS_PATHS = [
    (1, "hosts/ssh_hosts_1.txt"),
    (2, "hosts/ssh_hosts_2.txt"),
]

# Get ec2 describe-instances list, return as a json structure
def describe_timely_instances():
    raw_output = subprocess.run(
        [
            "aws", "ec2", "describe-instances",
            "--filters", "Name=tag:Purpose,Values=Timely"
        ],
        stdout=subprocess.PIPE
    )
    return json.loads(raw_output.stdout.decode('utf-8'))

def get_private_ipv4s(json_data):
    result = [
        instance["PrivateIpAddress"]
        for reservation in json_data["Reservations"]
        for instance in reservation["Instances"]
    ]
    print(f"Got private IP addresses: {result}")
    print(result)
    return result

def get_ssh_host_names(json_data):
    result = [
        "ubuntu@" + instance["PublicDnsName"]
        for reservation in json_data["Reservations"]
        for instance in reservation["Instances"]
    ]
    print(f"Got SSH host names: {result}")
    return result

def save_string_list_to_file(list, filepath):
    with open(filepath, 'w') as fh:
        for item in list:
            fh.write(item + '\n')
    print(f"Successfully wrote {len(list)} lines to file {filepath}")

json_data = describe_timely_instances()
private_ipv4s = get_private_ipv4s(json_data)
ssh_host_names = get_ssh_host_names(json_data)
save_string_list_to_file(private_ipv4s, EC2_PRIVATE_IPS_PATH)
save_string_list_to_file(ssh_host_names, EC2_SSH_HOSTS_PATH)
for numlines, path in EC2_PARTIAL_SSH_HOSTS_PATHS:
    save_string_list_to_file(ssh_host_names[0:numlines], path)
