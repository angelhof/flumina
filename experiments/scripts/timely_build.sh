#!/bin/bash
# Run cargo build --release on remote host

hostname=$1
remote_cargo=/home/ubuntu/.cargo/bin/cargo

ssh $hostname << EOF
cd flumina-devel/experiments/timely-experiment/
${remote_cargo} build --release
EOF
