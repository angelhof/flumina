#!/bin/bash
# Run experiments with cargo run --release on remote host
# Currently just runs exp1, edit to run exp2 and exp3 also

hostname=$1
remote_cargo=/home/ubuntu/.cargo/bin/cargo

ssh $hostname << EOF
cd flumina-devel/experiments/naiad-experiment/
${remote_cargo} run --release exp1
${remote_cargo} run --release exp2
${remote_cargo} run --release exp3
${remote_cargo} run --release exp4
EOF
