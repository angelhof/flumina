/*
Example barrier computation between EC2 nodes.

When run on multiple EC2 instances, waits for all to start,
then all of them complete the barrier and terminate.
*/

use naiad_experiment::ec2::{ec2_barrier, get_ec2_node_number, local_barrier};

use structopt::StructOpt;

#[derive(StructOpt)]
#[structopt(
    author = "Caleb Stanford",
    no_version,
    about = "Command line for running Timely experiments."
)]
enum BarrierTest {
    #[structopt(about = "local barrier test")]
    Local {
        num_nodes: u64,
        this_node: u64,
    },
    EC2 {
        num_nodes: u64,
    },
}
const START_PORT: u16 = 5000;
impl BarrierTest {
    fn run_core<F: Fn(u64, u64, u16)>(
        num_nodes: u64,
        this_node: u64,
        barrier_fun: F,
    ) {
        println!("[node {}/{}] entering barrier", this_node, num_nodes);
        barrier_fun(num_nodes, this_node, START_PORT);
        println!("[node {}/{}] done", this_node, num_nodes);
    }
    fn run_barrier(&self) {
        match self {
            BarrierTest::Local { num_nodes, this_node } => {
                Self::run_core(*num_nodes, *this_node, local_barrier);
            }
            BarrierTest::EC2 { num_nodes } => {
                let this_node = get_ec2_node_number();
                Self::run_core(*num_nodes, this_node, ec2_barrier);
            }
        };
    }
}

fn main() {
    BarrierTest::from_args().run_barrier();
}
