/*
    Abstractions for running experiments in Timely.
*/

// Sorry Clippy, in this case it's a minor problem with too many instances
// to be worth fixing right now.
#![allow(clippy::wrong_self_convention)]

use super::common::{Scope, Stream};
use super::ec2::{
    ec2_barrier, get_ec2_node_number, local_barrier, prepare_ec2_host_file,
    prepare_local_host_file,
};
use super::operators::save_to_file;
use super::perf::latency_throughput_meter;
use super::util::{current_datetime_str, run_as_process, string_to_static_str};

use abomonation_derive::Abomonation;
use structopt::StructOpt;

use std::str::FromStr;
use std::string::String;
use std::vec::Vec;

/* Constants */

// Results filenames
const RESULTS_DIR: &str = "results/";
const RESULTS_EXT: &str = ".out";
fn make_results_path<T: AsRef<str>>(
    exp_name: &str,
    args: &[T],
) -> &'static str {
    let mut out = RESULTS_DIR.to_owned() + &current_datetime_str() + "_";
    out += exp_name;
    for arg in args {
        out += "_";
        out += arg.as_ref();
    }
    out += RESULTS_EXT;
    string_to_static_str(out)
}

// Ports for distributed communication over EC2
const EC2_STARTING_PORT: u64 = 4000;
const LOCAL_STARTING_PORT: u64 = 4000;
const BARRIER_START_PORT: u16 = 5000;
/*
    Types of networks where Timely distributed experiments can be run
*/
#[derive(Abomonation, Copy, Clone, Debug, Eq, PartialEq)]
enum TimelyNetworkType {
    SingleNode,
    Local,
    EC2,
}
impl FromStr for TimelyNetworkType {
    type Err = &'static str;
    fn from_str(input: &str) -> Result<TimelyNetworkType, Self::Err> {
        match input {
            "s" => Ok(Self::SingleNode),
            "l" => Ok(Self::Local),
            "e" => Ok(Self::EC2),
            _ => Err("Invalid network type (choices: 's', 'l', 'e')"),
        }
    }
}

/*
    Distributed node information
    Network together with a node number.
    Used to run distributed experiments that can be either local
    (with a node number) or over EC2 (where node number will be derived).
*/
#[derive(Abomonation, Copy, Clone, Debug, Eq, PartialEq)]
pub enum TimelyNodeInfo {
    Local(u64), // node number
    EC2,
}
impl FromStr for TimelyNodeInfo {
    type Err = &'static str;
    fn from_str(input: &str) -> Result<TimelyNodeInfo, Self::Err> {
        if input == "e" {
            Ok(Self::EC2)
        } else if &input[0..1] == "l" {
            match u64::from_str(&input[1..]) {
                Ok(this_node) => Ok(Self::Local(this_node)),
                Err(_err) => Err("Node ID should be a u64. Example usage: l3"),
            }
        } else {
            Err("Invalid node info (choices: 'l<id>', 'e')")
        }
    }
}
impl TimelyNodeInfo {
    fn this_node(&self) -> u64 {
        match &self {
            TimelyNodeInfo::Local(this_node) => *this_node,
            TimelyNodeInfo::EC2 => get_ec2_node_number(),
        }
    }
    fn is_main_node(&self) -> bool {
        self.this_node() == 0
    }
}

/*
    Parameters to run a Timely dataflow between several
    parallel workers or nodes
*/
#[derive(Abomonation, Copy, Clone, Debug, StructOpt)]
pub struct TimelyParallelism {
    // Command line -w, should be >= 1
    workers: u64,
    // Command line -n, should be >= 1
    nodes: u64,
    // Command line -p, betwewen 0 and nodes-1 (unused if nodes=1)
    this_node: u64,
    // Type of network
    #[structopt(default_value = "s")]
    network: TimelyNetworkType,
    // Experiment number -- to disambiguate unique experiments,
    // in case multiple are going on at once so they don't interfere.
    // This is incorporated into the port number. If not needed can just
    // be set to 0.
    #[structopt(skip = 0u64)]
    experiment_num: u64,
}
impl TimelyParallelism {
    /* Constructors */
    fn new_single_node(workers: u64) -> TimelyParallelism {
        let result = TimelyParallelism {
            workers,
            nodes: 1,
            this_node: 0,
            network: TimelyNetworkType::SingleNode,
            experiment_num: 0,
        };
        result.validate();
        result
    }
    pub fn new_sequential() -> TimelyParallelism {
        Self::new_single_node(1)
    }
    pub fn new_distributed_local(
        workers: u64,
        nodes: u64,
        this_node: u64,
        experiment_num: u64,
    ) -> TimelyParallelism {
        let result = TimelyParallelism {
            workers,
            nodes,
            this_node,
            network: TimelyNetworkType::Local,
            experiment_num,
        };
        result.validate();
        result
    }
    pub fn new_distributed_ec2(
        workers: u64,
        nodes: u64,
        experiment_num: u64,
    ) -> TimelyParallelism {
        let result = TimelyParallelism {
            workers,
            nodes,
            this_node: get_ec2_node_number(),
            network: TimelyNetworkType::EC2,
            experiment_num,
        };
        result.validate();
        result
    }
    pub fn new_from_info(
        node_info: TimelyNodeInfo,
        workers: u64,
        nodes: u64,
        experiment_num: u64,
    ) -> TimelyParallelism {
        match node_info {
            TimelyNodeInfo::Local(this_node) => Self::new_distributed_local(
                workers,
                nodes,
                this_node,
                experiment_num,
            ),
            TimelyNodeInfo::EC2 => {
                Self::new_distributed_ec2(workers, nodes, experiment_num)
            }
        }
    }

    /* Private methods */
    fn validate(&self) {
        assert!(self.workers >= 1 && self.nodes >= 1);
        if self.network == TimelyNetworkType::SingleNode {
            assert!(self.nodes == 1);
        }
    }
    fn is_participating(&self) -> bool {
        self.this_node < self.nodes
    }
    fn prepare_host_file(&self) -> &'static str {
        match self.network {
            TimelyNetworkType::SingleNode => unreachable!(),
            TimelyNetworkType::Local => {
                let port = LOCAL_STARTING_PORT + self.experiment_num;
                prepare_local_host_file(port)
            }
            TimelyNetworkType::EC2 => {
                let port = EC2_STARTING_PORT + self.experiment_num * self.nodes;
                prepare_ec2_host_file(port)
            }
        }
    }

    /* Data summaries */
    pub fn to_csv(&self) -> String {
        self.validate();
        format!("{} wkrs, {} nodes", self.workers, self.nodes)
    }
    pub fn to_vec(&self) -> Vec<String> {
        self.validate();
        vec![self.workers.to_string(), self.nodes.to_string()]
    }

    /* Compute arguments to pass to Timely */
    // Note 1: call only once per experiment. Creates/initializes a host file
    // specific to that experiment.
    // Note 2: returns None if this node is not involved in this experiment
    // (i.e. node # is larger than number of nodes)
    pub fn timely_args(&self) -> Option<Vec<String>> {
        self.validate();
        if !self.is_participating() {
            None
        } else {
            let mut vec = vec!["-w".to_string(), self.workers.to_string()];
            if self.nodes > 1 {
                vec.push("-n".to_string());
                vec.push(self.nodes.to_string());
                vec.push("-p".to_string());
                vec.push(self.this_node.to_string());

                let hostfile = self.prepare_host_file();
                vec.push("-h".to_string());
                vec.push(hostfile.to_string());
            }
            Some(vec)
        }
    }

    /* Main node (used for restricting output so it is less noisy) */
    fn is_main_node(&self) -> bool {
        self.this_node == 0
    }
    /* Barrier (used for synchronizing experiments) */
    fn barrier(&self) {
        match self.network {
            TimelyNetworkType::SingleNode => (),
            TimelyNetworkType::Local => {
                let port = BARRIER_START_PORT
                    + ((self.experiment_num * 2 * self.nodes) as u16);
                local_barrier(self.nodes, self.this_node, port);
            }
            TimelyNetworkType::EC2 => {
                let port = BARRIER_START_PORT
                    + ((self.experiment_num * 2 * self.nodes) as u16);
                ec2_barrier(self.nodes, self.this_node, port);
            }
        }
    }
}

/*
    Trait to capture parameters that form the input to a Timely experiment.

    to_csv should output the parameters separated by commas.
    to_vec should output the parameters as strings in a list.
    get_exp_duration_secs is the total time that the experiment runs (in secs).
    set_rate should vary one or more of the parameters to set the
    input throughput (in events / ms), which can be used to test throughput.

    ExperimentParams should be immutable and implement the Copy trait.
    For example, set_rate returns a new object.
*/
pub trait ExperimentParams: Copy + StructOpt + timely::ExchangeData {
    fn to_csv(&self) -> String;
    fn to_vec(&self) -> Vec<String>;
    fn get_exp_duration_secs(&self) -> u64;
    fn set_rate(&self, rate_per_milli: u64) -> Self;
}

/*
    Trait to capture the full executable experiment.

    To use, implement the get_name and build_dataflow methods.

    One reason this trait is needed is in order to hide the top-level scope
    parameter passed by run_dataflow to build the dataflow, which is instead made
    generic in the build_dataflow method. This
    is necessary because Rust generics are weird -- see
    https://stackoverflow.com/questions/37606035/pass-generic-function-as-argument
*/
pub trait LatencyThroughputExperiment<P, I, O>: timely::ExchangeData
where
    P: ExperimentParams,
    I: std::fmt::Debug + Clone + timely::Data,
    O: std::fmt::Debug + Clone + timely::Data,
{
    /* Functionality to implement */
    fn get_name(&self) -> String;
    fn build_dataflow<G: Scope<Timestamp = u128>>(
        &self,
        params: P,
        scope: &mut G,
        worker_index: usize,
    ) -> (Stream<G, I>, Stream<G, O>);

    /* Functionality provided, but mostly considered private */
    // The core dataflow to be run
    fn run_dataflow<G: Scope<Timestamp = u128>>(
        &self,
        scope: &mut G,
        params: P,
        parallelism: TimelyParallelism,
        worker_index: usize,
        output_filename: &'static str,
    ) {
        let (input, output) = self.build_dataflow(params, scope, worker_index);
        // Optional other meters, uncomment for testing
        // volume_meter(&input);
        // completion_meter(&output);
        // latency_meter(&output);
        // throughput_meter(&input, &output);
        let latency_throughput = latency_throughput_meter(&input, &output);
        let parallelism_csv = parallelism.to_csv();
        let params_csv = params.to_csv();
        save_to_file(
            &latency_throughput,
            &output_filename,
            move |(latency, throughput)| {
                format!(
                    "{}, {}, {} ms, {} events/ms",
                    parallelism_csv, params_csv, latency, throughput
                )
            },
        );
    }
    // Run an experiment with a filename
    // Only necessary if the user wants a custom filename
    fn run_with_filename(
        &'static self,
        params: P,
        parallelism: TimelyParallelism,
        output_filename: &'static str,
    ) {
        if parallelism.is_main_node() {
            println!(
                "{} Experiment Parameters: {}, Parallelism: {}",
                self.get_name(),
                params.to_csv(),
                parallelism.to_csv(),
            );
        }
        let opt_args = parallelism.timely_args();
        let node_index = parallelism.this_node;
        match opt_args {
            Some(mut args) => {
                // Barrier to make sure different experiments don't overlap!
                parallelism.barrier();

                println!("[node {}] initializing experiment", node_index);
                println!("[node {}] timely args: {:?}", node_index, args);
                let func = move || {
                    timely::execute_from_args(args.drain(0..), move |worker| {
                        let worker_index = worker.index();
                        worker.dataflow(move |scope| {
                            self.run_dataflow(
                                scope,
                                params,
                                parallelism,
                                worker_index,
                                output_filename,
                            );
                            println!(
                                "[worker {}] setup complete",
                                worker_index
                            );
                        });
                    })
                    .unwrap();
                };
                run_as_process(func);
            }
            None => {
                println!(
                    "[node {}] skipping experiment between nodes {:?}",
                    node_index,
                    (0..parallelism.nodes).collect::<Vec<u64>>()
                );
                // Old -- no longer necessary, it was just a hack not as general
                // as using the barrier to synchronize experiments
                // let sleep_dur = params.get_exp_duration_secs();
                // println!("[node {}] sleeping for {}", node_index, sleep_dur);
                // sleep_for_secs(sleep_dur);
            }
        }
    }

    /* Functionality provided and exposed as the main options */
    // Run a single experiment.
    fn run_single(&'static self, params: P, parallelism: TimelyParallelism) {
        let mut args = Vec::new();
        args.append(&mut params.to_vec());
        args.append(&mut parallelism.to_vec());
        let results_path = make_results_path(&self.get_name(), &args[..]);
        self.run_with_filename(params, parallelism, results_path);
    }
    // Run many experiments
    fn run_all(
        &'static self,
        node_info: TimelyNodeInfo,
        default_params: P,
        rates_per_milli: &[u64],
        par_workers: &[u64],
        par_nodes: &[u64],
    ) {
        // Run experiment for all different configurations
        let mut exp_num = 0;
        for &par_w in par_workers {
            for &par_n in par_nodes {
                if node_info.is_main_node() {
                    println!(
                        "===== Parallelism: {} w, {} n =====",
                        par_w, par_n
                    );
                }
                let results_path = make_results_path(
                    &self.get_name(),
                    &[
                        &("w".to_owned() + &par_w.to_string()),
                        &("n".to_owned() + &par_n.to_string()),
                    ],
                );
                for &rate in rates_per_milli {
                    if node_info.is_main_node() {
                        println!("=== Input Rate (events/ms): {} ===", rate);
                    }
                    let params = default_params.set_rate(rate);
                    let parallelism = TimelyParallelism::new_from_info(
                        node_info, par_w, par_n, exp_num,
                    );
                    self.run_with_filename(params, parallelism, results_path);
                    exp_num += 1;
                }
            }
        }
    }
}
