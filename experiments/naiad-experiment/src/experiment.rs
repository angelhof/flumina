/*
    Abstractions for running experiments in Timely.
*/

use super::common::{Scope, Stream};
use super::ec2::{get_ec2_node_number, prepare_ec2_host_file};
use super::operators::save_to_file;
use super::perf::latency_throughput_meter;
use super::util::{current_datetime_str, sleep_for_secs, string_to_static_str};

use abomonation_derive::Abomonation;

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
// TODO
// const LOCAL_STARTING_PORT: u64 = 4000;

/*
    Parameters to run a Timely dataflow between several
    parallel workers or nodes
*/
#[derive(Abomonation, Copy, Clone, Debug)]
pub struct TimelyParallelism {
    // Command line -w, should be >= 1
    workers: u64,
    // Command line -n, should be >= 1
    nodes: u64,
    // Command line -p, betwewen 0 and nodes-1 (unused if nodes=1)
    this_node: u64,
    // Experiment number -- to disambiguate unique experiments,
    // in case multiple are going on at once so they don't interfere
    experiment_num: u64,
}
impl TimelyParallelism {
    // Constructors
    fn new_single_node(workers: u64) -> TimelyParallelism {
        let result = TimelyParallelism {
            workers,
            nodes: 1,
            this_node: 0,
            experiment_num: 0,
        };
        result.validate();
        result
    }
    pub fn new_sequential() -> TimelyParallelism {
        Self::new_single_node(1)
    }
    pub fn new_for_ec2(workers: u64, nodes: u64) -> TimelyParallelism {
        if nodes == 1 {
            Self::new_single_node(workers)
        } else {
            let result = TimelyParallelism {
                workers,
                nodes,
                this_node: get_ec2_node_number(),
                experiment_num: 0,
            };
            result.validate();
            result
        }
    }
    // Private methods
    fn validate(&self) {
        assert!(
            self.workers >= 1 && self.nodes >= 1 && self.this_node < self.nodes
        );
    }
    fn increment_experiment_num(&mut self) {
        self.experiment_num += 1;
    }
    fn prepare_ec2_host_file(&self) -> &'static str {
        let port = EC2_STARTING_PORT + self.experiment_num;
        prepare_ec2_host_file(port)
    }
    // String summary
    pub fn to_csv(&self) -> String {
        self.validate();
        format!("{} wkrs, {} nodes", self.workers, self.nodes)
    }
    pub fn to_vec(&self) -> Vec<String> {
        self.validate();
        let mut result = Vec::new();
        result.push(self.workers.to_string());
        result.push(self.nodes.to_string());
        result
    }
    // Compute arguments to pass to Timely
    // Note: call only once per experiment. Creates/initializes a host file
    // specific to that experiment.
    pub fn timely_args(&mut self) -> Vec<String> {
        self.validate();
        self.increment_experiment_num();

        let mut vec: Vec<String> = Vec::new();
        vec.push("-w".to_string());
        vec.push(self.workers.to_string());
        if self.nodes > 1 {
            vec.push("-n".to_string());
            vec.push(self.nodes.to_string());
            vec.push("-p".to_string());
            vec.push(self.this_node.to_string());

            let hostfile = self.prepare_ec2_host_file();
            vec.push("-h".to_string());
            vec.push(hostfile.to_string());
        }
        vec
    }
}

/*
    Trait to capture parameters that form the input to a Timely experiment.

    to_csv should output the parameters separated by commas.
    to_vec should output the parameters as strings in a list.
    get_exp_duration_secs is the total time that the experiment runs (in secs).
    set_rate should vary one or more of the parameters to set the
    input throughput (in events / ms), which can be used to test throughput.
*/
pub trait ExperimentParams: Copy + timely::ExchangeData {
    fn to_csv(&self) -> String;
    fn to_vec(&self) -> Vec<String>;
    fn get_exp_duration_secs(&self) -> u64;
    fn set_rate(&mut self, rate_per_milli: u64);
}

/*
    Trait to capture the full executable experiment.

    To use, implement the get_name and build_dataflow methods.

    One reason this trait is needed is in order to hide the top-level scope
    parameter passed by run_core to build the dataflow, which is instead made
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

    /* Functionality provided */
    // The core dataflow: should be considered private
    fn run_core<G: Scope<Timestamp = u128>>(
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
    // Run an experiment: also can be considered private if the user does
    // not want a custom filename
    fn run(
        &'static self,
        params: P,
        parallelism: &mut TimelyParallelism,
        output_filename: &'static str,
    ) {
        println!(
            "{} Experiment Parameters: {}, Parallelism: {}",
            self.get_name(),
            params.to_csv(),
            parallelism.to_csv(),
        );
        let mut args = parallelism.timely_args();
        let parallelism_copy = *parallelism;
        println!("Timely args: {:?}", args);
        timely::execute_from_args(args.drain(0..), move |worker| {
            let worker_index = worker.index();
            worker.dataflow(move |scope| {
                self.run_core(
                    scope,
                    params,
                    parallelism_copy,
                    worker_index,
                    output_filename,
                );
                println!("[worker {}] setup complete", worker_index);
            });
        })
        .unwrap();
    }
    // Run a single experiment and calculate the filepath rather than
    // providing it
    fn run_single(
        &'static self,
        params: P,
        parallelism: &mut TimelyParallelism,
    ) {
        let mut args = Vec::new();
        args.append(&mut params.to_vec());
        args.append(&mut parallelism.to_vec());
        let results_path = make_results_path(&self.get_name(), &args[..]);
        self.run(params, parallelism, results_path);
    }
    // Run many experiments
    fn run_all(
        &'static self,
        default_params: P,
        rates_per_milli: &[u64],
        par_workers: &[u64],
        par_nodes: &[u64],
    ) {
        let mut params = default_params;
        for &par_w in par_workers {
            for &par_n in par_nodes {
                // Note: fix this so that it works across experiments
                let mut parallelism =
                    TimelyParallelism::new_for_ec2(par_w, par_n);
                // parallelism.workers = par_w;
                // parallelism.nodes = par_n;
                // Only run experiment if this node # is used in the experiment
                if get_ec2_node_number() >= par_n {
                    for &_rate in rates_per_milli {
                        let sleep_dur = params.get_exp_duration_secs();
                        println!("Sleeping for {}", sleep_dur);
                        sleep_for_secs(sleep_dur);
                        parallelism.increment_experiment_num();
                    }
                } else {
                    println!(
                        "===== Parallelism: {} =====",
                        parallelism.to_csv()
                    );
                    let results_path = make_results_path(
                        &self.get_name(),
                        &[
                            &("w".to_owned() + &par_w.to_string()),
                            &("n".to_owned() + &par_n.to_string()),
                        ],
                    );
                    for &rate in rates_per_milli {
                        params.set_rate(rate);
                        println!("=== Input Rate (events/ms): {} ===", rate);
                        self.run(
                            default_params,
                            &mut parallelism,
                            results_path,
                        );
                    }
                }
            }
        }
    }
}
