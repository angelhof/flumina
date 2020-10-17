/*
    Abstractions for running experiments in Timely.
*/

use super::common::{Scope, Stream};
use super::ec2::{get_ec2_node_number, prepare_ec2_host_file};
use super::operators::save_to_file;
use super::perf::latency_throughput_meter;

use abomonation_derive::Abomonation;

use std::string::String;
use std::vec::Vec;

const EC2_STARTING_PORT: u64 = 4000;

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
    set_rate should vary one or more of the parameters to set the
    input throughput (in events / ms), which can be used to test throughput.
*/
pub trait ExperimentParams: Copy + timely::ExchangeData {
    fn to_csv(&self) -> String;
    // fn set_rate(&mut self, rate_per_milli: u64);
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
    // Functionality to implement
    fn get_name(&self) -> String;
    fn build_dataflow<G: Scope<Timestamp = u128>>(
        &self,
        params: P,
        scope: &mut G,
        worker_index: usize,
    ) -> (Stream<G, I>, Stream<G, O>);

    // Functionality provided
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
    fn run(
        &'static self,
        params: P,
        parallelism: &mut TimelyParallelism,
        output_filename: &'static str
    ) {
        println!(
            "{} Experiment Parameters: {}, Parallelism: {}",
            self.get_name(),
            params.to_csv(),
            parallelism.to_csv(),
        );
        let mut args = parallelism.timely_args();
        let parallelism_copy = parallelism.clone();
        println!("Timely args: {:?}", args);
        timely::execute_from_args(
            args.drain(0..),
            move |worker| {
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
            },
        )
        .unwrap();
    }
}
