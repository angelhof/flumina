/*
    Abstractions for running experiments in Timely.
*/

use super::common::{Scope, Stream};
use super::operators::{save_to_file};
use super::perf::{latency_throughput_meter};

use std::string::String;
use std::vec::Vec;

pub trait ExperimentParams {
    /* Getters */
    fn get_parallelism(&self) -> u64;
    // fn get_filestring(&self) -> String;
    /* Functionality */
    // fn set_output_file(&self);
    fn to_csv(&self) -> String;
    fn timely_args(&self) -> Vec<String> {
        let mut vec : Vec<String> = Vec::new();
        vec.push("-w".to_string());
        vec.push(self.get_parallelism().to_string());
        vec
    }
}

// Trait for experiments
// To use, implement the "build_dataflow" method.
// This trait is largely needed in order to hide the generic scope parameter G,
// which is instead generic in the build_dataflow method. This is necessary
// because Rust generics are weird -- see
// https://stackoverflow.com/questions/37606035/pass-generic-function-as-argument
pub trait LatencyThroughputExperiment<P, I, O>: timely::ExchangeData
where
    P: ExperimentParams + Copy + timely::ExchangeData,
    I: std::fmt::Debug + Clone + timely::Data,
    O: std::fmt::Debug + Clone + timely::Data,
{
    fn get_name(&self) -> String;
    fn build_dataflow<G: Scope<Timestamp = u128>>(
        &self, params: P, scope: &G
    ) -> (Stream<G, I>, Stream<G, O>);
    fn run_core<G: Scope<Timestamp = u128>>(
        &self, params: P, scope: &G, output_filename: &'static str,
    ) {
        let (input, output) = self.build_dataflow(params, scope);
        // Optional other meters, uncomment for testing
        // volume_meter(&input);
        // completion_meter(&output);
        // latency_meter(&output);
        // throughput_meter(&input, &output);
        let latency_throughput = latency_throughput_meter(&input, &output);
        let params_csv = params.to_csv().to_owned();
        save_to_file(
            &latency_throughput,
            &output_filename,
            move |(latency, throughput)| { format!(
                "{}, {} ms, {} events/ms",
                params_csv, latency, throughput
            )}
        );
    }
    fn run(
        &'static self, params: P, output_filename: &'static str,
    ) {
        println!("{} Experiment Parameters: {}", self.get_name(), params.to_csv());
        timely::execute_from_args(params.timely_args().drain(0..), move |worker| {
            let worker_index = worker.index();
            worker.dataflow(move |scope| {
                self.run_core(params, scope, output_filename);
                println!("[worker {}] setup complete", worker_index);
            });
        }).unwrap();
    }
}

// fn latency_throughput_experiment_core<P, G, I, O, F>(
//     params: P,
//     scope: &G,
//     dataflow_builder: F,
//     output_filename: &'static str,
// )
// where
//     P: ExperimentParams + Copy,
//     G: Scope<Timestamp = u128>,
//     I: std::fmt::Debug + Clone + timely::Data + timely::ExchangeData,
//     O: std::fmt::Debug + Clone + timely::Data + timely::ExchangeData,
//     F: LatencyThroughputDataflowBuilder<P, I, O>,
// {
//     let (input, output) = dataflow_builder.build(params, scope);
//     let latency_throughput = latency_throughput_meter(&input, &output);
//     let params_csv = params.to_csv().to_owned();
//     save_to_file(
//         &latency_throughput,
//         &output_filename,
//         move |(latency, throughput)| { format!(
//             "{}, {} ms, {} events/ms",
//             params_csv, latency, throughput
//         )}
//     );
// }

// trait DataflowBuilder<P> {
//     fn build<G, F: FnOnce(&G)>(&self, params: P) -> F;
// }
// //
// pub fn latency_throughput_experiment<P, F>(
//     params: P,
//     dataflow_builder: F,
//     output_filename: &'static str,
// )
// where
//     P: ExperimentParams,
//     F: LatencyThroughputDataflowBuilder<P>,
// {
//     println!("{} Experiment Parameters: {}", params.get_name(), params.to_csv());
//     timely::execute_from_args(params.timely_args().drain(0..), move |worker| {
//         let worker_index = worker.index();
//         worker.dataflow(move |scope| {
//             latency_throughput_experiment_core(
//                 params, scope,
//                 dataflow_gen(params, scope),
//                 output_filename
//             );
//             println!("[worker {}] setup complete", worker_index);
//         });
//     }).unwrap();
// }
