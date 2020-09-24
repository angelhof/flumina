/*
    Timely experiments for the Pageview example.

    This contains the core computation logic and functions to run
    different experiments. The data items and generators
    are defined in the other pageview_* modules.
*/

use super::common::{Duration, Scope, Stream};
use super::operators::{save_to_file};
use super::perf::{latency_throughput_meter};
use super::pageview_data::{PVItem};
use super::pageview_generators::{pageview_source_twopages};

use timely::dataflow::operators::{Inspect};
// use timely::dataflow::operators::{Accumulate, Broadcast, Exchange, Filter,
//                                   Inspect, Map, Reclock};

use std::vec::Vec;
use std::string::String;

/* Experiment data */

#[derive(Debug, Copy, Clone)]
pub struct PVExperimentData {
    parallelism: u64,
    page0_per_page1: u64,
    views_per_update: u64,
    events_per_milli: u64,
    exp_duration_secs: u64,
}
impl PVExperimentData {
    pub fn to_csv(&self) -> String {
        format!(
            "{} wkrs, {} page0/page1, {} views/update, {} events/ms, {} s",
            self.parallelism,
            self.page0_per_page1,
            self.views_per_update,
            self.events_per_milli,
            self.exp_duration_secs,
        )
    }
    pub fn timely_args(&self) -> Vec<String>
    {
        let mut vec : Vec<String> = Vec::new();
        vec.push("-w".to_string());
        vec.push(self.parallelism.to_string());
        vec
    }
}

/* Core computation */

fn pv_gen_only<G>(
    pv_stream: &Stream<G, PVItem>,
) -> Stream<G, PVItem>
where
    G: Scope<Timestamp = u128>,
{
    pv_stream
    .inspect(|x| println!("event generated: {:?}", x))
}

fn pv_experiment_core<G, O, F>(
    params: PVExperimentData,
    scope: &G,
    computation: F,
    output_filename: &'static str,
)
where
    G: Scope<Timestamp = u128>,
    O: std::fmt::Debug + Clone + timely::Data + timely::ExchangeData,
    F: FnOnce(&Stream<G, PVItem>) -> Stream<G, O> + 'static,
{
    /* 1. Initialize */

    let page_0_prob = 1.0 / (params.page0_per_page1 as f64 + 1.0);
    let update_prob = 1.0 / (params.views_per_update as f64 + 1.0);
    let frequency = Duration::from_nanos(1000000 / params.events_per_milli);
    let exp_duration = Duration::from_secs(params.exp_duration_secs);

    /* 2. Create the Dataflow */

    let input = pageview_source_twopages(
        scope, page_0_prob, update_prob, frequency, exp_duration
    );
    let output = computation(&input);

    /* 3. Monitor the Performance */

    // volume_meter(&input);
    // completion_meter(&output);
    // latency_meter(&output);
    // throughput_meter(&input, &output);
    let latency_throughput = latency_throughput_meter(&input, &output);
    save_to_file(
        &latency_throughput,
        &output_filename,
        move |(latency, throughput)| { format!(
            "{}, {} ms, {} events/ms",
            params.to_csv(), latency, throughput
        )}
    );
}

/* Exposed experiments */

pub fn pv_experiment_gen_only(
    params: PVExperimentData,
    output_filename: &'static str,
) {
    println!("PVgen Experiment Parameters: {}", params.to_csv());
    timely::execute_from_args(params.timely_args().drain(0..), move |worker| {
        let worker_index = worker.index();
        worker.dataflow(move |scope| {
            pv_experiment_core(
                params, scope,
                |s| pv_gen_only(s),
                output_filename,
            );
            println!("[worker {}] setup complete", worker_index);
        });
    }).unwrap();
}
