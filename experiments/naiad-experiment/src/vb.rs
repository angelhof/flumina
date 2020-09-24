/*
    Timely experiments for the Value Barrier example.

    This contains the core computation logic and functions to run
    different experiments. The data items and generators
    are defined in the other vb_* modules.
*/

use super::common::{Duration, Scope, Stream};
use super::operators::{save_to_file, Sum};
use super::perf::{latency_throughput_meter};
use super::vb_data::{VBData, VBItem};
use super::vb_generators::{barrier_source, value_source};

use timely::dataflow::operators::{Accumulate, Broadcast, Exchange, Filter,
                                  Inspect, Map, Reclock};

use std::string::String;

/* Experiment data */

#[derive(Debug, Copy, Clone)]
pub struct VBExperimentData {
    pub parallelism: u64,
    pub val_rate_per_milli: u64,
    pub vals_per_hb_per_worker: f64,
    pub hbs_per_bar: u64,
    pub exp_duration_secs: u64,
}
impl VBExperimentData {
    pub fn to_csv(&self) -> String {
        format!(
            "{} wkrs, {} vals/ms, {} val/hb/wkr, {} hb/bar, {} s",
            self.parallelism,
            self.val_rate_per_milli,
            self.vals_per_hb_per_worker,
            self.hbs_per_bar,
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

fn vb_dataflow<G>(
    value_stream: &Stream<G, VBItem>,
    barrier_stream: &Stream<G, VBItem>,
) -> Stream<G, usize>
where
    G: Scope<Timestamp = u128>,
{
    // Use barrier stream to create two clocks, one with
    // hearbeats and one without
    let barrier_broadcast = barrier_stream
        .broadcast();
    let barrier_clock_withheartbeats = barrier_broadcast
        // .inspect(move |x| println!("barrier or heartbeat seen: {:?}", x))
        .map(|_| ());
    let barrier_clock_noheartbeats = barrier_broadcast
        .filter(|x| x.data == VBData::Barrier)
        // .inspect(move |x| println!("barrier seen: {:?}", x))
        .map(|_| ());

    value_stream
        // .inspect(move |x| println!("value seen: {:?}", x))
        .reclock(&barrier_clock_withheartbeats)
        // .inspect(move |x| println!("reclocked: {:?}", x))
        .count()
        // .inspect(move |x| println!("count per heartbeat: {:?}", x))
        .reclock(&barrier_clock_noheartbeats)
        // .inspect(move |x| println!("reclocked: {:?}", x))
        .sum()
        // .inspect(move |x| println!("count: {:?}", x))
        .exchange(|_x| 0)
        .sum()
        // .inspect(move |x| println!("total: {:?}", x))
}

fn vb_gen_only<G>(
    value_stream: &Stream<G, VBItem>,
    barrier_stream: &Stream<G, VBItem>,
) -> Stream<G, VBItem>
where
    G: Scope<Timestamp = u128>,
{
    barrier_stream
    .inspect(|x| println!("barrier generated: {:?}", x));
    value_stream
    // .inspect(|_x| {})
    .inspect(|x| println!("value generated: {:?}", x))
}

fn vb_experiment_core<G, O, F>(
    params: VBExperimentData,
    scope: &G,
    computation: F,
    worker_index: usize,
    output_filename: &'static str,
)
where
    G: Scope<Timestamp = u128>,
    O: std::fmt::Debug + Clone + timely::Data + timely::ExchangeData,
    F: FnOnce(&Stream<G, VBItem>, &Stream<G, VBItem>) -> Stream<G, O> + 'static,
{
    /* 1. Initialize */

    let val_frequency = Duration::from_nanos(1000000 / params.val_rate_per_milli);
    let val_total = Duration::from_secs(params.exp_duration_secs);
    let mut bar_total = val_total.clone();
    if worker_index != 0 {
        // Only generate barriers at worker 0
        bar_total = Duration::from_secs(0);
    }
    let hb_frequency = val_frequency.mul_f64(params.vals_per_hb_per_worker);

    /* 2. Create the Dataflow */

    let bars = barrier_source(
        scope, worker_index, hb_frequency, params.hbs_per_bar, bar_total
    );
    let vals = value_source(scope, worker_index, val_frequency, val_total);
    let output = computation(&vals, &bars);

    /* 3. Monitor the Performance */

    // volume_meter(&vals);
    // completion_meter(&output);
    // latency_meter(&output);
    // throughput_meter(&vals, &output);
    let latency_throughput = latency_throughput_meter(&vals, &output);
    save_to_file(
        &latency_throughput,
        &output_filename,
        move |(latency, throughput)| { format!(
            "{}, {} ms, {} events/ms",
            params.to_csv(), latency, throughput,
        )}
    );
}

/* Exposed experiments */

pub fn vb_experiment_main(
    params: VBExperimentData,
    output_filename: &'static str,
) {
    println!("VB Experiment Parameters: {}", params.to_csv());
    timely::execute_from_args(params.timely_args().drain(0..), move |worker| {
        let worker_index = worker.index();
        worker.dataflow(move |scope| {
            vb_experiment_core(
                params, scope,
                |s1, s2| vb_dataflow(s1, s2),
                worker_index, output_filename
            );
            println!("[worker {}] setup complete", worker_index);
        });
    }).unwrap();
}

pub fn vb_experiment_gen_only(
    params: VBExperimentData,
    output_filename: &'static str,
) {
    println!("VBgen Experiment Parameters: {}", params.to_csv());
    timely::execute_from_args(params.timely_args().drain(0..), move |worker| {
        let worker_index = worker.index();
        worker.dataflow(move |scope| {
            vb_experiment_core(
                params, scope,
                |s1, s2| vb_gen_only(s1, s2),
                worker_index, output_filename,
            );
            println!("[worker {}] setup complete", worker_index);
        });
    }).unwrap();
}
