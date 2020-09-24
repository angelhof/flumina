/*
    Timely experiments for the Value Barrier example.

    This contains the core computation logic and functions to run
    different experiments. The data items and generators
    are defined in the other vb_* modules.
*/

use abomonation_derive::Abomonation;

use super::common::{Duration, Scope, Stream};
use super::experiment::{ExperimentParams, LatencyThroughputExperiment};
use super::operators::{Sum};
use super::vb_data::{VBData, VBItem};
use super::vb_generators::{barrier_source, value_source};

use timely::dataflow::operators::{Accumulate, Broadcast, Exchange, Filter,
                                  Inspect, Map, Reclock};

use std::string::String;

/* Experiment data */

#[derive(Abomonation, Copy, Clone, Debug)]
pub struct VBExperimentParams {
    pub parallelism: u64,
    pub val_rate_per_milli: u64,
    pub vals_per_hb_per_worker: u64,
    pub hbs_per_bar: u64,
    pub exp_duration_secs: u64,
}
impl ExperimentParams for VBExperimentParams {
    fn get_parallelism(&self) -> u64 { self.parallelism }
    fn to_csv(&self) -> String {
        format!(
            "{} wkrs, {} vals/ms, {} val/hb/wkr, {} hb/bar, {} s",
            self.parallelism,
            self.val_rate_per_milli,
            self.vals_per_hb_per_worker,
            self.hbs_per_bar,
            self.exp_duration_secs,
        )
    }
}

/* Core computation */

fn vb_datagen<G>(
    params: VBExperimentParams,
    scope: &G,
    worker_index: usize,
) -> (Stream<G, VBItem>, Stream<G, VBItem>)
where
    G: Scope<Timestamp = u128>,
{
    // Calculate parameters
    let val_frequency = Duration::from_nanos(1000000 / params.val_rate_per_milli);
    let val_total = Duration::from_secs(params.exp_duration_secs);
    let mut bar_total = val_total.clone();
    if worker_index != 0 {
        // Only generate barriers at worker 0
        bar_total = Duration::from_secs(0);
    }
    let hb_frequency = val_frequency.mul_f64(params.vals_per_hb_per_worker as f64);
    // Return the two source streams
    let bars = barrier_source(
        scope, worker_index, hb_frequency, params.hbs_per_bar, bar_total
    );
    let vals = value_source(scope, worker_index, val_frequency, val_total);
    (bars, vals)
}

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

/* Exposed experiments */

#[derive(Abomonation, Copy, Clone, Debug)]
struct VBGenExperiment;
impl LatencyThroughputExperiment<
    VBExperimentParams, VBItem, VBItem
> for VBGenExperiment {
    fn get_name(&self) -> String { "VBgen".to_owned() }
    fn build_dataflow<G: Scope<Timestamp = u128>>(
        &self, params: VBExperimentParams, scope: &G, worker_index: usize,
    ) -> (Stream<G, VBItem>, Stream<G, VBItem>) {
        let (vals, bars) = vb_datagen(params, scope, worker_index);
        let output = vals.inspect(|x| println!("event generated: {:?}", x));
        bars.inspect(|x| println!("event generated: {:?}", x));
        (vals, output)
    }
}

#[derive(Abomonation, Copy, Clone, Debug)]
struct VBExperiment;
impl LatencyThroughputExperiment<
    VBExperimentParams, VBItem, usize
> for VBExperiment {
    fn get_name(&self) -> String { "VB".to_owned() }
    fn build_dataflow<G: Scope<Timestamp = u128>>(
        &self, params: VBExperimentParams, scope: &G, worker_index: usize,
    ) -> (Stream<G, VBItem>, Stream<G, usize>) {
        let (vals, bars) = vb_datagen(params, scope, worker_index);
        let output = vb_dataflow(&vals, &bars);
        (vals, output)
    }
}

impl VBExperimentParams {
    pub fn run_vb_experiment_main(self, output_filename: &'static str) {
        VBExperiment.run(self, output_filename);
    }
    pub fn run_vb_experiment_gen_only(self, output_filename: &'static str) {
        VBGenExperiment.run(self, output_filename);
    }
}
