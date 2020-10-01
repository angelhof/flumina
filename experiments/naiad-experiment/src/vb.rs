/*
    Timely experiments for the Value Barrier and Fraud Detection examples.

    This contains the core computation logic and functions to run
    different experiments. The data items and generators
    are defined in the other vb_* modules.
*/

use super::common::{Duration, Scope, Stream};
use super::experiment::{
    ExperimentParams, LatencyThroughputExperiment, TimelyParallelism,
};
use super::operators::{join_by_timestamp, Sum};
use super::vb_data::{VBData, VBItem};
use super::vb_generators::{barrier_source, value_source};

use abomonation_derive::Abomonation;

use timely::dataflow::operators::{
    Accumulate, Broadcast, Concat, ConnectLoop, Exchange, Feedback, Filter,
    Inspect, Map, Reclock, ToStream,
};

use std::string::String;

/* Experiment data */

#[derive(Abomonation, Copy, Clone, Debug)]
pub struct VBExperimentParams {
    pub parallelism: TimelyParallelism,
    pub val_rate_per_milli: u64,
    pub vals_per_hb_per_worker: u64,
    pub hbs_per_bar: u64,
    pub exp_duration_secs: u64,
}
impl ExperimentParams for VBExperimentParams {
    fn get_parallelism(&self) -> TimelyParallelism {
        self.parallelism
    }
    fn to_csv(&self) -> String {
        format!(
            "{}, {} vals/ms, {} val/hb/wkr, {} hb/bar, {} s",
            self.parallelism.to_csv(),
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
    let val_frequency =
        Duration::from_nanos(1000000 / params.val_rate_per_milli);
    let val_duration = Duration::from_secs(params.exp_duration_secs);
    let bar_duration = if worker_index != 0 {
        // Only generate barriers at worker 0
        Duration::from_secs(0)
    } else {
        val_duration
    };
    let hb_frequency = val_frequency * (params.vals_per_hb_per_worker as u32);
    // Return the two source streams
    let bars = barrier_source(
        scope,
        worker_index,
        hb_frequency,
        params.hbs_per_bar,
        bar_duration,
    );
    let vals = value_source(scope, worker_index, val_frequency, val_duration);
    (vals, bars)
}

#[rustfmt::skip]
fn vb_dataflow<G>(
    value_stream: &Stream<G, VBItem>,
    barrier_stream: &Stream<G, VBItem>,
) -> Stream<G, usize>
where
    G: Scope<Timestamp = u128>,
{
    // Use barrier stream to create two clocks, one with
    // hearbeats and one without
    let barrier_broadcast = barrier_stream.broadcast();
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
        .map(|x| match x.data {
            VBData::Value(v) => v,
            VBData::BarrierHeartbeat => unreachable!(),
            VBData::Barrier => unreachable!(),
        })
        .sum()
        // .inspect(move |x| println!("count per heartbeat: {:?}", x))
        .reclock(&barrier_clock_noheartbeats)
        // .inspect(move |x| println!("reclocked: {:?}", x))
        .sum()
        // .inspect(move |x| println!("count: {:?}", x))
        .exchange(|_x| 0)
        .sum()
        // .inspect(move |x| println!("total: {:?}", x))
}

// Fraud detection version!
// Because this is iterative, needs the scope
#[rustfmt::skip]
fn fd_dataflow<G>(
    value_stream: &Stream<G, VBItem>,
    barrier_stream: &Stream<G, VBItem>,
    scope: &mut G,
) -> Stream<G, VBItem>
where
    G: Scope<Timestamp = u128>,
{
    // Use barrier stream to create two clocks, one with
    // hearbeats and one without
    let barrier_broadcast = barrier_stream.broadcast();
    // Currently unused
    // let barrier_clock_withheartbeats = barrier_broadcast
    //     // .inspect(move |x| println!("barrier or heartbeat seen: {:?}", x))
    //     .map(|_| ());
    let barrier_clock_noheartbeats = barrier_broadcast
        .filter(|x| x.data == VBData::Barrier)
        // .inspect(move |x| println!("barrier seen: {:?}", x))
        .map(|_| ());

    // Create cycle / feedback loop with trained model based on the values
    let (handle, model_stream) = scope.feedback(1);
    // start with a model with value 0
    let init_model = (0..1).to_stream(scope);
    let model_broadcast = model_stream
        .concat(&init_model)
        // "line up" the stream with barrier clock, then broadcast
        .reclock(&barrier_clock_noheartbeats)
        // .inspect(move |x| println!("model: {:?}", x))
        .broadcast();

    // Calculate aggregate of values between windows, but this
    // time also given the previous state (trained model)
    // NB: In this simplified computation we don't need the trained
    //     model to aggregate the values, only to produce output per
    //     value. Because of this, it's possible to do the computation
    //     without the iteration (copying the value stream, and classifying
    //     one stream while aggregating the other).
    //     But in this form it is more general and the value processing
    //     can be an arbitrary stateful function taking the previous trained
    //     model as input.
    let value_reclocked = value_stream
        // .inspect(move |x| println!("value seen: {:?}", x))
        // NB: The following ignores the heartbeats which is a bit inefficient
        //     in terms of latency.
        .reclock(&barrier_clock_noheartbeats);
        // .inspect(move |x| println!("reclocked: {:?}", x));

    // Use the model to predict
    // Includes core logic for how prediction works in this simplified example.
    let value_labeled = join_by_timestamp(&model_broadcast, &value_reclocked)
        // .inspect(move |x| println!("joined: {:?}", x))
        .map(|(model, value)| {
            let label = match value.data {
                VBData::Value(v) => model % 1000 == v,
                VBData::BarrierHeartbeat => unreachable!(),
                VBData::Barrier => unreachable!(),
            };
            (value, label)
        });

    // Aggregate the (labeled) values to calculate the next model
    // In order to avoid a bug when there are no values in between
    // two barriers, we also need to insert a 0 for each barrier.
    let barrier_zeros = barrier_clock_noheartbeats
        .map(|()| 0);
    value_labeled
        // .inspect(move |x| println!("labeled: {:?}", x))
        .count()
        // .inspect(move |x| println!("count per heartbeat: {:?}", x))
        .reclock(&barrier_clock_noheartbeats)
        // .inspect(move |x| println!("reclocked: {:?}", x))
        .sum()
        // .inspect(move |x| println!("count: {:?}", x))
        .concat(&barrier_zeros)
        .exchange(|_x| 0)
        .sum()
        // .inspect(move |x| println!("total: {:?}", x))
        .connect_loop(handle);

    // Output labels that were labeled fraudulent
    value_labeled
        .filter(|(_value, label)| *label)
        .map(|(value, _label)| value)
        // .inspect(|value| println!("Fraudulent: {:?}", value))
}

/* Exposed experiments */

#[derive(Abomonation, Copy, Clone, Debug)]
struct VBGenExperiment;
impl LatencyThroughputExperiment<VBExperimentParams, VBItem, VBItem>
    for VBGenExperiment
{
    fn get_name(&self) -> String {
        "VBgen".to_owned()
    }
    fn build_dataflow<G: Scope<Timestamp = u128>>(
        &self,
        params: VBExperimentParams,
        scope: &mut G,
        worker_index: usize,
    ) -> (Stream<G, VBItem>, Stream<G, VBItem>) {
        let (vals, bars) = vb_datagen(params, scope, worker_index);
        let output = vals.clone();
        // let output = vals.inspect(|x| println!("event generated: {:?}", x));
        bars.inspect(|x| println!("event generated: {:?}", x));
        (vals, output)
    }
}

#[derive(Abomonation, Copy, Clone, Debug)]
struct VBExperiment;
impl LatencyThroughputExperiment<VBExperimentParams, VBItem, usize>
    for VBExperiment
{
    fn get_name(&self) -> String {
        "VB".to_owned()
    }
    fn build_dataflow<G: Scope<Timestamp = u128>>(
        &self,
        params: VBExperimentParams,
        scope: &mut G,
        worker_index: usize,
    ) -> (Stream<G, VBItem>, Stream<G, usize>) {
        let (vals, bars) = vb_datagen(params, scope, worker_index);
        let output = vb_dataflow(&vals, &bars);
        (vals, output)
    }
}

#[derive(Abomonation, Copy, Clone, Debug)]
struct FDExperiment;
impl LatencyThroughputExperiment<VBExperimentParams, VBItem, VBItem>
    for FDExperiment
{
    fn get_name(&self) -> String {
        "FD".to_owned()
    }
    fn build_dataflow<G: Scope<Timestamp = u128>>(
        &self,
        params: VBExperimentParams,
        scope: &mut G,
        worker_index: usize,
    ) -> (Stream<G, VBItem>, Stream<G, VBItem>) {
        let (vals, bars) = vb_datagen(params, scope, worker_index);
        let output = fd_dataflow(&vals, &bars, scope);
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
    pub fn run_fd_experiment(self, output_filename: &'static str) {
        FDExperiment.run(self, output_filename);
    }
}
