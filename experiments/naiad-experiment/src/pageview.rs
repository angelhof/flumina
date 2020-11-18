/*
    Timely experiments for the Pageview example.

    This contains the core computation logic and functions to run
    different experiments. The data items and generators
    are defined in the other pageview_* modules.
*/

use super::common::{Duration, Scope, Stream};
use super::experiment::{ExperimentParams, LatencyThroughputExperiment};
use super::operators::join_by_timestamp;
use super::pageview_data::PVItem;
use super::pageview_generators::{
    page_partition_function, partitioned_update_source, partitioned_view_source,
};

use abomonation_derive::Abomonation;
use structopt::StructOpt;

use timely::dataflow::operators::{Broadcast, Exchange, Filter, Map, Reclock};

use std::string::String;

/* Experiment data */

const NUM_PAGES: usize = 2;

#[derive(Abomonation, Copy, Clone, Debug, StructOpt)]
pub struct PVExperimentParams {
    pub views_per_milli: u64,
    pub views_per_update: u64,
    pub exp_duration_secs: u64,
}
impl ExperimentParams for PVExperimentParams {
    fn to_csv(&self) -> String {
        format!(
            "{} views/ms, {} views/update, {} s",
            self.views_per_milli, self.views_per_update, self.exp_duration_secs,
        )
    }
    fn to_vec(&self) -> Vec<String> {
        let mut result = Vec::new();
        result.push(self.views_per_milli.to_string());
        result.push(self.views_per_update.to_string());
        result.push(self.exp_duration_secs.to_string());
        result
    }
    fn get_exp_duration_secs(&self) -> u64 {
        self.exp_duration_secs
    }
    fn set_rate(&self, rate_per_milli: u64) -> Self {
        let mut copy = *self;
        copy.views_per_milli = rate_per_milli;
        copy
    }
}

/* Core computation */

// Data source with 2 pages
fn pv_datagen<G>(
    params: PVExperimentParams,
    scope: &G,
    w_index: usize,
) -> (Stream<G, PVItem>, Stream<G, PVItem>)
where
    G: Scope<Timestamp = u128>,
{
    let v_freq = Duration::from_nanos(1000000 / params.views_per_milli);
    let u_freq = v_freq * (params.views_per_update as u32);
    let exp_dur = Duration::from_secs(params.exp_duration_secs);
    let v_stream =
        partitioned_view_source(NUM_PAGES, v_freq, exp_dur, scope, w_index);
    let u_stream =
        partitioned_update_source(NUM_PAGES, u_freq, exp_dur, scope, w_index);
    (v_stream, u_stream)
}

fn pv_dataflow_good<G>(
    views: &Stream<G, PVItem>,
    updates: &Stream<G, PVItem>,
    worker_index: usize,
) -> Stream<G, PVItem>
where
    G: Scope<Timestamp = u128>,
{
    // broadcast updates then filter to only ones relevant to this partition
    let partitioned_updates = updates
        .broadcast()
        .filter(move |x| {
            x.name == page_partition_function(NUM_PAGES, worker_index)
        })
        // .inspect(move |x| {
        //     println!("update {:?} at worker {}", x, worker_index)
        // })
        ;

    // re-timestamp views using updates
    let updates_clock = partitioned_updates.map(|_| ());
    let clocked_views = views.reclock(&updates_clock);

    // join each value with the most recent update
    join_by_timestamp(&partitioned_updates, &clocked_views)
        // .inspect(move |(x, y)| {
        //     println!("Result: ({:?}, {:?}) at worker {}", x, y, worker_index)
        // })
        .map(|(x, _y)| x)
}

fn pv_dataflow_bad<G>(
    views: &Stream<G, PVItem>,
    updates: &Stream<G, PVItem>,
) -> Stream<G, PVItem>
where
    G: Scope<Timestamp = u128>,
{
    // Unlike in the good version, here partition by page name
    // (creates one stream for each page, fails for a small number
    // of pages taking up a large amount of the traffic).

    let partitioned_updates = updates.exchange(|x| x.name);
    let partitioned_views = views.exchange(|x| x.name);

    // The rest of the computation is the same as the 'good' version

    // re-timestamp views using updates
    let updates_clock = partitioned_updates
        // .inspect(move |x| {
        //     println!("Update: {:?}", x)
        // })
        .map(|_| ());
    let clocked_views = partitioned_views.reclock(&updates_clock);

    // join each value with the most recent update
    join_by_timestamp(&partitioned_updates, &clocked_views)
        // .inspect(move |(x, y)| {
        //     println!("Result: ({:?}, {:?})", x, y)
        // })
        .map(|(x, _y)| x)
}

/* Exposed experiments */

#[derive(Abomonation, Copy, Clone, Debug)]
pub struct PVGenExperiment;
impl LatencyThroughputExperiment<PVExperimentParams, PVItem, PVItem>
    for PVGenExperiment
{
    fn get_name(&self) -> String {
        "PVgen".to_owned()
    }
    fn build_dataflow<G: Scope<Timestamp = u128>>(
        &self,
        params: PVExperimentParams,
        scope: &mut G,
        worker_index: usize,
    ) -> (Stream<G, PVItem>, Stream<G, PVItem>) {
        let (views, updates) = pv_datagen(params, scope, worker_index);
        // let output = input.inspect(|x| println!("event generated: {:?}", x));
        (views, updates)
    }
}

#[derive(Abomonation, Copy, Clone, Debug)]
pub struct PVGoodExperiment;
impl LatencyThroughputExperiment<PVExperimentParams, PVItem, PVItem>
    for PVGoodExperiment
{
    fn get_name(&self) -> String {
        "PVgood".to_owned()
    }
    fn build_dataflow<G: Scope<Timestamp = u128>>(
        &self,
        params: PVExperimentParams,
        scope: &mut G,
        worker_index: usize,
    ) -> (Stream<G, PVItem>, Stream<G, PVItem>) {
        let (views, updates) = pv_datagen(params, scope, worker_index);
        let output = pv_dataflow_good(&views, &updates, worker_index);
        (views, output)
    }
}

#[derive(Abomonation, Copy, Clone, Debug)]
pub struct PVBadExperiment;
impl LatencyThroughputExperiment<PVExperimentParams, PVItem, PVItem>
    for PVBadExperiment
{
    fn get_name(&self) -> String {
        "PVbad".to_owned()
    }
    fn build_dataflow<G: Scope<Timestamp = u128>>(
        &self,
        params: PVExperimentParams,
        scope: &mut G,
        worker_index: usize,
    ) -> (Stream<G, PVItem>, Stream<G, PVItem>) {
        let (views, updates) = pv_datagen(params, scope, worker_index);
        let output = pv_dataflow_bad(&views, &updates);
        (views, output)
    }
}

// impl PVExperimentParams {
//     pub fn run_pv_experiment_main(
//         self,
//         par: TimelyParallelism,
//         output_filename: &'static str
//     ) {
//         PVExperiment::from_params(self).run(par, output_filename);
//     }
//     pub fn run_pv_experiment_gen_only(
//         self,
//         par: TimelyParallelism,
//         output_filename: &'static str
//     ) {
//         PVGenExperiment::from_params(self).run(par, output_filename);
//     }
// }
