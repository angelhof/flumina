/*
    Timely experiments for the Pageview example.

    This contains the core computation logic and functions to run
    different experiments. The data items and generators
    are defined in the other pageview_* modules.
*/

use abomonation_derive::Abomonation;

use super::common::{Duration, Scope, Stream};
use super::experiment::{ExperimentParams, LatencyThroughputExperiment};
use super::pageview_data::{PVItem};
use super::pageview_generators::{pageview_source_twopages};

use timely::dataflow::operators::{Inspect};
// use timely::dataflow::operators::{Accumulate, Broadcast, Exchange, Filter,
//                                   Inspect, Map, Reclock};

use std::string::String;

/* Experiment data */

#[derive(Abomonation, Copy, Clone, Debug)]
pub struct PVExperimentParams {
    parallelism: u64,
    page0_per_page1: u64,
    views_per_update: u64,
    events_per_milli: u64,
    exp_duration_secs: u64,
}
impl ExperimentParams for PVExperimentParams {
    fn get_parallelism(&self) -> u64 { self.parallelism }
    fn to_csv(&self) -> String {
        format!(
            "{} wkrs, {} page0/page1, {} views/update, {} events/ms, {} s",
            self.parallelism,
            self.page0_per_page1,
            self.views_per_update,
            self.events_per_milli,
            self.exp_duration_secs,
        )
    }
}

/* Core computation */

fn pv_datagen<G>(
    params: PVExperimentParams,
    scope: &G,
) -> Stream<G, PVItem>
where
    G: Scope<Timestamp = u128>,
{
    let page_0_prob = 1.0 / (params.page0_per_page1 as f64 + 1.0);
    let update_prob = 1.0 / (params.views_per_update as f64 + 1.0);
    let frequency = Duration::from_nanos(1000000 / params.events_per_milli);
    let exp_duration = Duration::from_secs(params.exp_duration_secs);

    pageview_source_twopages(scope, page_0_prob, update_prob, frequency, exp_duration)
}

/* Exposed experiments */

#[derive(Abomonation, Copy, Clone, Debug)]
pub struct PVGenExperiment;
impl LatencyThroughputExperiment<
    PVExperimentParams, PVItem, PVItem
> for PVGenExperiment {
    fn get_name(&self) -> String { "PVgen".to_owned() }
    fn build_dataflow<G: Scope<Timestamp = u128>>(
        &self, params: PVExperimentParams, scope: &G, _worker_index: usize,
    ) -> (Stream<G, PVItem>, Stream<G, PVItem>) {
        let input = pv_datagen(params, scope);
        let output = input.inspect(|x| println!("event generated: {:?}", x));
        (input, output)
    }
}
