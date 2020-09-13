/*
    Timely experiments for the Value Barrier example.

    This contains the core computation logic and functions to run
    different experiments. The data items and generators
    are defined in the other vb_* modules.
*/

use super::common::{Duration, Scope, Stream};
use super::perf::{latency_meter, throughput_meter};
use super::vb_data::VBItem;
use super::vb_generators::{barrier_source, value_source};

use timely::dataflow::operators::{Accumulate, Broadcast, Map, Inspect, Reclock};
use timely::dataflow::operators::aggregation::Aggregate;

type Item = VBItem<u128>;

fn vb_dataflow<G>(
    value_stream: &Stream<G, Item>,
    barrier_stream: &Stream<G, Item>,
) -> Stream<G, usize>
where
    G: Scope<Timestamp = u128>,
{
    // Use barrier stream as clock, dropping data
    let barrier_clock =
        barrier_stream
        .broadcast()
        // .inspect(move |x| println!("barrier seen: {:?}", x))
        .map(|_| ());

    value_stream
        // .inspect(move |x| println!("value seen: {:?}", x))
        .reclock(&barrier_clock)
        // .inspect(move |x| println!("reclocked: {:?}", x))
        .count()
        // .inspect(move |x| println!("count: {:?}", x))
        .map(|x| (0, x))
        .aggregate(
            |_key, val, agg| { *agg += val; },
            |_key, agg: usize| agg,
            |_key| 0,
        )
        .inspect(move |x| println!("total: {:?}", x))
}

fn vb_do_nothing<G>(
    value_stream: &Stream<G, Item>,
    barrier_stream: &Stream<G, Item>,
) -> Stream<G, Item>
where
    G: Scope<Timestamp = u128>,
{
    barrier_stream
    .inspect(|x| println!("barrier generated: {:?}", x));
    value_stream
    .inspect(|x| println!("value generated: {:?}", x))
}

fn vb_experiment_core<G, O, F>(
    val_frequency: Duration,
    bar_frequency: Duration,
    exp_duration: Duration,
    scope: &G,
    computation: F,
    worker_index: usize,
)
where
    G: Scope<Timestamp = u128>,
    O: std::fmt::Debug + Clone + timely::Data + timely::ExchangeData,
    F: FnOnce(&Stream<G, Item>, &Stream<G, Item>) -> Stream<G, O> + 'static,
{
    /* 1. Initialize */

    let val_total = exp_duration;
    let mut bar_total = exp_duration.clone();
    // Only generate barriers at worker 0
    if worker_index != 0 {
        bar_total = Duration::from_secs(0);
    }

    /* 2. Create the Dataflow */

    let bars = barrier_source(scope, worker_index, bar_frequency, bar_total);
    let vals = value_source(scope, worker_index, val_frequency, val_total);
    let output = computation(&bars, &vals);

    /* 3. Monitor the Performance */

    // volume_meter(&vals);
    // completion_meter(&output);
    latency_meter(&output);
    throughput_meter(&vals, &output);

    println!("[worker {}] setup complete", worker_index);
}

pub fn vb_experiment_main(
    val_frequency: Duration,
    bar_frequency: Duration,
    exp_duration: Duration,
) {
    timely::execute_from_args(std::env::args(), move |worker| {
        let worker_index = worker.index();
        worker.dataflow(move |scope| {
            vb_experiment_core(
                val_frequency, bar_frequency, exp_duration, scope,
                |s1, s2| vb_dataflow(s1, s2),
                worker_index
            );
        });
    }).unwrap();
}

pub fn vb_experiment_gen_only(
    val_frequency: Duration,
    bar_frequency: Duration,
    exp_duration: Duration,
) {
    timely::execute_from_args(std::env::args(), move |worker| {
        let worker_index = worker.index();
        worker.dataflow(move |scope| {
            vb_experiment_core(
                val_frequency, bar_frequency, exp_duration, scope,
                |s1, s2| vb_do_nothing(s1, s2),
                worker_index
            );
        });
    }).unwrap();
}
