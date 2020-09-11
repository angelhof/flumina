/*
    Timely code for the Value Barrier example, Version 3.

    This is like version 1, but implemented using a custom generator,
    and also using more high-level mechanisms provided by timely rather
    than low-level state management per worker.
*/

use naiad_experiment::util;
use naiad_experiment::perf::{latency_meter, throughput_meter};
use naiad_experiment::vb_generators::{barrier_source, value_source};

use timely::dataflow::operators::{Accumulate, Broadcast, Map, Inspect, Reclock};
use timely::dataflow::operators::aggregation::Aggregate;

use std::time::Duration;

fn vb_experiment(
    value_frequency: Duration,
    barrier_frequency: Duration,
    experiment_duration: Duration,
)
{
    timely::execute_from_args(std::env::args(), move |worker| {

        /***** 1. Initialization *****/

        let value_total = experiment_duration;
        let mut barrier_total = experiment_duration.clone();

        // Index of this worker and the total number in existence
        let w_index = worker.index();

        // Only generate barriers at worker 0
        if w_index != 0 {
            barrier_total = Duration::from_secs(0);
        }

        println!("[worker {}] initialized", w_index);

        /***** 2. Create the dataflow *****/

        worker.dataflow(move |scope| {

            let barrier_stream =
                barrier_source(scope, w_index, barrier_frequency, barrier_total)
                .broadcast()
                // .inspect(move |x| println!("[worker {}] barrier seen: {:?}",
                //                            w_index, x))
                .map(|_| ()); // drop data to use barrier stream as clock

            let value_source = value_source(
                scope, w_index, value_frequency, value_total
            );

            let out_stream =
                &value_source
                // .inspect(move |x| println!("[worker {}] value seen: {:?}",
                //                            w_index, x))
                .reclock(&barrier_stream)
                // .inspect(move |x| println!("[worker {}] reclocked: {:?}",
                //                            w_index, x))
                .count()
                // .inspect(move |x| println!("[worker {}] count: {:?}",
                //                            w_index, x))
                .map(|x| (0, x))
                .aggregate(
                    |_key, val, agg| { *agg += val; },
                    |_key, agg: usize| agg,
                    |_key| 0,
                )
                .inspect(move |x| println!("[worker {}] total: {:?}",
                                           w_index, x));

            // volume_meter(&value_source);
            // completion_meter(&out_stream);
            latency_meter(&out_stream);
            throughput_meter(&value_source, &out_stream);
        });

        println!("[worker {}] dataflow created", w_index);

    }).unwrap();

}

fn main() {
    let value_frequency = Duration::from_micros(
        util::get_input("Value frequency in microseconds:")
    );
    let barrier_frequency = Duration::from_millis(
        util::get_input("Barrier frequency in milliseconds:")
    );
    let experiment_duration = Duration::from_secs(
        util::get_input("Total time to run in seconds:")
    );

    vb_experiment(value_frequency, barrier_frequency, experiment_duration);
}
