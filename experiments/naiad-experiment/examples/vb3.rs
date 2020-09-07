/*
Timely code for the Value Barrier example, Version 3.

This is like version 1, but implemented using a custom generator,
and also using more high-level mechanisms provided by timely rather
than low-level state management per worker.
*/

use naiad_experiment::vb_generator::{barrier_source, value_source};
// use naiad_experiment::vb_data::{VBData, VBItem};

use timely::dataflow::operators::{Accumulate, Broadcast, Map, Inspect, Reclock};

use std::time::Duration;

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {

        // Parameters for the experiment
        let value_frequency = Duration::from_micros(40000);
        let value_total = Duration::from_secs(1);
        let barrier_frequency = Duration::from_micros(200000);
        let mut barrier_total = Duration::from_secs(1);

        // Index of this worker and the total number in existence
        let w_index = worker.index();
        // let w_total = worker.peers();

        /***** 1. Initialization *****/

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
                .inspect(move |x| println!("[worker {}] barrier seen: {:?}",
                                           w_index, x))
                .map(|_| ()); // drop data to use barrier stream as clock

            let _value_stream =
                value_source(scope, w_index, value_frequency, value_total)
                .inspect(move |x| println!("[worker {}] value seen: {:?}",
                                           w_index, x))
                .reclock(&barrier_stream)
                .inspect(move |x| println!("[worker {}] reclocked: {:?}",
                                           w_index, x))
                .count()
                .inspect(move |x| println!("[worker {}] count: {:?}",
                                           w_index, x));
        });

        println!("[worker {}] dataflow created", w_index);

        /***** 3. Provide input data and run *****/

        // // Step any remaining computation
        // while v_probe.less_than(barrier_input.time()) ||
        //       b_probe.less_than(barrier_input.time()) {
        //     worker.step();
        // }

        println!("[worker {}] end of code", w_index);
    }).unwrap();
}
