/*
Timely code for the Value Barrier example, Version 3.

This is like version 1, but implemented using a custom generator,
and also using more high-level mechanisms provided by timely rather
than low-level state management per worker.
*/

use naiad_experiment::vb_generator::{barrier_source, value_source};
use naiad_experiment::vb_data::{VBData, VBItem};

use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Accumulate, Input, Inspect, Exchange, Probe};

use std::time::Duration;

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {

        // Parameters for the experiment
        let value_frequency = Duration::from_micros(1000);
        let value_total = Duration::from_secs(1);
        let barrier_frequency = Duration::from_micros(100000);
        let mut barrier_total = Duration::from_secs(1);

        // Index of this worker and the total number in existence
        let w_index = worker.index();
        // let w_total = worker.peers();

        /***** 1. Initialization *****/

        // Only generate barriers at worker 0
        if w_index != 0 {
            barrier_total = Duration::from_secs(0);
        }

        let mut b_probe = ProbeHandle::new();
        let mut v_probe = ProbeHandle::new();

        println!("[worker {}] initialized", w_index);

        /***** 2. Create the dataflow *****/

        worker.dataflow(|scope| {
            value_source(scope, w_index, value_frequency, value_total)
            .inspect(|x| println!("value seen: {:?}", x))
            .probe_with(&mut v_probe);

            barrier_source(scope, w_index, barrier_frequency, barrier_total)
            .exchange(|x: &VBItem<u128>| (x.loc as u64))
            .inspect(|x| println!("barrier seen: {:?}", x))
            .probe_with(&mut b_probe);
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
