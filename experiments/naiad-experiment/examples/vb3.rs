/*
Timely code for the Value Barrier example, Version 3.

This is like version 1, but implemented using a custom generator,
and also using more high-level mechanisms provided by timely rather
than low-level state management per worker.
*/

use naiad_experiment::vb_generator::value_source;
use naiad_experiment::vb_data::{VBData, VBItem};

use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Accumulate, Input, Inspect, Exchange, Probe};

use std::time::Duration;

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {

        // Parameters for the experiment
        let frequency = Duration::from_micros(10000);
        let total = Duration::from_secs(1);

        // Index of this worker and the total number in existence
        let w_index = worker.index();
        let w_total = worker.peers();

        /***** 1. Initialization *****/

        let mut barrier_input = InputHandle::new();
        let mut b_probe = ProbeHandle::new();
        let mut v_probe = ProbeHandle::new();

        // let barriers = Rc::new(RefCell::new(VecDeque::new()));
        // let num_barriers = Rc::new(RefCell::new(0));
        // let max_barrier = Rc::new(RefCell::new(-1));

        println!("[worker {}] initialized", w_index);

        /***** 2. Create the dataflow *****/

        worker.dataflow(|scope| {
            value_source(scope, w_index, frequency, total)
            .inspect(|x| println!("value seen: {:?}", x))
            .probe_with(&mut v_probe);

            scope.input_from(&mut barrier_input)
            .exchange(|x: &VBItem<i64>| (x.loc as u64))
            .inspect(|x| println!("barrier seen: {:?}", x))
            .probe_with(&mut b_probe);
        });

        println!("[worker {}] dataflow created", w_index);

        /***** 3. Provide input data and run *****/

        let mut epoch = 0;
        if w_index == 0 {
            for round in 0..1000 {
                for w_other in 0..w_total {
                    barrier_input.send(VBItem {
                        data: VBData::Barrier,
                        time: round,
                        loc: w_other,
                    });
                    println!("[worker {}] sent barrier: {:?}",
                             w_index, (0, round, w_other));
                }
                epoch += 1;
                barrier_input.advance_to(epoch);
            }
        }

        // Step any remaining computation
        while v_probe.less_than(barrier_input.time()) ||
              b_probe.less_than(barrier_input.time()) {
            worker.step();
        }

        println!("[worker {}] end of code", w_index);
    }).unwrap();
}
