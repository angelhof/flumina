/*
Timely code for the Value Barrier example, Version 2.

For this version, we define a new epoch for every timestamp.
This should allow us to naturally use timely's progress tracking mechanism
to enforce that values and barriers are processed in order, rather than
having to manually implement our own mailbox.

Notes:
- Input values are ordered triples. Barriers are (0, x, i) and values are
  (1, x, i), where x is the value (timestamp) and i is the worker where the
  event should be processed.
*/

extern crate timely;
#[macro_use] extern crate abomonation_derive;

use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Input, Inspect, Exchange, Probe};
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::channels::pact::Pipeline;

use std::vec::Vec;

mod vb_data;
use vb_data::{VBData, VBItem};

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {

        // Index of this worker and the total number in existence
        let w_index = worker.index();
        let w_total = worker.peers();

        /***** 1. Initialization *****/

        println!("[worker {}] initializing", w_index);

        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        /***** 2. Create the dataflow *****/

        worker.dataflow(|scope| {
            scope.input_from(&mut input)
                // Shuffle events (forward barriers to appropriate worker)
                .exchange(|x: &VBItem| (x.loc as u64))
                // .inspect(move |x| {
                //     println!("[worker {}] received: {:?}", w_index, x)
                // })
                // Custom unary stateful update for values and barriers
                .unary(Pipeline, "count values", |_capability, _info| {
                    let mut count = 0;
                    let mut inputs = Vec::new();
                    move |input, output| {
                        while let Some((time, data)) = input.next() {
                            data.swap(&mut inputs);
                            for datum in inputs.drain(..) {
                                match datum.data {
                                    VBData::Barrier => {
                                        let mut session = output.session(&time);
                                        session.give(count);
                                        count = 0;
                                    }
                                    VBData::Value => {
                                        count += 1;
                                    }
                                }
                            }
                        }
                    }
                })
                .inspect(move |x| println!("[worker {}]\tcount {}", w_index, x))
                .probe_with(&mut probe);
        });

        println!("[worker {}] dataflow created", w_index);

        /***** 3. Provide input data and run *****/

        // Each worker has its own input values
        // (but barriers are only at worker 0)
        println!("[worker {}] [input] initial epoch: {}", w_index, input.epoch());
        let mut epoch = 0; // Initial input.epoch()
        for round in 0..10001 {
            // Send barrier
            if w_index == 0 {
                if round % 1000 == 0 {
                    for w_other in 0..w_total {
                        input.send(VBItem {
                            data: VBData::Barrier,
                            time: round,
                            loc: w_other,
                        });
                        println!("[worker {}] sent barrier: {:?}",
                                 w_index, (0, round, w_other));
                    }
                }
            }
            // Send value (except on last round)
            if round != 10000 {
                input.send(VBItem {
                    data: VBData::Value,
                    time: round,
                    loc: w_index,
                });
            }
            epoch += 1;
            input.advance_to(epoch);
            // println!("[worker {}] [input] new epoch: {}",
            //          w_index, input.epoch());
            // Step computation
            while probe.less_than(input.time()) {
                worker.step();
            }
        }

        println!("[worker {}] end of code", w_index);

    }).unwrap();
}
