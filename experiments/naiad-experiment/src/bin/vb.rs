/*
Timely code for the Value Barrier example.

There will eventually be two examples of this code, differing in
how they define epochs for the computation.
- Option 1 is to define a new epoch whenever a barrier event occurs.
  This is more efficient as it allows batching value events.
- Option 2 is to define a new epoch for every timestamp.
  This should allow us to naturally use timely's progress tracking mechanism
  to enforce that values and barriers are processed in order, rather than
  having to manually implement our own mailbox.

Notes:
- Input values are ordered pairs. Barriers are (0, 0, i) where i is the
  worker it should be sent to.
  Values are (1, x, i) where x is the value and i is the worker it was
  generated at.
*/

extern crate timely;

use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Input, Exchange, Filter, Inspect, Probe, Accumulate};

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {

        /***** 1. Initialization *****/

        // Index of this worker and the total number in existence
        let w_index = worker.index();
        let w_total = worker.peers();

        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        println!("[worker {}] initializing", w_index);

        /***** 2. Create the dataflow *****/

        worker.dataflow(|scope| {
            scope.input_from(&mut input)
                // Shuffle events (forward barriers to appropriate worker)
                .exchange(|(_x, _y, z)| (*z as u64))
                // Filter out barriers
                .filter(|(x, _y, _z)| *x == 1)
                // Count (for each epoch)
                .count()
                // Print output; probe for progress
                .inspect(move |x| println!("[worker {}]\tcount {}", w_index, x))
                .probe_with(&mut probe);
        });

        println!("[worker {}] dataflow created", w_index);

        /***** 3. Provide input data and run *****/

        // Each worker has its own input values
        // (but barriers are only at worker 0)
        println!("[worker {}] [input] initial epoch: {}", w_index, input.epoch());
        let mut epoch = 0; // Initial input.epoch()
        for round in 0..100000 {
            if round % 1000 == 0 {
                // worker 0: sends barrier event
                if w_index == 0 {
                    for w_other in 0..w_total {
                        input.send((0, 0, w_other));
                    }
                    epoch += 1;
                    input.advance_to(epoch);
                    println!("[worker {}] [input] new epoch: {}", w_index, input.epoch());
                }
            }
            input.send((1, round, w_index)); // value event

            // Code to step the computation as needed
            while probe.less_than(input.time()) {
                worker.step();
            }
        }
        println!("[worker {}] [input] Done sending input!", w_index);

        // while probe.less_than(input.time()) {
        //     worker.step();
        // }
        // for _wait_time in 0..1000000 {
        //     worker.step();
        // }

        println!("[worker {}] end of code", w_index);
    }).unwrap();
}
