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
- Input values are ordered pairs. Barriers are (0, x, i) and values are
  (1, x, i), where x is the value (timestamp) and i is the worker where the
  event should be processed.
*/

extern crate timely;

use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Accumulate, Input, Inspect, Exchange,
                                  Partition, Probe};
use std::collections::VecDeque;

use std::cell::RefCell;
use std::rc::Rc;

// Possibly better design: use struct
// struct worker_data {
//     index: u64,
//     total: u64,
//     barriers: ...
// }

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {

        // Index of this worker and the total number in existence
        let w_index = worker.index();
        let w_total = worker.peers();

        /***** 1. Initialization *****/

        println!("[worker {}] initializing", w_index);

        let mut input = InputHandle::new();
        let mut probe1 = ProbeHandle::new();
        let mut probe2 = ProbeHandle::new();

        let barriers = Rc::new(RefCell::new(VecDeque::new()));
        let num_barriers = Rc::new(RefCell::new(0));
        let max_barrier = Rc::new(RefCell::new(-1));

        /***** 2. Create the dataflow *****/

        // copy of barriers_ref to pass ownership to closure
        let barriers_copy = barriers.clone();
        let num_barriers_copy = num_barriers.clone();
        let max_barrier_copy = max_barrier.clone();
        worker.dataflow(|scope| {
            // Shuffle events (forward barriers to appropriate worker),
            // then separate into values and barriers
            let streams = scope.input_from(&mut input)
                .exchange(|(_x, _y, z): &(u64, i64, usize)| (*z as u64))
                .partition(2, |(x, y, _z)| (x, y));
            let v_stream = &streams[0];
            let b_stream = &streams[1];
            // Barrier stream: capture barriers, update max/count
            b_stream
                .inspect(move |x| {
                    barriers_copy.borrow_mut().push_back(*x);
                    *num_barriers_copy.borrow_mut() += 1;
                    // should be in inc order
                    assert!(*x > *max_barrier_copy.borrow());
                    *max_barrier_copy.borrow_mut() = *x;
                    println!("[worker {}]\tmax barrier {}", w_index, *max_barrier_copy.borrow())
                })
                .probe_with(&mut probe1);
            // Value stream: count and then probe
            v_stream
                // Count (for each epoch)
                .count()
                // Print output; probe for progress
                .inspect(move |x| println!("[worker {}]\tcount {}", w_index, x))
                .probe_with(&mut probe2);
        });

        println!("[worker {}] dataflow created", w_index);

        /***** 3. Provide input data and run *****/

        // Each worker has its own input values
        // (but barriers are only at worker 0)
        println!("[worker {}] [input] initial epoch: {}", w_index, input.epoch());
        let mut epoch = 0; // Initial input.epoch()
        for round in 0..100000 {
            if w_index == 0 && round % 1000 == 0 {
                // worker 0: send barrier event, update epoch
                for w_other in 0..w_total {
                    input.send((0, round, w_other));
                }
                // epoch += 1;
                // input.advance_to(epoch);
                // println!("[worker {}] [input] new epoch: {}", w_index, input.epoch());
            }
            // MAILBOX LOGIC
            // - If max_barrier is behind the current round, step the computation
            // - Otherwise, update the input epoch if needed
            // - Only after the above is done, release the value event
            while *max_barrier.borrow() < round {
                worker.step();
            }
            while round >= barriers.borrow()[0] {
                // New Epoch
                barriers.borrow_mut().pop_front();
                epoch += 1;
                input.advance_to(epoch);
                println!("[worker {}] [input] new epoch: {}", w_index, input.epoch());
            }
            input.send((1, round, w_index));
        }
        println!("[worker {}] [input] Done sending input!", w_index);

        // Not currently used: some methods of stepping the computation
        // while probe1.less_than(input.time()) {
        //     worker.step();
        // }
        // for _wait_time in 0..1000000 {
        //     worker.step();
        // }

        println!("[worker {}] end of code", w_index);
    }).unwrap();
}
