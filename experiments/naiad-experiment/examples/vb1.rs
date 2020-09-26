/*
Timely code for the Value Barrier example, Version 1.

For this version, we define a new epoch whenever a barrier event occurs.
This is more efficient as it allows batching value events.

Notes:
- Input values are ordered triples. Barriers are (0, x, i) and values are
  (1, x, i), where x is the value (timestamp) and i is the worker where the
  event should be processed.
*/

use naiad_experiment::vb_data::{VBData, VBItem};

use timely::dataflow::operators::{
    Accumulate, Exchange, Input, Inspect, Partition, Probe,
};
use timely::dataflow::{InputHandle, ProbeHandle};

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        // Index of this worker and the total number in existence
        let w_index = worker.index();
        let w_total = worker.peers();

        /***** 1. Initialization *****/

        println!("[worker {}] initializing", w_index);

        let mut input = InputHandle::new();
        let mut b_probe = ProbeHandle::new();
        let mut v_probe = ProbeHandle::new();

        let barriers = Rc::new(RefCell::new(VecDeque::new()));
        let num_barriers = Rc::new(RefCell::new(0));
        let max_barrier = Rc::new(RefCell::new(-1));

        /***** 2. Create the dataflow *****/

        // copies to pass ownership to closure
        let barriers_copy = barriers.clone();
        let num_barriers_copy = num_barriers.clone();
        let max_barrier_copy = max_barrier.clone();
        worker.dataflow(|scope| {
            // Shuffle events (forward barriers to appropriate worker),
            // then separate into values and barriers
            let streams = scope
                .input_from(&mut input)
                .exchange(|x: &VBItem<i64>| (x.loc as u64))
                // .inspect(move |x| {
                //     println!("[worker {}] received: {:?}", w_index, x)
                // })
                .partition(2, |x| match x.data {
                    VBData::Barrier => (0, x),
                    VBData::Value => (1, x),
                });

            // Barrier stream: capture barriers, update max/count
            streams[0]
                .inspect(move |x| {
                    println!("[worker {}]\treceived barrier: {:?}", w_index, x)
                })
                .inspect(move |x| {
                    barriers_copy.borrow_mut().push_back(x.time);
                    *num_barriers_copy.borrow_mut() += 1;
                    // should be in inc order
                    assert!(x.time > *max_barrier_copy.borrow());
                    *max_barrier_copy.borrow_mut() = x.time;
                    println!(
                        "[worker {}]\tmax barrier: {}",
                        w_index,
                        *max_barrier_copy.borrow()
                    )
                })
                .probe_with(&mut b_probe);

            // Value stream: count and then probe
            streams[1]
                // .inspect(move |x| println!("[worker {}]\tvalue: {:?}", w_index, x))
                // Count (for each epoch)
                .count()
                // Print output; probe for progress
                .inspect(move |x| println!("[worker {}]\tcount {}", w_index, x))
                .probe_with(&mut v_probe);
        });

        println!("[worker {}] dataflow created", w_index);

        /***** 3. Provide input data and run *****/

        // Each worker has its own input values
        // (but barriers are only at worker 0)
        println!(
            "[worker {}] [input] initial epoch: {}",
            w_index,
            input.epoch()
        );
        let mut epoch = 0; // Initial input.epoch()
        for round in 0..100001 {
            if w_index == 0 {
                if round % 1000 == 0 {
                    // worker 0: send barrier events, update epoch
                    for w_other in 1..w_total {
                        input.send(VBItem {
                            data: VBData::Barrier,
                            time: round,
                            loc: w_other,
                        });
                        println!(
                            "[worker {}] sent barrier: {:?}",
                            w_index,
                            (0, round, w_other)
                        );
                    }
                    epoch += 1;
                    input.advance_to(epoch);
                    println!(
                        "[worker {}] [input] new epoch: {}",
                        w_index,
                        input.epoch()
                    );
                }
                *max_barrier.borrow_mut() = round
            }
            // MAILBOX LOGIC
            // - If max_barrier is behind the current round, step the computation
            // - Otherwise, update the input epoch if needed
            // - Only after the above is done, release the value event
            while *max_barrier.borrow() < round {
                // println!("[worker {}] [input] stepping: {} < {}",
                //          w_index, *max_barrier.borrow(), round);
                worker.step();
            }
            while !barriers.borrow().is_empty() && round >= barriers.borrow()[0]
            {
                // New Epoch
                barriers.borrow_mut().pop_front();
                if w_index != 0 {
                    epoch += 1;
                    input.advance_to(epoch);
                    println!(
                        "[worker {}] [input] new epoch: {}",
                        w_index,
                        input.epoch()
                    );
                }
            }

            // Send value (except on last round)
            if round != 100000 {
                input.send(VBItem {
                    data: VBData::Value,
                    time: round,
                    loc: w_index,
                });
            }
        }
        println!("[worker {}] [input] done sending input!", w_index);

        // Step any remaining computation
        while v_probe.less_than(input.time()) || b_probe.less_than(input.time())
        {
            worker.step();
        }

        println!("[worker {}] end of code", w_index);
    })
    .unwrap();
}
