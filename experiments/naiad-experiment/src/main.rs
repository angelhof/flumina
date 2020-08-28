extern crate timely;

use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Input, Exchange, Filter, Inspect, Probe, Accumulate};

/*
    To model the value-barrier example, input values
    are ordered pairs. Barriers are (0, 0) and
    values are (1, x) for some integer x.
*/


fn main() {
    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), |worker| {

        let index = worker.index();
        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        println!("[worker {}] initializing", index);

        // create a new input, and inspect its output
        worker.dataflow(|scope| {
            scope.input_from(&mut input)
                // Shuffle events (assign workers based on value)
                .exchange(|(_x, y)| *y)
                // Filter barriers
                .filter(|(x, _y)| *x == 1)
                // Count (for each epoch)
                .count()
                // Print output; probe if desired
                .inspect(move |x| println!("[worker {}]\tcount {}", index, x))
                .probe_with(&mut probe);
        });

        println!("[worker {}] dataflow created", index);

        // introduce input data
        // Each worker has its own input values
        // (but barriers are only at worker 0)
        println!("[worker {}] [input] initial epoch: {}", index, input.epoch());
        let mut epoch = 0; // Initial input.epoch()
        for round in 0..100000 {
            if round % 1000 == 0 {
                // worker 0: sends barrier event
                if index == 0 {
                    input.send((0, 0));
                }
                epoch += 1;
                input.advance_to(epoch);
                println!("[worker {}] [input] new epoch: {}", index, input.epoch());
            }
            input.send((1, round)); // value event

            // Code to step the computation as needed
            while probe.less_than(input.time()) {
                worker.step();
            }
        }
        println!("[worker {}] [input] Done sending input!", index);

        // while probe.less_than(input.time()) {
        //     worker.step();
        // }
        // for _wait_time in 0..1000000 {
        //     worker.step();
        // }

        println!("[worker {}] end of code", index);
    }).unwrap();
}
