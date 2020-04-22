extern crate timely;

use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Input, Exchange, Inspect, Probe, Accumulate};

// fn create_dataflow() {
//     // Creates the dataflow (no input).
//     // Returns a handle to the input for producers.
// }

fn main() {
    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), |worker| {

        let index = worker.index();
        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        println!("worker {} initializing", index);

        // create a new input, and inspect its output
        worker.dataflow(|scope| {
            scope.input_from(&mut input)
                 .exchange(|x| *x)
                 // .inspect(move |x| println!("worker {}:\tvalue {}", index, x))
                 .count()
                 .inspect(move |x| println!("worker {}:\tcount {}", index, x))
                 .probe_with(&mut probe);
        });

        println!("worker {} dataflow created", index);

        // introduce input data
        if index == 0 {
            println!("initial epoch: {}", input.epoch());
            let mut epoch = 0; // Initial input.epoch()
            for round in 0..1000 {
                if round % 100 == 0 {
                    // input.send(round); // barrier event
                    epoch += 1;
                    input.advance_to(epoch);
                    // println!("new epoch: {}", input.epoch());
                } else {
                    input.send(round); // value event
                }
                // Now step the computation as needed
                // while probe.less_than(input.time()) {
                //     worker.step();
                // }
                //  else {
                //     input.send(round);              
                // }
            }
            println!("Done sending input!");
        }

        for _wait_time in 0..1000000 {
            worker.step();
        }

        println!("worker {} end of code", index);
    }).unwrap();
}
