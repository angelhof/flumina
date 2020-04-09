extern crate timely;

// use timely::dataflow::{InputHandle, ProbeHandle};
// use timely::dataflow::operators::{Input, Exchange, Inspect, Probe};
use timely::dataflow::InputHandle;
use timely::dataflow::operators::{Input, Exchange, Inspect};

fn main() {
    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), |worker| {

        let index = worker.index();
        let mut input = InputHandle::new();
        // let mut probe = ProbeHandle::new();

        // create a new input, and inspect its output
        worker.dataflow(|scope| {
            scope.input_from(&mut input)
                 .exchange(|x| *x)
                 .inspect(move |x| println!("worker {}:\thello {}", index, x));
                 // .probe_with(&mut probe);
        });

        // introduce input data
        if index == 0 {
            for round in 0..100 {
                if round % 10 == 0 {
                    input.send(0); // barrier event
                } else {
                    input.send(round); // value event                
                }
                input.advance_to(round + 1);
            }
        }
    }).unwrap();
}
