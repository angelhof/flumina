/*
    Run the value generator as a standalone binary,
    outputting the stream without performing any computation
*/

use naiad_experiment::vb_generators::value_source;
use naiad_experiment::util;

use timely::dataflow::operators::Inspect;

use std::time::Duration;

fn main() {

    let frequency = Duration::from_micros(
        util::get_input("Frequency in microseconds:")
    );
    let total = Duration::from_secs(
        util::get_input("Total time to run in seconds:")
    );

    timely::execute_from_args(std::env::args(), move |worker| {
        let w_index = worker.index();
        worker.dataflow(|scope| {
            value_source(scope, w_index, frequency, total)
            .inspect(|x| println!("item generated: {:?}", x));
        });
    }).unwrap();
}
