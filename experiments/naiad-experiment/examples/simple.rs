extern crate timely;

use timely::dataflow::operators::*;

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        let w_index = worker.index();
        let w_total = worker.peers();
        worker.dataflow::<(),_,_>(|scope| {
            (0..10).to_stream(scope).inspect(move |x|
                println!("worker {}/{} seen: {:?}", w_index, w_total, x)
            );
        })
    }).unwrap();
}
