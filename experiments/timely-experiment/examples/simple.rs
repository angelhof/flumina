extern crate timely;

use timely::dataflow::operators::*;

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        let w_index = worker.index();
        let w_total = worker.peers();
        println!("worker {}/{} start", w_index, w_total);
        worker.dataflow::<(), _, _>(|scope| {
            (0..10).to_stream(scope).inspect(move |x| {
                println!("worker {}/{} seen: {:?}", w_index, w_total, x)
            });
        });
        println!("worker {}/{} complete", w_index, w_total);
    })
    .unwrap();
}
