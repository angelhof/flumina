/*
    This file tests running a Timely computation with
    a lot of data at once, to try to crash the system.
*/

extern crate timely;

use timely::dataflow::operators::generic::operator::source;
use timely::dataflow::operators::*;
use timely::dataflow::scopes::Scope;
use timely::dataflow::stream::Stream;

fn flood_source<G>(
    scope: &G,
    batch_size: usize,
    num_batches: usize,
) -> Stream<G, usize>
where
    G: Scope<Timestamp = usize>,
{
    source(scope, "Source", |capability, info| {
        let activator = scope.activator_for(&info.address[..]);
        let mut maybe_cap = Some(capability);

        let mut batch = 0;
        move |output| {
            if let Some(cap) = maybe_cap.as_mut() {
                if batch < num_batches {
                    // Output one batch of values
                    println!("Sending batch {} ({} items)", batch, batch_size);
                    batch += 1;
                    cap.downgrade(&batch);
                    for item in 0..batch_size {
                        output.session(&cap).give(item)
                    }
                    activator.activate();
                } else {
                    // Release capability
                    maybe_cap = None;
                }
            }
        }
    })
}

fn main() {
    let mut args: Vec<String> = std::env::args().collect();
    let batch_size = args[1].parse().unwrap();
    let num_batches = args[2].parse().unwrap();
    timely::execute_from_args(args.drain(..), move |worker| {
        let w_index = worker.index();
        let w_total = worker.peers();
        println!("worker {}/{} start", w_index, w_total);
        worker.dataflow::<_, _, _>(|scope| {
            flood_source(scope, batch_size, num_batches)
                .exchange(|_| 0)
                .inspect(move |x| {
                    println!("worker {}/{} seen: {:?}", w_index, w_total, x);
                });
        });
        println!("worker {}/{} complete", w_index, w_total);
    })
    .unwrap_or_else(|err| panic!("Runtime error: {}", err));
}
