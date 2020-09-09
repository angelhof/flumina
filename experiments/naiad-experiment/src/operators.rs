/*
    Some useful custom operators and aggregators
    that aren't provided out-of-the-box in Timely,
    particularly for experiments where we just
    want to aggregate the entire stream.
*/

use timely::dataflow::channels::pact::Pipeline;
use timely::communication::message::RefOrMut;
use timely::dataflow::operators::{Operator};
// use timely::dataflow::operators::aggregation::Aggregate;
use timely::dataflow::scopes::Scope;
use timely::dataflow::stream::Stream;
use timely::progress::timestamp::Timestamp;

pub fn window_all_parallel<D1, D2, D3, I, F, E, T, G>(
    name: &str,
    in_stream: &Stream<G, D1>,
    init: I,
    fold: F,
    emit: E,
) -> Stream<G, D3>
where
    D1: timely::Data, // input data
    D2: timely::Data, // accumulator
    D3: timely::Data, // output data
    I: FnOnce() -> D2 + 'static,
    F: Fn(&mut D2, &T, RefOrMut<Vec<D1>>) + 'static,
    E: Fn(&D2) -> D3 + 'static,
    T: Timestamp + Copy,
    G: Scope<Timestamp = T>,
{
    in_stream.unary_frontier(Pipeline, name, |capability1, _info| {

        let mut agg = init();
        let cap_time = *capability1.time();
        let mut maybe_cap = Some(capability1);

        move |input, output| {
            while let Some((capability2, data)) = input.next() {
                fold(&mut agg, capability2.time(), data);
                if *capability2.time() > cap_time {
                    maybe_cap = Some(capability2.retain());
                }
            }
            // Check if entire input is done
            if input.frontier().is_empty() {
                if let Some(cap) = maybe_cap.as_ref() {
                    output.session(&cap).give(emit(&agg));
                    maybe_cap = None;
                }
            }
        }
    })
}

// pub fn window_all<G, D>(
//     stream: &Stream<G, D>,
// 
// )
