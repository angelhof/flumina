/*
    Some useful custom operators and aggregators
    that aren't provided out-of-the-box in Timely,
    particularly for experiments where we just
    want to aggregate the entire stream.
*/

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{Exchange, Filter, Map, Operator};
use timely::dataflow::scopes::Scope;
use timely::dataflow::stream::Stream;
use timely::progress::timestamp::Timestamp;

use std::fmt::Debug;
use std::vec::Vec;

/*
    Window over the entire input stream, producing a single
    output at the end.

    This version is parallel: it preserves the partition on the
    input stream and thus produces one output per worker.
*/
pub fn window_all_parallel<D1, D2, D3, I, F, E, T, G>(
    name: &str,
    in_stream: &Stream<G, D1>,
    init: I,
    fold: F,
    emit: E,
) -> Stream<G, D3>
where
    D1: timely::Data + Debug, // input data
    D2: timely::Data + Debug, // accumulator
    D3: timely::Data + Debug, // output data
    I: FnOnce() -> D2 + 'static,
    F: Fn(&mut D2, &T, Vec<D1>) + 'static,
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
                let mut data_vec = Vec::new();
                data.swap(&mut data_vec);
                fold(&mut agg, capability2.time(), data_vec);
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

/*
    Window over the entire input stream, producing a single
    output at the end.

    This version forwards all inputs to a single worker,
    and produces only one output item (for that worker).
*/
pub fn window_all<D1, D2, D3, I, F, E, T, G>(
    name: &str,
    in_stream: &Stream<G, D1>,
    init: I,
    fold: F,
    emit: E,
) -> Stream<G, D3>
where
    D1: timely::Data + Debug + timely::ExchangeData, // input data
    D2: timely::Data + Debug, // accumulator
    D3: timely::Data + Debug, // output data
    I: FnOnce() -> D2 + 'static,
    F: Fn(&mut D2, &T, Vec<D1>) + 'static,
    E: Fn(&D2) -> D3 + 'static,
    T: Timestamp + Copy,
    G: Scope<Timestamp = T>,
{
    let in_stream_single = in_stream.exchange(|_x| 0);
    window_all_parallel(
        name,
        &in_stream_single,
        || (init(), false),
        move |(x, nonempty), time, data| { fold(x, time, data); *nonempty = true; },
        move |(x, nonempty)| { if *nonempty { Some(emit(x)) } else { None } },
    )
    .filter(|x| x.is_some())
    .map(|x| x.unwrap()) // guaranteed not to panic
}
