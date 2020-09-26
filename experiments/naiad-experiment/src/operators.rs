/*
    Some useful custom operators and aggregators
    that aren't provided out-of-the-box in Timely,
    particularly for experiments where we just
    want to aggregate the entire stream.
*/

use super::common::{Pipeline, Scope, Stream, Timestamp};
use super::either::Either;

use timely::dataflow::operators::{Accumulate, Concat, Exchange, Filter,
                                  Inspect, Map, Operator};

use std::fmt::Debug;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::vec::Vec;

/*
    Window over the entire input stream, producing a single
    output at the end.

    This version is parallel: it preserves the partition on the
    input stream and thus produces one output per worker.

    Possible Improvements:
    - It would be nice if 'emit' was an FnOnce. Right now I'm not sure
      how to accomplish that.
    - It would also be nice if the window does not persist at all after emit
      is called; perhaps this can be accomplished by using Option magic
      to set the state to None at the end.
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

/*
    Unary operation on a "singleton" stream, i.e.
    one which has only one element.

    Notes:
    - Panics if called on an input stream which receives more than 2 elements.
    - Waits for an input stream to finish before emitting output, so will hang
      on an input stream which isn't closed even if it only ever gets 1 element.
    - Clones the input once. (This shouldn't be necessary, it's just due to some
      difficulties with ownernship, probably because window_all isn't quite
      implemented in the best way yet.)
*/
pub fn single_op_unary<D1, D2, F, T, G>(
    name: &str,
    in_stream: &Stream<G, D1>,
    op: F,
) -> Stream<G, D2>
where
    D1: timely::Data + Debug + timely::ExchangeData, // input data
    D2: timely::Data + Debug, // output data
    F: Fn(D1) -> D2 + 'static,
    T: Timestamp + Copy,
    G: Scope<Timestamp = T>,
{
    window_all(
        name,
        in_stream,
        || None,
        |seen, _time, data| {
            for d in data {
                assert!(seen.is_none());
                *seen = Some(d);
            }
        },
        move |seen| { op(seen.clone().unwrap()) },
    )
}

/*
    Binary operation on two "singleton" streams, i.e.
    streams which have only one element each.
*/
pub fn single_op_binary<D1, D2, D3, F, T, G>(
    name: &str,
    in_stream1: &Stream<G, D1>,
    in_stream2: &Stream<G, D2>,
    op: F,
) -> Stream<G, D3>
where
    D1: timely::Data + Debug + timely::ExchangeData, // input data 1
    D2: timely::Data + Debug + timely::ExchangeData, // input data 2
    D3: timely::Data + Debug, // output data
    F: Fn(D1, D2) -> D3 + 'static,
    T: Timestamp + Copy,
    G: Scope<Timestamp = T>,
{
    let stream1 = in_stream1.map(Either::Left);
    let stream2 = in_stream2.map(Either::Right);
    let stream = stream1.concat(&stream2);

    window_all(
        name,
        &stream,
        || (None, None),
        |(seen1, seen2), _time, data| {
            for d in data {
                match d {
                    Either::Left(d1) => {
                        assert!(seen1.is_none());
                        *seen1 = Some(d1);
                    },
                    Either::Right(d2) => {
                        assert!(seen2.is_none());
                        *seen2 = Some(d2);
                    }
                }
            }
        },
        move |(seen1, seen2)| {
            op(seen1.clone().unwrap(), seen2.clone().unwrap())
        },
    )
}

/*
    Save a stream to a file in append mode.

    Requires as input a formatting function for what to print.
    Returns the input stream (unchanged as output).
    Panics if file handling fails.
*/
pub fn save_to_file<D, F, T, G>(
    in_stream: &Stream<G, D>,
    filename: &str,
    format: F,
) -> Stream<G, D>
where
    D: timely::Data, // input data
    F: Fn(&D) -> std::string::String + 'static,
    T: Timestamp,
    G: Scope<Timestamp = T>,
{
    let mut file = OpenOptions::new()
        .create(true).append(true)
        .open(filename).unwrap();
    in_stream.inspect(move |d| {
        writeln!(file, "{}", format(d)).unwrap();
    })
}

/*
    Sum the values in each timestamp.
    (Like count, produce a separate value for each worker)
*/
pub trait Sum<G: Scope> {
    fn sum(&self) -> Stream<G, usize>;
}
impl<G: Scope> Sum<G> for Stream<G, usize> {
    fn sum(&self) -> Stream<G, usize>{
        self.accumulate(0, |sum, data| { for &x in data.iter() { *sum += x; } })
    }
}
