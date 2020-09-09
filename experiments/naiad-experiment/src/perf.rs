/*
    Operators ("meters") for performance measurement of timely dataflow programs.

    These are not streaming operators; they compute their summaries after input
    is completely finished. They are designed this way for easier use in experiments.
*/

use super::operators::window_all_parallel;
use super::util::{nanos_timestamp};

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{Capability, Map, Inspect, Operator};
use timely::dataflow::operators::aggregation::Aggregate;
use timely::dataflow::operators::generic::{OperatorInfo};
use timely::dataflow::scopes::Scope;
use timely::dataflow::stream::Stream;

use std::cmp::max;
use std::time::SystemTime;
use std::vec::Vec;

/*
    Meter which computes latency statistics for an output stream.
*/
pub fn latency_meter<G, D>(
    stream: &Stream<G, D>,
    _output_filename: &'static str,
) -> ()
where
    D: timely::Data,
    G: Scope<Timestamp = u128>,
{
    window_all_parallel(
        "Latency Meter",
        stream,
        || Vec::new(),
        |latencies, time, data| {
            let num_inputs = data.len();
            let timestamp_now = nanos_timestamp(SystemTime::now());
            let latency = timestamp_now - time;
            for _i in 0..num_inputs {
                latencies.push(latency);
            }
        },
        |latencies| {
            println!("Latencies: {:?}", latencies);
        },
    );
}

/*
    Meter which computes the total volume on a stream.
*/
pub fn volume_meter<G, D>(
    stream: &Stream<G, D>,
) -> ()
where
    D: timely::Data + timely::ExchangeData,
    G: Scope<Timestamp = u128>,
{
    stream.unary_frontier(Pipeline, "Volume Meter",
                          |capability: Capability<u128>, _info: OperatorInfo| {

        let mut count = 0;
        let cap_time = *capability.time();
        let mut maybe_cap = Some(capability);

        move |input, output| {
            while let Some((capability, data)) = input.next() {
                count += data.len();
                if *capability.time() > cap_time {
                    maybe_cap = Some(capability.retain());
                }
            }
            // Check if entire input is done
            if input.frontier().is_empty() {
                let cap = maybe_cap.as_ref().unwrap();
                output.session(cap).give(count);
                maybe_cap = None;
            }
        }
    })
    .map(|x| (0, x))
    .aggregate(
        |_key, val, agg| { *agg += val; },
        |_key, agg: usize| agg,
        |_key| 0,
    )
    .inspect(|x| println!("Total Volume: {}", x));
}

/*
    Meter which computes the max timestamp on a stream.
*/
pub fn completion_meter<G, D>(
    stream: &Stream<G, D>,
) -> ()
where
    D: timely::Data + timely::ExchangeData,
    G: Scope<Timestamp = u128>,
{
    stream.unary_frontier(Pipeline, "Completion Meter",
                          |capability: Capability<u128>, _info: OperatorInfo| {

        let cap_time = *capability.time();
        let mut maybe_cap = Some(capability);

        move |input, output| {
            while let Some((capability, _data)) = input.next() {
                if *capability.time() > cap_time {
                    maybe_cap = Some(capability.retain());
                }
            }
            // Check if entire input is done
            if input.frontier().is_empty() {
                let cap = maybe_cap.as_ref().unwrap();
                output.session(&cap).give(*cap.time());
                maybe_cap = None;
            }
        }
    })
    .map(|x| (0, x))
    .aggregate(
        |_key, val, agg| { *agg += max(*agg, val); },
        |_key, agg| agg,
        |_key| 0,
    )
    .inspect(|x| println!("Completion: {}", x));
}

/*
    Meter which computes the throughput on a computation from an input
    stream to an output stream.
*/
pub fn throughput_meter<G1, D1, G2, D2>(
    in_stream: &Stream<G1, D1>,
    out_stream: &Stream<G2, D2>,
    _output_filename: &str,
) -> ()
where
    D1: timely::Data + timely::ExchangeData,
    G1: Scope<Timestamp = u128>,
    D2: timely::Data + timely::ExchangeData,
    G2: Scope<Timestamp = u128>,
{
    volume_meter(in_stream);
    completion_meter(out_stream);
    // in_stream.binary_frontier(&out_stream, Pipeline, Pipeline)
}
