/*
    Operators ("meters") for performance measurement of timely dataflow programs.

    These are not streaming operators; they compute their summaries after input
    is completely finished. They are designed this way for easier use in experiments.
*/

use super::common::{Scope, Stream, SystemTime};
use super::operators::{single_op_binary, window_all, window_all_parallel};
use super::util::{nanos_timestamp};

use timely::dataflow::operators::Inspect;

use std::cmp::max;
use std::fmt::Debug;
use std::vec::Vec;

/*
    Meter which computes latency statistics for an output stream.
*/
pub fn latency_meter<G, D>(
    stream: &Stream<G, D>,
) -> Stream<G, f64>
where
    D: timely::Data + Debug,
    G: Scope<Timestamp = u128>,
{
    let stream = window_all_parallel(
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
        |latencies| latencies.clone(),
    );
    let stream = window_all(
        "Latency Meter Collect",
        &stream,
        || Vec::new(),
        |latencies, _time, data| {
            for latencies_other in data {
                latencies.append(&mut latencies_other.clone());
            }
        },
        |latencies| {
            let sum : u128 = Iterator::sum(latencies.iter());
            (sum as f64) / (1000000.0 * (latencies.len() as f64))
        }
    );
    stream.inspect(|latency| println!("Avg Latency (ms): {:?}", latency))
}

/*
    Meter which computes the total volume on a stream.
*/
pub fn volume_meter<G, D>(
    stream: &Stream<G, D>,
) -> Stream<G, usize>
where
    D: timely::Data + timely::ExchangeData + Debug,
    G: Scope<Timestamp = u128>,
{
    let stream = window_all_parallel(
        "Volume Meter",
        stream,
        || 0,
        |count, _time, data| { *count += data.len(); },
        |count| count.clone(),
    );
    let stream = window_all(
        "Volume Meter Collect",
        &stream,
        || 0,
        |count, _time, data| {
            for count_other in data {
                *count += count_other;
            }
        },
        |count| count.clone(),
    );
    stream.inspect(|count| println!("Volume (events): {:?}", count))
}

/*
    Meter which computes the total completion time
    (max timestamp - starting timestamp) on a stream.
*/
pub fn completion_meter<G, D>(
    stream: &Stream<G, D>,
) -> Stream<G, f64>
where
    D: timely::Data + timely::ExchangeData + Debug,
    G: Scope<Timestamp = u128>,
{
    let start_timestamp = nanos_timestamp(SystemTime::now());
    let stream = window_all_parallel(
        "Completion Meter",
        stream,
        || (),
        |_agg, _time, _data| {},
        |_agg| nanos_timestamp(SystemTime::now())
    );
    let stream = window_all(
        "Completion Meter Collect",
        &stream,
        || 0,
        |max_time, _time, data| {
            for max_time_other in data {
                *max_time = max(*max_time, max_time_other);
            }
        },
        move |max_time| (((*max_time - start_timestamp) as f64) / 1000000.0),
    );
    stream.inspect(|compl_time| println!("Completion Time (ms): {:?}", compl_time))
}

/*
    Meter which computes the throughput on a computation from an input
    stream to an output stream.
*/
pub fn throughput_meter<D1, D2, G>(
    in_stream: &Stream<G, D1>,
    out_stream: &Stream<G, D2>,
) -> Stream<G, f64>
where
    D1: timely::Data + timely::ExchangeData + Debug,
    D2: timely::Data + timely::ExchangeData + Debug,
    G: Scope<Timestamp = u128>,
{
    let volume = volume_meter(in_stream);
    let compl_time = completion_meter(out_stream);
    let throughput = single_op_binary(
        "Throughput Meter Collect",
        &volume, &compl_time,
        |volume, compl_time| { (volume as f64) / compl_time }
    );
    throughput.inspect(|throughput| println!("Throughput (events/ms): {:?}", throughput))
}

/*
    Meter for both latency and throughput.
*/
pub fn latency_throughput_meter<D1, D2, G>(
    in_stream: &Stream<G, D1>,
    out_stream: &Stream<G, D2>,
) -> Stream<G, (f64, f64)>
where
    D1: timely::Data + timely::ExchangeData + Debug,
    D2: timely::Data + timely::ExchangeData + Debug,
    G: Scope<Timestamp = u128>,
{
    let latency = latency_meter(out_stream);
    let throughput = throughput_meter(in_stream, out_stream);
    single_op_binary(
        "Latency-Throughput Meter Collect",
        &latency, &throughput,
        |latency, throughput| { (latency, throughput) }
    )
}
