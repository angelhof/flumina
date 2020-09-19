/*
    Useful generator patterns for source data
*/

use super::common::{Duration, Scope, Stream, SystemTime};
use super::util::{div_durations, time_since, nanos_timestamp};

use timely::dataflow::operators::generic::operator::source;

/*
    Data source which produces a number of output items
    over time given by an arbitrary function giving the cumulative
    total to produce, and stopping after the uptime is complete.
*/
fn variable_rate_source<D, F, G, H>(
    item_gen: F,
    scope: &G,
    cumulative_total_fun: H,
    uptime: Duration,
) -> Stream<G, D>
where
    D: timely::Data + timely::ExchangeData,
    F: Fn(u128) -> D + 'static, // Input: timestamp in nanoseconds
    G: Scope<Timestamp = u128>,
    H: Fn(Duration) -> u128 + 'static, // Input: time sicne source start time
{
    source(scope, "Source", |capability, info| {

        // Internal details of timely dataflow
        // 1. Acquire a re-activator for this operator.
        // 2. Wrap capability in an option so that we can release it when
        //    done by setting it to None
        let activator = scope.activator_for(&info.address[..]);
        let mut maybe_cap = Some(capability);

        // Initialize with current time; keep track of # vals sent
        let start_time = SystemTime::now();
        let mut vals_sent = 0;
        let vals_max = cumulative_total_fun(uptime);

        // Return closure
        move |output| {
            if let Some(cap) = maybe_cap.as_mut() {

                // Decide how behind we are on outputting values
                let elapsed = time_since(start_time);
                let vals_to_send = cumulative_total_fun(elapsed);

                // Output values to catch up
                while vals_sent < vals_to_send &&
                      vals_sent < vals_max {
                    let time_nanos = nanos_timestamp(SystemTime::now());
                    let item = item_gen(time_nanos);
                    cap.downgrade(&time_nanos);
                    output.session(&cap).give(item);
                    vals_sent += 1;
                }
                if vals_sent == vals_max {
                    maybe_cap = None;
                }
                else {
                    activator.activate();
                }
            }
        }
    })
}

/*
    Data source which produces output items at a constant rate,
    stopping after the uptime is complete
*/
pub fn fixed_rate_source<D, F, G>(
    item_gen: F,
    scope: &G,
    frequency: Duration,
    uptime: Duration,
) -> Stream<G, D>
where
    D: timely::Data + timely::ExchangeData,
    F: Fn(u128) -> D + 'static,
    G: Scope<Timestamp = u128>,
{
    variable_rate_source(
        item_gen, scope,
        move |elapsed| div_durations(elapsed, frequency),
        uptime,
    )
}

/*
    Data source which produces output items at a linearly increasing rate,
    stopping after the uptime is complete
*/
pub fn linear_rate_source<D, F, G>(
    item_gen: F,
    scope: &G,
    frequency_init: Duration,
    acceleration: f64, // output events / microsecond^2
    uptime: Duration,
) -> Stream<G, D>
where
    D: timely::Data + timely::ExchangeData,
    F: Fn(u128) -> D + 'static,
    G: Scope<Timestamp = u128>,
{
    variable_rate_source(
        item_gen, scope,
        move |elapsed| {
            // Integral from 0 to T of (1 / f + a * t)
            //     = T / f + (a / 2) * T^2
            let micros_elapsed = elapsed.as_micros() as f64;
            div_durations(elapsed, frequency_init)
            + (((acceleration / 2.0) * micros_elapsed * micros_elapsed) as u128)
        },
        uptime,
    )
}
