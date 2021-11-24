/*
    Input parameters, then run the value barrier experiment
    data generation only.
*/

use timely_experiment::util;
use timely_experiment::vb::vb_experiment_gen_only;

use std::time::Duration;

fn main() {
    let value_frequency = Duration::from_micros(util::get_input(
        "Value frequency in microseconds:",
    ));
    let barrier_frequency = Duration::from_millis(util::get_input(
        "Barrier frequency in milliseconds:",
    ));
    let experiment_duration =
        Duration::from_secs(util::get_input("Total time to run in seconds:"));

    vb_experiment_gen_only(
        value_frequency,
        barrier_frequency,
        experiment_duration,
    );
}
