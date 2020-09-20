/*
    Command-line entrypoint to run experiments.
*/

use naiad_experiment::util::{current_datetime_str,string_to_static_str};
use naiad_experiment::vb::{vb_experiment_main,vb_experiment_gen_only};

use clap::{Arg, App, AppSettings, SubCommand};

use std::vec::Vec;
use std::time::Duration;

const RESULTS_DIR: &str = "results/";
const RESULTS_EXT: &str = ".out";

fn main() {
    let command_vb = SubCommand::with_name("vb")
        .about("Value-Barrier experiment")
        .arg(Arg::with_name("VAL_FREQ")
            .help("frequency of values in microseconds")
            .index(1)
            .required(true))
            // .validator(|s| { s.parse::<u64>(); }))
        .arg(Arg::with_name("VALS_PER_HB")
            .help("values per heartbeat (per worker)")
            .index(2)
            .required(true))
            // .validator(|s| { s.parse::<u64>(); }))
        .arg(Arg::with_name("HBS_PER_BAR")
            .help("heartbeats per barrier")
            .index(3)
            .required(true))
            // .validator(|s| { s.parse::<u64>(); }))
        .arg(Arg::with_name("DURATION")
            .help("experiment duration in seconds")
            .index(4)
            .required(true))
            // .validator(|s| { s.parse::<u64>(); }));
        .arg(Arg::with_name("gen-only")
            .short("g")
            .help("data generation only (no processing)"
        ));
    let command_vb_exp1 = SubCommand::with_name("vbe1")
        .about("Value barrier experiment 1: vary the rate");

    let app = App::new("naiad-experiment")
        .author("Caleb Stanford")
        .about("Command line for running Naiad (Timely Dataflow) experiments")
        .setting(AppSettings::SubcommandRequired)
        .arg(Arg::with_name("workers")
            .short("w")
            .help("number of workers (is passed to Timely)")
            .takes_value(true)
            .number_of_values(1))
        .subcommand(command_vb)
        .subcommand(command_vb_exp1);

    let matches = app.clone().get_matches();

    if let Some(matches) = matches.subcommand_matches("vb") {
        // Value Barrier Command

        // Extract args to pass to Timely
        let mut timely_args : Vec<String> = Vec::new();
        if let Some(n_workers) = matches.value_of("workers") {
            timely_args.push("-w".to_string());
            timely_args.push(n_workers.to_string());
        }

        let arg1 = matches.value_of("VAL_FREQ").unwrap();
        let arg2 = matches.value_of("VALS_PER_HB").unwrap();
        let arg3 = matches.value_of("HBS_PER_BAR").unwrap();
        let arg4 = matches.value_of("DURATION").unwrap();
        let val_frequency = Duration::from_micros(
            arg1.parse::<u64>().expect("expected u64 (microseconds)")
        );
        let vals_per_hb_per_worker = arg2.parse::<f64>().expect("expected f64");
        let hbs_per_bar = arg3.parse::<u64>().expect("expected u64");
        let exp_duration = Duration::from_micros(
            arg4.parse::<u64>().expect("expected u64 (microseconds)")
        );

        if matches.is_present("-g") {
            let results_path = string_to_static_str(
                RESULTS_DIR.to_owned()
                + &current_datetime_str()
                + "_vbgen_" + arg1 + "_" + arg2 + "_" + arg3 + "_" + arg4
                + RESULTS_EXT
            );
            vb_experiment_gen_only(
                val_frequency, vals_per_hb_per_worker, hbs_per_bar, exp_duration,
                timely_args.drain(0..),
                results_path,
            );
        }
        else {
            let results_path = string_to_static_str(
                RESULTS_DIR.to_owned()
                + &current_datetime_str()
                + "_vb_" + arg1 + "_" + arg2 + "_" + arg3 + "_" + arg4
                + RESULTS_EXT
            );
            vb_experiment_main(
                val_frequency, vals_per_hb_per_worker, hbs_per_bar, exp_duration,
                timely_args.drain(0..),
                results_path,
            );
        }
    }
    else if let Some(_matches) = matches.subcommand_matches("vbe1") {
        let val_freqs = &[
            200, 190, 180, 170, 160, 150, 140, 130, 120, 110,
            100, 90, 80, 70, 60, 50, 40, 30, 20, 10
        ];
        let parallelism = &[1, 2, 3, 4];
        let vals_per_hb_per_worker = 100.0;
        let hbs_per_bar = 100;
        let exp_duration = Duration::from_secs(5);
        for par in parallelism {
            println!("===== Parallelism: {} =====", par);
            let mut timely_args : Vec<String> = Vec::new();
            timely_args.push("-w".to_string());
            timely_args.push(par.to_string());
            let results_path = string_to_static_str(
                RESULTS_DIR.to_owned()
                + &current_datetime_str() + "_vbe1_" + &par.to_string()
                + RESULTS_EXT
            );
            for val_freq in val_freqs {
                println!("=== Value frequency (ms): {} ===", val_freq);
                let val_frequency = Duration::from_micros(*val_freq);
                vb_experiment_main(
                    val_frequency, vals_per_hb_per_worker, hbs_per_bar, exp_duration,
                    timely_args.drain(0..),
                    results_path,
                );
            }
        }
    }
    else {
        unreachable!();
    }
}
