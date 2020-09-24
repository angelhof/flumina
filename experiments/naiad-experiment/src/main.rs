/*
    Command-line entrypoint to run experiments.
*/

use naiad_experiment::util::{current_datetime_str, string_to_static_str};
use naiad_experiment::vb::{VBExperimentData, vb_experiment_main, vb_experiment_gen_only};

use clap::{Arg, App, AppSettings, SubCommand};

const RESULTS_DIR: &str = "results/";
const RESULTS_EXT: &str = ".out";

fn main() {

    /* Create experiment-specific subcommands */

    let command_vb = SubCommand::with_name("vb")
        .about("Value-Barrier experiment")
        .arg(Arg::with_name("PAR")
            .help("parallelism (1 = sequential)")
            .index(1)
            .required(true))
        .arg(Arg::with_name("VAL_RATE")
            .help("rate of values in events/ms")
            .index(2)
            .required(true))
            // .validator(|s| { s.parse::<u64>(); }))
        .arg(Arg::with_name("VALS_PER_HB")
            .help("values per heartbeat (per worker)")
            .index(3)
            .required(true))
            // .validator(|s| { s.parse::<u64>(); }))
        .arg(Arg::with_name("HBS_PER_BAR")
            .help("heartbeats per barrier")
            .index(4)
            .required(true))
            // .validator(|s| { s.parse::<u64>(); }))
        .arg(Arg::with_name("DURATION")
            .help("experiment duration in seconds")
            .index(5)
            .required(true))
            // .validator(|s| { s.parse::<u64>(); }));
        .arg(Arg::with_name("gen-only")
            .short("g")
            .help("data generation only (no processing)"
        ));
    let command_vb_exp1 = SubCommand::with_name("vbe1")
        .about("Value barrier experiment 1: vary the rate");

    /* Create application */

    let app = App::new("naiad-experiment")
        .author("Caleb Stanford")
        .about("Command line for running Naiad (Timely Dataflow) experiments")
        .setting(AppSettings::SubcommandRequired)
        .subcommand(command_vb)
        .subcommand(command_vb_exp1);

    /* Parse arguments depending on subcommand */

    let matches = app.clone().get_matches();

    if let Some(matches) = matches.subcommand_matches("vb") {
        // Value Barrier Command

        let arg1 = matches.value_of("PAR").unwrap();
        let arg2 = matches.value_of("VAL_RATE").unwrap();
        let arg3 = matches.value_of("VALS_PER_HB").unwrap();
        let arg4 = matches.value_of("HBS_PER_BAR").unwrap();
        let arg5 = matches.value_of("DURATION").unwrap();
        let params = VBExperimentData {
            parallelism: arg1.parse::<u64>().expect("expected u64"),
            val_rate_per_milli: arg2.parse::<u64>().expect("expected u64 (events/ms)"),
            vals_per_hb_per_worker: arg3.parse::<f64>().expect("expected f64"),
            hbs_per_bar: arg4.parse::<u64>().expect("expected u64"),
            exp_duration_secs: arg5.parse::<u64>().expect("expected u64 (secs)"),
        };

        if matches.is_present("-g") {
            let results_path = string_to_static_str(
                RESULTS_DIR.to_owned()
                + &current_datetime_str()
                + "_vbgen_" + arg1 + "_" + arg2 + "_" + arg3 + "_" + arg4 + "_" + arg5
                + RESULTS_EXT
            );
            vb_experiment_gen_only(params, results_path);
        }
        else {
            let results_path = string_to_static_str(
                RESULTS_DIR.to_owned()
                + &current_datetime_str()
                + "_vb_" + arg1 + "_" + arg2 + "_" + arg3 + "_" + arg4 + "_" + arg5
                + RESULTS_EXT
            );
            vb_experiment_main(params, results_path);
        }
    }
    else if let Some(_matches) = matches.subcommand_matches("vbe1") {
        let mut params = VBExperimentData {
            parallelism: 0, // will be set
            val_rate_per_milli: 0, // will be set
            vals_per_hb_per_worker: 100.0,
            hbs_per_bar: 100,
            exp_duration_secs: 5,
        };
        let val_rates = &[
            1, 5, 10, 50, 100,
            150, 200, 250, 300, 350, 400,
        ];
        let parallelisms = &[1, 2, 4, 8];
        for &par in parallelisms {
            println!("===== Parallelism: {} =====", par);
            params.parallelism = par;
            let results_path = string_to_static_str(
                RESULTS_DIR.to_owned()
                + &current_datetime_str() + "_vbe1_par" + &par.to_string()
                + RESULTS_EXT
            );
            for &val_rate in val_rates {
                println!("=== Value rate (events/ms): {} ===", val_rate);
                params.val_rate_per_milli = val_rate;
                vb_experiment_main(params, results_path);
            }
        }
    }
    else {
        unreachable!();
    }
}
