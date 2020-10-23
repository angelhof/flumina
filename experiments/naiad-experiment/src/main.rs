/*
    Command-line entrypoint to run experiments.
*/

use naiad_experiment::experiment::{
    LatencyThroughputExperiment, TimelyParallelism,
};
use naiad_experiment::pageview::{
    PVExperiment, PVExperimentParams, PVGenExperiment,
};
use naiad_experiment::vb::{
    FDExperiment, VBExperiment, VBExperimentParams, VBGenExperiment,
};

use structopt::StructOpt;

/* Use StructOpt to create CLI subcommands and arguments */

#[derive(StructOpt)]
#[structopt(
    author = "Caleb Stanford",
    no_version,
    about = "Command line for running Timely experiments."
)]
enum TimelyExperiments {
    #[structopt(about = "Value-Barrier Example")]
    VB {
        #[structopt(flatten)]
        pms: VBExperimentParams,
        #[structopt(flatten)]
        plsm: TimelyParallelism,
    },
    #[structopt(about = "Value-Barrier Data Generation Only")]
    VBG {
        #[structopt(flatten)]
        pms: VBExperimentParams,
        #[structopt(flatten)]
        plsm: TimelyParallelism,
    },
    #[structopt(about = "Fraud-Detection Example")]
    FD {
        #[structopt(flatten)]
        pms: VBExperimentParams,
        #[structopt(flatten)]
        plsm: TimelyParallelism,
    },
    #[structopt(about = "Page-View Example")]
    PV {
        #[structopt(flatten)]
        pms: PVExperimentParams,
        #[structopt(flatten)]
        plsm: TimelyParallelism,
    },
    #[structopt(about = "Page-View Data Generation Only")]
    PVG {
        #[structopt(flatten)]
        pms: PVExperimentParams,
        #[structopt(flatten)]
        plsm: TimelyParallelism,
    },
    #[structopt(about = "Pre-Defined Experiment 1: Value-Barrier")]
    Exp1,
    #[structopt(about = "Pre-Defined Experiment 2: Fraud Detection")]
    Exp2,
    #[structopt(about = "Pre-Defined Experiment 3: Page-view")]
    Exp3,
}
impl TimelyExperiments {
    fn run(&mut self) {
        match self {
            Self::VB { pms, plsm } => VBExperiment.run_single(*pms, *plsm),
            Self::VBG { pms, plsm } => VBGenExperiment.run_single(*pms, *plsm),
            Self::FD { pms, plsm } => FDExperiment.run_single(*pms, *plsm),
            Self::PV { pms, plsm } => PVExperiment.run_single(*pms, *plsm),
            Self::PVG { pms, plsm } => PVGenExperiment.run_single(*pms, *plsm),
            Self::Exp1 => {
                /* Experiment 1: Value-Barrier */
                let params = VBExperimentParams {
                    val_rate_per_milli: 0, // will be set
                    vals_per_hb_per_worker: 100,
                    hbs_per_bar: 100,
                    exp_duration_secs: 5,
                };
                let rates = &[
                    50, 100, 150, 200, 250, 300, 350, 400, 450, 500, 550, 600,
                ];
                let par_workers = &[1, 2];
                let par_nodes = &[1, 2, 4]; // , 8, 12, 16, 20];
                VBExperiment.run_all(params, rates, par_workers, par_nodes);
            }
            Self::Exp2 => {
                /*  Experiment 2: Fraud Detection */
                let params = VBExperimentParams {
                    val_rate_per_milli: 0, // will be set
                    vals_per_hb_per_worker: 100,
                    hbs_per_bar: 100,
                    exp_duration_secs: 5,
                };
                let rates = &[
                    50, 100, 150, 200, 250, 300, 350, 400, 450, 500, 550, 600,
                ];
                let par_workers = &[1, 2];
                let par_nodes = &[1, 2, 4]; // , 8, 12, 16, 20];
                FDExperiment.run_all(params, rates, par_workers, par_nodes);
            }
            Self::Exp3 => {
                /* Experiment 3: Page-View */
                let params = PVExperimentParams {
                    views_per_milli: 0, // will be set
                    views_per_update: 10000,
                    exp_duration_secs: 5,
                };
                let rates = &[
                    50, 100, 150, 200, 250, 300, 350, 400, 450, 500, 550, 600,
                ];
                let par_workers = &[1, 2];
                let par_nodes = &[1, 2, 4]; // , 8, 12, 16, 20];
                PVExperiment.run_all(params, rates, par_workers, par_nodes);
            }
        }
    }
}

/* Entrypoint */

fn main() {
    TimelyExperiments::from_args().run();
}
