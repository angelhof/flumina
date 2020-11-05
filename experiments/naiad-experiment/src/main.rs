/*
    Command-line entrypoint to run experiments.
*/

use naiad_experiment::experiment::{
    LatencyThroughputExperiment, TimelyNodeInfo, TimelyParallelism,
};
use naiad_experiment::pageview::{
    PVBadExperiment, PVExperimentParams, PVGenExperiment, PVGoodExperiment,
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
    VBGen {
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
    #[structopt(about = "Page-View Example, Good Version (Scales)")]
    PVGood {
        #[structopt(flatten)]
        pms: PVExperimentParams,
        #[structopt(flatten)]
        plsm: TimelyParallelism,
    },
    #[structopt(about = "Page-View Example, Bad Version (Doesn't Scale)")]
    PVBad {
        #[structopt(flatten)]
        pms: PVExperimentParams,
        #[structopt(flatten)]
        plsm: TimelyParallelism,
    },
    #[structopt(about = "Page-View Data Generation Only")]
    PVGen {
        #[structopt(flatten)]
        pms: PVExperimentParams,
        #[structopt(flatten)]
        plsm: TimelyParallelism,
    },
    #[structopt(about = "Pre-Defined Experiment 1: Value-Barrier")]
    Exp1 {
        #[structopt(default_value = "e")]
        node_info: TimelyNodeInfo,
    },
    #[structopt(about = "Pre-Defined Experiment 2: Fraud Detection")]
    Exp2 {
        #[structopt(default_value = "e")]
        node_info: TimelyNodeInfo,
    },
    #[structopt(about = "Pre-Defined Experiment 3: Page-View Good")]
    Exp3 {
        #[structopt(default_value = "e")]
        node_info: TimelyNodeInfo,
    },
    #[structopt(about = "Pre-Defined Experiment 4: Page-View Bad (part a)")]
    Exp4a {
        #[structopt(default_value = "e")]
        node_info: TimelyNodeInfo,
    },
    #[structopt(about = "Pre-Defined Experiment 4: Page-View Bad (part b)")]
    Exp4b {
        #[structopt(default_value = "e")]
        node_info: TimelyNodeInfo,
    },
}
impl TimelyExperiments {
    fn run(&mut self) {
        match self {
            Self::VB { pms, plsm } => VBExperiment.run_single(*pms, *plsm),
            Self::VBGen { pms, plsm } => {
                VBGenExperiment.run_single(*pms, *plsm)
            }
            Self::FD { pms, plsm } => FDExperiment.run_single(*pms, *plsm),
            Self::PVGood { pms, plsm } => {
                PVGoodExperiment.run_single(*pms, *plsm)
            }
            Self::PVBad { pms, plsm } => {
                PVBadExperiment.run_single(*pms, *plsm)
            }
            Self::PVGen { pms, plsm } => {
                PVGenExperiment.run_single(*pms, *plsm)
            }
            Self::Exp1 { node_info } => {
                /* Experiment 1: Value-Barrier */
                let params = VBExperimentParams {
                    val_rate_per_milli: 0, // will be set
                    vals_per_hb_per_worker: 100,
                    hbs_per_bar: 100,
                    exp_duration_secs: 5,
                };
                let rates = &[
                    200, 400, 600, 800, 1000, 1200, 1400, 1600, 1800, 2000,
                    2200, 2400,
                ];
                let par_workers = &[1];
                let par_nodes = &[1, 2, 4, 8, 12, 16, 20];
                VBExperiment.run_all(
                    *node_info,
                    params,
                    rates,
                    par_workers,
                    par_nodes,
                );
            }
            Self::Exp2 { node_info } => {
                /*  Experiment 2: Fraud Detection */
                let params = VBExperimentParams {
                    val_rate_per_milli: 0, // will be set
                    vals_per_hb_per_worker: 100,
                    hbs_per_bar: 100,
                    exp_duration_secs: 5,
                };
                let rates = &[
                    200, 400, 600, 800, 1000, 1200, 1400, 1600, 1800, 2000,
                    2200, 2400,
                ];
                let par_workers = &[1];
                let par_nodes = &[1, 2, 4, 8, 12, 16, 20];
                FDExperiment.run_all(
                    *node_info,
                    params,
                    rates,
                    par_workers,
                    par_nodes,
                );
            }
            Self::Exp3 { node_info } => {
                /* Experiment 3: Page-View Good */
                let params = PVExperimentParams {
                    views_per_milli: 0, // will be set
                    views_per_update: 10000,
                    exp_duration_secs: 5,
                };
                let rates = &[
                    200, 400, 600, 800, 1000, 1200, 1400, 1600, 1800, 2000,
                    2200, 2400,
                ];
                let par_workers = &[1];
                let par_nodes = &[1, 2, 4, 8, 12, 16, 20];
                PVGoodExperiment.run_all(
                    *node_info,
                    params,
                    rates,
                    par_workers,
                    par_nodes,
                );
            }
            Self::Exp4a { node_info } => {
                /* Experiment 4: Page-View Bad (part a) */
                let params = PVExperimentParams {
                    views_per_milli: 0, // will be set
                    views_per_update: 10000,
                    exp_duration_secs: 5,
                };
                let rates = &[
                    200, 400, 600, 800, 1000, 1200, 1400, 1600, 1800, 2000,
                    2200, 2400,
                ];
                let par_workers = &[1];
                let par_nodes = &[1, 2, 4];
                PVBadExperiment.run_all(
                    *node_info,
                    params,
                    rates,
                    par_workers,
                    par_nodes,
                );
            }
            Self::Exp4b { node_info } => {
                /* Experiment 4: Page-View Bad (part b) */
                let params = PVExperimentParams {
                    views_per_milli: 0, // will be set
                    views_per_update: 10000,
                    exp_duration_secs: 5,
                };
                let rates = &[
                    20, 40, 60, 80, 100, 120, 140, 160, 180, 200, 220, 240,
                ];
                let par_workers = &[1];
                let par_nodes = &[4, 8, 12, 16, 20];
                PVBadExperiment.run_all(
                    *node_info,
                    params,
                    rates,
                    par_workers,
                    par_nodes,
                );
            }
        }
    }
}

/* Entrypoint */

fn main() {
    TimelyExperiments::from_args().run();
}
