/*
    Timely code for the generators (data producers)
    in the Pageview example.
*/

use super::common::{Duration, Scope, Stream};
use super::generators::fixed_rate_source;
use super::pageview_data::{PageName, PageData, PVData, PVItem};
use super::util::{rand_bool, rand_range};

fn pageview_source<F1, F2, G>(
    mut name_gen: F1,
    mut data_gen: F2,
    update_prob: f64,
    scope: &G,
    frequency: Duration,
    total: Duration,
) -> Stream<G, PVItem>
where
    F1: FnMut() -> PageName + 'static,
    F2: FnMut() -> PageData + 'static,
    G: Scope<Timestamp = u128>,
{
    let item_gen = move |_time| {
        let page_name = name_gen();
        if rand_bool(update_prob) {
            let page_data = data_gen();
            PVItem { data: PVData::Update(page_data), name: page_name }
        }
        else {
            PVItem { data: PVData::View, name: page_name }
        }
    };
    fixed_rate_source(item_gen, scope, frequency, total)
}

pub fn pageview_source_two_pages<G>(
    page_0_prob: f64,
    update_prob: f64,
    scope: &G,
    frequency: Duration,
    total: Duration
) -> Stream<G, PVItem>
where
    G: Scope<Timestamp = u128>,
{
    let name_gen = move || { if rand_bool(page_0_prob) { 0 } else { 1 }};
    let data_gen = move || { rand_range(0, 100) };
    pageview_source(name_gen, data_gen, update_prob, scope, frequency, total)
}
