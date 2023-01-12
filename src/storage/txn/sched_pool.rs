// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cell::RefCell,
    mem,
    sync::{Arc, Mutex},
};

use collections::HashMap;
use file_system::{set_io_type, IoType};
use kvproto::{kvrpcpb::CommandPri, pdpb::QueryKind};
use pd_client::{Feature, FeatureGate};
use prometheus::local::*;
use raftstore::store::WriteStats;
use resource_control::{ControlledFuture, ResourceController};
use tikv_util::{
    sys::SysQuota,
    yatp_pool::{Full, FuturePool, PoolTicker, YatpPoolBuilder},
};
use yatp::queue::Extras;

use crate::storage::{
    kv::{destroy_tls_engine, set_tls_engine, Engine, FlowStatsReporter, Statistics},
    metrics::*,
    test_util::latest_feature_gate,
};

pub struct SchedLocalMetrics {
    local_scan_details: HashMap<&'static str, Statistics>,
    command_keyread_histogram_vec: LocalHistogramVec,
    local_write_stats: WriteStats,
}

thread_local! {
    static TLS_SCHED_METRICS: RefCell<SchedLocalMetrics> = RefCell::new(
        SchedLocalMetrics {
            local_scan_details: HashMap::default(),
            command_keyread_histogram_vec: KV_COMMAND_KEYREAD_HISTOGRAM_VEC.local(),
            local_write_stats:WriteStats::default(),
        }
    );

    static TLS_FEATURE_GATE: RefCell<FeatureGate> = RefCell::new(latest_feature_gate());
}

#[derive(Clone)]
pub struct SchedTicker<R: FlowStatsReporter> {
    reporter: R,
}

impl<R: FlowStatsReporter> PoolTicker for SchedTicker<R> {
    fn on_tick(&mut self) {
        tls_flush(&self.reporter);
    }
}

#[derive(Clone)]
pub struct SchedPool {
    pub pool: FuturePool,
    resource_ctl: Arc<ResourceController>,
}

impl SchedPool {
    pub fn new<E: Engine, R: FlowStatsReporter>(
        engine: E,
        pool_size: usize,
        reporter: R,
        feature_gate: FeatureGate,
        name_prefix: &str,
        resource_ctl: Arc<ResourceController>,
    ) -> Self {
        let engine = Arc::new(Mutex::new(engine));
        // for low cpu quota env, set the max-thread-count as 4 to allow potential cases
        // that we need more thread than cpu num.
        let max_pool_size = std::cmp::max(
            pool_size,
            std::cmp::max(4, SysQuota::cpu_cores_quota() as usize),
        );
        let pool = YatpPoolBuilder::new(SchedTicker {reporter:reporter.clone()})
            .thread_count(1, pool_size, max_pool_size)
            .name_prefix(name_prefix)
            // Safety: by setting `after_start` and `before_stop`, `FuturePool` ensures
            // the tls_engine invariants.
            .after_start(move || {
                set_tls_engine(engine.lock().unwrap().clone());
                set_io_type(IoType::ForegroundWrite);
                TLS_FEATURE_GATE.with(|c| *c.borrow_mut() = feature_gate.clone());
            })
            .before_stop(move || unsafe {
                // Safety: we ensure the `set_` and `destroy_` calls use the same engine type.
                destroy_tls_engine::<E>();
                tls_flush(&reporter);
            })
            .build_priority_future_pool();
        SchedPool { pool, resource_ctl }
    }

    pub fn spawn(
        &self,
        group_name: &str,
        pri: CommandPri,
        f: impl futures::Future<Output = ()> + Send + 'static,
    ) -> Result<(), Full> {
        let mut extras = Extras::single_level();
        let priority = self.resource_ctl.get_priority(group_name, pri);
        extras.set_priority(priority);
        self.pool.spawn_with_extras(
            ControlledFuture::new(
                async move {
                    f.await;
                },
                self.resource_ctl.clone(),
                group_name.to_owned(),
                pri,
            ),
            extras,
        )
    }
}

pub fn tls_collect_scan_details(cmd: &'static str, stats: &Statistics) {
    TLS_SCHED_METRICS.with(|m| {
        m.borrow_mut()
            .local_scan_details
            .entry(cmd)
            .or_insert_with(Default::default)
            .add(stats);
    });
}

pub fn tls_flush<R: FlowStatsReporter>(reporter: &R) {
    TLS_SCHED_METRICS.with(|m| {
        let mut m = m.borrow_mut();
        for (cmd, stat) in m.local_scan_details.drain() {
            for (cf, cf_details) in stat.details().iter() {
                for (tag, count) in cf_details.iter() {
                    KV_COMMAND_SCAN_DETAILS
                        .with_label_values(&[cmd, *cf, *tag])
                        .inc_by(*count as u64);
                }
            }
        }
        m.command_keyread_histogram_vec.flush();

        // Report PD metrics
        if !m.local_write_stats.is_empty() {
            let mut write_stats = WriteStats::default();
            mem::swap(&mut write_stats, &mut m.local_write_stats);
            reporter.report_write_stats(write_stats);
        }
    });
}

pub fn tls_collect_query(region_id: u64, kind: QueryKind) {
    TLS_SCHED_METRICS.with(|m| {
        let mut m = m.borrow_mut();
        m.local_write_stats.add_query_num(region_id, kind);
    });
}

pub fn tls_collect_keyread_histogram_vec(cmd: &str, count: f64) {
    TLS_SCHED_METRICS.with(|m| {
        m.borrow_mut()
            .command_keyread_histogram_vec
            .with_label_values(&[cmd])
            .observe(count);
    });
}

pub fn tls_can_enable(feature: Feature) -> bool {
    TLS_FEATURE_GATE.with(|feature_gate| feature_gate.borrow().can_enable(feature))
}

#[cfg(test)]
pub fn set_tls_feature_gate(feature_gate: FeatureGate) {
    TLS_FEATURE_GATE.with(|f| *f.borrow_mut() = feature_gate);
}
