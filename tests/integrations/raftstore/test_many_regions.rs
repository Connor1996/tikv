// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use memory_stats::memory_stats;

use std::{cmp::max, time::Duration};

use server::memory::MemoryTraceManager;
use raftstore::store::memory::MEMTRACE_ROOT as MEMTRACE_RAFTSTORE;
use engine_traits::{RaftEngine, WriteBatch, WriteBatchExt};
use kvproto::{metapb, raft_serverpb::PeerState};
use raftstore::store::{
    write_initial_apply_state, write_initial_raft_state, write_peer_state, INIT_EPOCH_CONF_VER,
    INIT_EPOCH_VER,
};
use test_raftstore::*;
use tikv_alloc::{activate_prof, deactivate_prof, dump_prof};
use tikv_util::{
    config::GIB,
    sys::{cpu_time::ProcessStat, get_global_memory_usage, record_global_memory_usage},
    time::Instant,
};
use prometheus::core::Collector;
use tikv::server::MEM_TRACE_SUM_GAUGE;

/// Create a new cluster with specified number of nodes and regions.
fn new_cluster_with_many_regions(node_count: usize, region_count: u64) -> Cluster<NodeCluster> {
    let mut cluster = new_node_cluster(1, node_count);
    cluster.run();

    for i in cluster.get_node_ids() {
        cluster.stop_node(i);
    }

    let epoch_ver = INIT_EPOCH_VER + 1;
    let epoch_conf_ver = INIT_EPOCH_CONF_VER + 1;

    for engines in cluster.engines.values() {
        let mut kv_wb = engines.kv.write_batch();
        let mut raft_wb = engines.raft.log_batch(1024);

        for region_id in 1..region_count + 1 {
            let mut region = metapb::Region::default();
            region.set_id(region_id);
            region.set_start_key(format!("{:06}", region_id - 1).into_bytes());
            region.set_end_key(format!("{:06}", region_id).into_bytes());
            region.mut_region_epoch().set_version(epoch_ver);
            region.mut_region_epoch().set_conf_ver(epoch_conf_ver);
            for i in 1..node_count + 1 {
                region.mut_peers().push(new_peer(i as u64, 1));
            }

            write_peer_state(&mut kv_wb, &region, PeerState::Normal, None).unwrap();
            write_initial_apply_state(&mut kv_wb, region.get_id()).unwrap();
            write_initial_raft_state(&mut raft_wb, region.get_id()).unwrap();
        }

        kv_wb.write().unwrap();
        engines.sync_kv().unwrap();
        engines.raft.consume(&mut raft_wb, true).unwrap();
    }
    cluster
}

fn run_all_nodes(cluster: &mut Cluster<NodeCluster>, node_count: usize) {
    for i in 1..node_count + 1 {
        cluster.run_node(i as u64).unwrap();
    }
}

fn bytes_to_gib(bytes: usize) -> f64 {
    bytes as f64 / GIB as f64
}

fn print_memory_usage(message: &str) {
    println!("{}: {:.3} GiB", message, bytes_to_gib(memory_stats().unwrap().physical_mem));
    for m in MEM_TRACE_SUM_GAUGE.collect()[0].get_metric() {
        println!("trace {:?} {:.3} GiB", m.get_label(), bytes_to_gib(m.get_gauge().get_value() as usize));
    }
}

fn test_memory_usage_with_region_count(node_count: u64, region_count: u64) {
    let mut mem_trace_metrics = MemoryTraceManager::default();
    mem_trace_metrics.register_provider(MEMTRACE_RAFTSTORE.clone());
   
    activate_prof().unwrap();
    let mut cluster = new_cluster_with_many_regions(node_count as usize, region_count);
    run_all_nodes(&mut cluster, node_count as usize);

    println!("wait for all regions to report heartbeat");
    let timer = Instant::now();
    while cluster.pd_client.get_regions_number() < region_count as usize {
        std::thread::sleep(std::time::Duration::from_millis(100));
        if timer.saturating_elapsed() > Duration::from_secs(max(5, region_count / 200)) {
            panic!("wait for all regions to report heartbeat timeout");
        }
    }
    std::thread::sleep(Duration::from_secs(15));
    deactivate_prof().unwrap();
    dump_prof("./heap1").unwrap();

    let now = Instant::now();
    mem_trace_metrics.flush(now);
    print_memory_usage(format!("memory usage with {} regions", region_count).as_str());
    drop(cluster);
}

/// Test the store start time of a cluster with many regions.
/// One test shows that it takes 8.5s to start with 10k regions,
/// and it takes 48s when the region count is 50k.
///
/// Note that the total start time of a tikv instance mainly consists of
/// 1. the time to start the storage (raft-engine + rocksdb)
/// 2. the time to start the raftstore
fn test_store_start_time_with_region_count(region_count: u64) {
    let node_count = 3;
    let mut cluster = new_cluster_with_many_regions(node_count, region_count);
    let now = Instant::now();
    run_all_nodes(&mut cluster, node_count as usize);
    println!(
        "It takes {} seconds to start {} nodes with {} regions",
        now.saturating_elapsed_secs(),
        node_count,
        region_count
    );
}

// #[ignore]
// #[test]
// fn test_store_start_time_with_10k() {
//     test_start_time_with_region_count(10_000);
// }

// #[ignore]
// #[test]
// fn test_store_start_time_with_50k() {
//     test_start_time_with_region_count(50_000);
// }

#[ignore]
#[test]
fn test_memory_usage_with_1k_regions() {
    test_memory_usage_with_region_count(1, 1000);
}

#[ignore]
#[test]
fn test_memory_usage_with_10k_regions() {
    test_memory_usage_with_region_count(1, 10_000);
}

// #[ignore]
// #[test]
// fn test_memory_usage_with_10k_regions_3_node() {
//     test_memory_usage_with_region_count(3, 10_000);
// }

#[ignore]
#[test]
fn test_memory_usage_with_30k_regions() {
    test_memory_usage_with_region_count(1, 30_000);
}

#[ignore]
#[test]
fn test_memory_usage_with_50k_regions() {
    test_memory_usage_with_region_count(1, 50_000);
}

//     let mut stats = ProcessStat::cur_proc_stat().unwrap();
//     activate_prof().unwrap();
//
//     for i in 1..node_count + 1 {
//         cluster.run_node(i).unwrap();
//     }
//     print_memory_usage(
//         format!(
//             "memory usage after cluster starts with {} regions",
//             region_count
//         )
//         .as_str(),
//     );
//     sleep_ms(10000);
//     println!("cpu usage: {:.3}", stats.cpu_usage().unwrap());
//     print_memory_usage("memory usage after cluster works for a while");
//     deactivate_prof().unwrap();
//     dump_prof("./heap1").unwrap();
//     println!("heap1 dumped");
//
//     activate_prof().unwrap();
//     sleep_ms(10000);
//     println!("cpu usage: {:.3}", stats.cpu_usage().unwrap());
//     print_memory_usage("memory usage after cluster works for a while");
//     deactivate_prof().unwrap();
//     dump_prof("./heap2").unwrap();
//     println!("heap2 dumped");
// }
