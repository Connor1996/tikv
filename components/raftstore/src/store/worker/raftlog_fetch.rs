// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, RaftEngine};
use std::fmt;
use tikv_util::worker::Runnable;

use crate::store::{SignificantMsg, SignificantRouter};

pub enum Task {
    SendAppend {
        region_id: u64,
        to_peer: u64,
        low: u64,
        high: u64,
        max_size: usize,
    },
    Apply {
        region_id: u64,
        low: u64,
        high: u64,
        max_size: usize,
    },
    ScheduleMerge {
        region_id: u64,
        low: u64,
        high: u64,
        max_size: usize,
    },
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Task::SendAppend {
                region_id,
                to_peer,
                low,
                high,
                max_size,
            } => write!(
                f,
                "Fetch Raft Logs [region: {}, low: {}, high: {}, max_size: {}] for sending to peer {}",
                region_id, low, high, max_size, to_peer,
            ),
            _ => panic!(),
        }
    }
}

pub struct Runner<EK, ER, R>
where
    EK: KvEngine,
    ER: RaftEngine,
    R: SignificantRouter<EK>,
{
    router: R,
    raft_engine: ER,
    _phantom: std::marker::PhantomData<EK>,
}

impl<EK: KvEngine, ER: RaftEngine, R: SignificantRouter<EK>> Runner<EK, ER, R> {
    pub fn new(router: R, raft_engine: ER) -> Runner<EK, ER, R> {
        Runner {
            router,
            raft_engine,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<EK, ER, R> Runnable for Runner<EK, ER, R>
where
    EK: KvEngine,
    ER: RaftEngine,
    R: SignificantRouter<EK>,
{
    type Task = Task;

    fn run(&mut self, task: Task) {
        match task {
            Task::SendAppend {
                region_id,
                low,
                high,
                max_size,
                to_peer,
            } => {
                let mut ents = vec![];
                let res = self.raft_engine.fetch_entries_to(
                    region_id,
                    low,
                    high,
                    Some(max_size),
                    &mut ents,
                );
                self.router
                    .send(
                        region_id,
                        SignificantMsg::RaftLogFetched {
                            to_peer,
                            ents: res.map(|_| ents).map_err(|e| e.into()),
                        },
                    )
                    .unwrap();
            }
            _ => panic!(),
        }
    }
}
