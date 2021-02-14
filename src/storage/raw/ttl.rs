// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
// use crate::storage::{Error, Result};
use crate::storage::kv::{Cursor, Iterator, Result, ScanMode, Snapshot};

use engine_traits::util::{get_expire_ts, strip_expire_ts, truncate_expire_ts};
use engine_traits::CfName;
use engine_traits::{IterOptions, ReadOptions};
use tikv_util::time::UnixSecs;
use txn_types::{Key, Value};

const TEST_CURRENT_TS: u64 = 15;

#[derive(Clone)]
pub struct TTLSnapshot<S: Snapshot> {
    s: S,
    current_ts: u64,
}

impl<S: Snapshot> TTLSnapshot<S> {
    fn map_value(&self, mut value_with_ttl: Result<Option<Value>>) -> Result<Option<Value>> {
        if value_with_ttl.is_err() {
            return value_with_ttl;
        }

        if let Some(v) = value_with_ttl.as_ref().unwrap().as_ref() {
            let expire_ts = get_expire_ts(v)?;
            if expire_ts != 0 && expire_ts < self.current_ts {
                return Ok(None);
            }
        }

        value_with_ttl
            .as_mut()
            .unwrap()
            .as_mut()
            .map(|v| truncate_expire_ts(v).unwrap());
        value_with_ttl
    }

    #[cfg(not(test))]
    #[inline]
    fn current_ts() -> u64 {
        UnixSecs::now().into_inner()
    }

    #[cfg(test)]
    #[inline]
    fn current_ts() -> u64 {
        TEST_CURRENT_TS
    }
}

impl<S: Snapshot> From<S> for TTLSnapshot<S> {
    fn from(s: S) -> Self {
        TTLSnapshot {
            s,
            current_ts: Self::current_ts(),
        }
    }
}

impl<S: Snapshot> Snapshot for TTLSnapshot<S> {
    type Iter = TTLIterator<S::Iter>;

    fn get(&self, key: &Key) -> Result<Option<Value>> {
        self.map_value(self.s.get(key))
    }

    fn get_cf(&self, cf: CfName, key: &Key) -> Result<Option<Value>> {
        self.map_value(self.s.get_cf(cf, key))
    }

    fn get_cf_opt(&self, opts: ReadOptions, cf: CfName, key: &Key) -> Result<Option<Value>> {
        self.map_value(self.s.get_cf_opt(opts, cf, key))
    }

    fn iter(&self, iter_opt: IterOptions, mode: ScanMode) -> Result<Cursor<Self::Iter>> {
        self.s
            .iter(iter_opt, mode)
            .map(|c| c.into_with(|i| TTLIterator::new(i, self.current_ts)))
    }

    fn iter_cf(
        &self,
        cf: CfName,
        iter_opt: IterOptions,
        mode: ScanMode,
    ) -> Result<Cursor<Self::Iter>> {
        self.s
            .iter_cf(cf, iter_opt, mode)
            .map(|c| c.into_with(|i| TTLIterator::new(i, self.current_ts)))
    }

    #[inline]
    fn lower_bound(&self) -> Option<&[u8]> {
        self.s.lower_bound()
    }

    #[inline]
    fn upper_bound(&self) -> Option<&[u8]> {
        self.s.upper_bound()
    }

    #[inline]
    fn get_data_version(&self) -> Option<u64> {
        self.s.get_data_version()
    }

    fn is_max_ts_synced(&self) -> bool {
        self.s.is_max_ts_synced()
    }
}

pub struct TTLIterator<I: Iterator> {
    i: I,
    current_ts: u64,
}

impl<I: Iterator> TTLIterator<I> {
    fn new(i: I, current_ts: u64) -> Self {
        TTLIterator { i, current_ts }
    }

    fn find_valid_value(&mut self, mut res: Result<bool>, forward: bool) -> Result<bool> {
        loop {
            if res.is_err() {
                break;
            }

            if *res.as_ref().unwrap() == true {
                let expire_ts = get_expire_ts(self.i.value())?;
                if expire_ts != 0 && expire_ts < self.current_ts {
                    res = if forward {
                        self.i.next()
                    } else {
                        self.i.prev()
                    };
                    continue;
                }
            }
            break;
        }
        res
    }
}

impl<I: Iterator> Iterator for TTLIterator<I> {
    fn next(&mut self) -> Result<bool> {
        let res = self.i.next();
        self.find_valid_value(res, true)
    }

    fn prev(&mut self) -> Result<bool> {
        let res = self.i.prev();
        self.find_valid_value(res, false)
    }

    fn seek(&mut self, key: &Key) -> Result<bool> {
        let res = self.i.seek(key);
        self.find_valid_value(res, true)
    }

    fn seek_for_prev(&mut self, key: &Key) -> Result<bool> {
        let res = self.i.seek_for_prev(key);
        self.find_valid_value(res, false)
    }

    fn seek_to_first(&mut self) -> Result<bool> {
        let res = self.i.seek_to_first();
        self.find_valid_value(res, true)
    }

    fn seek_to_last(&mut self) -> Result<bool> {
        let res = self.i.seek_to_last();
        self.find_valid_value(res, false)
    }

    fn valid(&self) -> Result<bool> {
        self.i.valid()
    }

    fn validate_key(&self, key: &Key) -> Result<()> {
        self.i.validate_key(key)
    }

    fn key(&self) -> &[u8] {
        self.i.key()
    }

    fn value(&self) -> &[u8] {
        strip_expire_ts(self.i.value())
    }
}

mod tests {
    use super::*;
    use crate::storage::kv::Engine;
    use crate::storage::{SnapContext, TestEngineBuilder};

    use engine_traits::util::append_expire_ts;
    use engine_traits::{SyncMutable, CF_DEFAULT};

    #[test]
    fn test_ttl_snapshot() {
        let dir = tempfile::TempDir::new().unwrap();
        let engine = TestEngineBuilder::new()
            .path(dir.path())
            .enable_ttl()
            .build()
            .unwrap();
        let kvdb = engine.get_rocksdb();

        let key1 = b"key1";
        let mut value1 = b"value1".to_vec();
        append_expire_ts(&mut value1, 10);
        kvdb.put_cf(CF_DEFAULT, key1, &value1).unwrap();
        let mut value10 = b"value1".to_vec();
        append_expire_ts(&mut value10, 20);
        kvdb.put_cf(CF_DEFAULT, key1, &value10).unwrap();

        let key2 = b"key2";
        let mut value2 = b"value2".to_vec();
        append_expire_ts(&mut value2, 20);
        kvdb.put_cf(CF_DEFAULT, key2, &value2).unwrap();
        let mut value20 = b"value2".to_vec();
        append_expire_ts(&mut value20, 10);
        kvdb.put_cf(CF_DEFAULT, key2, &value20).unwrap();

        let key3 = b"key3";
        let mut value3 = b"value3".to_vec();
        append_expire_ts(&mut value3, 0);
        kvdb.put_cf(CF_DEFAULT, key3, &value3).unwrap();

        let snapshot = engine.snapshot(SnapContext::default()).unwrap();
        let ttl_snapshot = TTLSnapshot::from(snapshot);
        assert_eq!(
            ttl_snapshot.get(&Key::from_encoded_slice(b"key1")).unwrap(),
            Some(b"value1".to_vec())
        );
        assert_eq!(
            ttl_snapshot.get(&Key::from_encoded_slice(b"key2")).unwrap(),
            None
        );
        assert_eq!(
            ttl_snapshot.get(&Key::from_encoded_slice(b"key3")).unwrap(),
            Some(b"value3".to_vec())
        );
    }

    #[test]
    fn test_ttl_iterator() {
        let dir = tempfile::TempDir::new().unwrap();
        let engine = TestEngineBuilder::new()
            .path(dir.path())
            .enable_ttl()
            .build()
            .unwrap();
        let kvdb = engine.get_rocksdb();

        let key1 = b"key1";
        let mut value1 = b"value1".to_vec();
        append_expire_ts(&mut value1, 10);
        kvdb.put_cf(CF_DEFAULT, key1, &value1).unwrap();
        let mut value10 = b"value1".to_vec();
        append_expire_ts(&mut value10, 20);
        kvdb.put_cf(CF_DEFAULT, key1, &value10).unwrap();

        let key2 = b"key2";
        let mut value2 = b"value2".to_vec();
        append_expire_ts(&mut value2, 20);
        kvdb.put_cf(CF_DEFAULT, key2, &value2).unwrap();
        let mut value20 = b"value2".to_vec();
        append_expire_ts(&mut value20, 10);
        kvdb.put_cf(CF_DEFAULT, key2, &value20).unwrap();

        let key3 = b"key3";
        let mut value3 = b"value3".to_vec();
        append_expire_ts(&mut value3, 0);
        kvdb.put_cf(CF_DEFAULT, key3, &value3).unwrap();

        let key4 = b"key4";
        let mut value4 = b"value4".to_vec();
        append_expire_ts(&mut value4, 10);
        kvdb.put_cf(CF_DEFAULT, key4, &value4).unwrap();

        let key5 = b"key5";
        let mut value5 = b"value5".to_vec();
        append_expire_ts(&mut value5, 0);
        kvdb.put_cf(CF_DEFAULT, key5, &value5).unwrap();
        let mut value50 = b"value5".to_vec();
        append_expire_ts(&mut value50, 10);
        kvdb.put_cf(CF_DEFAULT, key5, &value50).unwrap();

        let snapshot = engine.snapshot(SnapContext::default()).unwrap();
        let ttl_snapshot = TTLSnapshot::from(snapshot);
        let mut iter = ttl_snapshot
            .iter(IterOptions::new(None, None, false), ScanMode::Mixed)
            .unwrap()
            .iter();
        iter.seek_to_first().unwrap();
        assert_eq!(iter.key(), b"key1");
        assert_eq!(iter.value(), b"value1");
        assert_eq!(iter.next().unwrap(), true);
        assert_eq!(iter.key(), b"key3");
        assert_eq!(iter.value(), b"value3");
        assert_eq!(iter.next().unwrap(), false);

        iter.seek_to_last().unwrap();
        assert_eq!(iter.key(), b"key3");
        assert_eq!(iter.value(), b"value3");
        assert_eq!(iter.prev().unwrap(), true);
        assert_eq!(iter.key(), b"key1");
        assert_eq!(iter.value(), b"value1");
        assert_eq!(iter.prev().unwrap(), false);

        iter.seek(&Key::from_encoded_slice(b"key2")).unwrap();
        assert_eq!(iter.valid().unwrap(), true);
        assert_eq!(iter.key(), b"key3");
        assert_eq!(iter.value(), b"value3");
        iter.seek(&Key::from_encoded_slice(b"key4")).unwrap();
        assert_eq!(iter.valid().unwrap(), false);

        iter.seek_for_prev(&Key::from_encoded_slice(b"key2"))
            .unwrap();
        assert_eq!(iter.valid().unwrap(), true);
        assert_eq!(iter.key(), b"key1");
        assert_eq!(iter.value(), b"value1");
        iter.seek_for_prev(&Key::from_encoded_slice(b"key1"))
            .unwrap();
        assert_eq!(iter.valid().unwrap(), true);
        assert_eq!(iter.key(), b"key1");
        assert_eq!(iter.value(), b"value1");
    }
}
