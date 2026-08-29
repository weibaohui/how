//! Client-side connection pool to a single WSP server target.

use crate::client::connection::{Connection, Status};
use crate::client::{ClientConfig, ClientInner};
use crate::log;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

/// Cap for the connector's exponential failure backoff (2, 4, 8, 16, 32, 60s).
const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// Pool manage a pool of connection to a remote Server.
pub struct Pool {
    client: Arc<ClientInner>,
    pub target: String,
    secret_key: String,
    connections: Mutex<Vec<Arc<Connection>>>,
    done: CancellationToken,
    self_weak: Weak<Pool>,
    /// Consecutive connection failures since the last stable connection, used
    /// to grow the backoff. Reset to 0 by a stable connection (one that lived
    /// past `STABLE_LIFETIME` in `connection.rs`).
    failures: Mutex<u32>,
    /// Deadline until which `connector()` refuses to dial. `None` when not
    /// backing off. Set by `note_outcome(false)`, cleared by `note_outcome(true)`.
    backoff_until: Mutex<Option<Instant>>,
    /// Last pool-composition snapshot that was logged. The stats line prints
    /// only when the composition CHANGES — keyed on (connecting, idle) only:
    /// tunnel created / connected / closed. `running` is shown in the line
    /// but deliberately NOT part of the key, so a request passing through
    /// (Idle -> Running -> Idle) does not emit two extra lines each time.
    last_stats: Mutex<Option<(usize, usize)>>,
}

/// Number of open connections per status.
#[allow(dead_code)]
struct PoolSize {
    connecting: usize,
    idle: usize,
    running: usize,
    total: usize,
}

impl Pool {
    pub(crate) fn new(
        client: Arc<ClientInner>,
        target: String,
        secret_key: String,
        done: CancellationToken,
    ) -> Arc<Self> {
        Arc::new_cyclic(|weak: &Weak<Pool>| Pool {
            client,
            target,
            secret_key,
            connections: Mutex::new(Vec::new()),
            done,
            self_weak: weak.clone(),
            failures: Mutex::new(0),
            backoff_until: Mutex::new(None),
            last_stats: Mutex::new(None),
        })
    }

    /// Record the outcome of a connection's lifecycle to drive the
    /// connector's failure backoff.
    ///
    /// - `ok=false` (connect error, or handshake ok then died within
    ///   `STABLE_LIFETIME` — e.g. the server accepts then immediately closes):
    ///   bump the consecutive-failure counter and schedule an exponentially
    ///   growing backoff (2, 4, 8, 16, 32, 60, 60… s). This stops the
    ///   connector from hammering an unavailable/broken server once a second.
    /// - `ok=true` (the connection lived past `STABLE_LIFETIME`): the server
    ///   is healthy, so reset the backoff and resume 1s dial cadence.
    pub(crate) fn note_outcome(&self, ok: bool) {
        let mut failures = self.failures.lock().unwrap();
        let mut backoff = self.backoff_until.lock().unwrap();
        if ok {
            // Only log the recovery when there was actually a backoff to
            // clear — every healthy long-lived close calls this too, and
            // those must stay silent.
            if *failures > 0 || backoff.is_some() {
                log::log("Stable connection: connector backoff cleared".to_string());
            }
            *failures = 0;
            *backoff = None;
            return;
        }
        *failures = failures.saturating_add(1);
        // 1<<1=2, 1<<2=4, 1<<3=8, 1<<4=16, 1<<5=32, 1<<6=64 -> cap 60.
        let shift = (*failures).min(6);
        let secs = (1u64 << shift).min(MAX_BACKOFF.as_secs());
        *backoff = Some(Instant::now() + Duration::from_secs(secs));
        // One line per failure event: while backing off the connector dials
        // at the backoff cadence (not 1s), so this is naturally low-noise
        // and makes the backoff observable in the field.
        log::log(format!(
            "Connection failure #{failures}: backing off {secs}s before the next dial"
        ));
    }

    /// Whether the connector is currently backing off (should not dial). The
    /// 1s tick still fires `connector()`, but it returns immediately so there
    /// is no log spam and no pressure on an unavailable server.
    fn in_backoff(&self) -> bool {
        match *self.backoff_until.lock().unwrap() {
            Some(until) => Instant::now() < until,
            None => false,
        }
    }

    /// Start the pool: connect once, then refresh every second.
    pub fn start(self: Arc<Self>) {
        // connector() FIRST, then the stats snapshot: the freshly created
        // `Connecting` entries are captured by this tick's line instead of
        // appearing only after they resolve (a sub-second connect would
        // otherwise never show a connecting>0 state at all).
        self.connector();
        self.log_stats_if_changed();
        let this = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = this.done.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {
                        this.connector();
                        this.log_stats_if_changed();
                    }
                }
            }
        });
    }

    /// Log "pool: N tunnels (connecting=X, idle=Y, running=Z)" whenever the
    /// composition changes since the previous line (keyed on connecting/idle
    /// only — see `last_stats`). Checked on the 1s tick, so a transition is
    /// reflected at most one second after it happens.
    fn log_stats_if_changed(&self) {
        let (key, connecting, idle, running) = {
            let conns = self.connections.lock().unwrap();
            let size = self.size_locked(&conns);
            (
                (size.connecting, size.idle),
                size.connecting,
                size.idle,
                size.running,
            )
        };
        let mut last = self.last_stats.lock().unwrap();
        if *last != Some(key) {
            *last = Some(key);
            let total = connecting + idle + running;
            log::log(format!(
                "pool: {total} tunnel{} (connecting={connecting}, idle={idle}, running={running})",
                if total == 1 { "" } else { "s" }
            ));
        }
    }

    /// Ensure enough idle connections, up to `PoolMaxSize`. Create only one
    /// connection if the pool is empty. Mirrors Go's `pool.connector`.
    ///
    /// No-op while backing off after consecutive connection failures, so a
    /// broken/unavailable server is not hammered once a second.
    pub fn connector(&self) {
        if self.in_backoff() {
            return;
        }
        let mut conns = self.connections.lock().unwrap();
        let size = self.size_locked(&conns);

        let mut to_create = self.client.config.pool_idle_size - size.idle as i64;
        if size.total == 0 {
            to_create = 1;
        }
        if size.total as i64 + to_create > self.client.config.pool_max_size {
            to_create = self.client.config.pool_max_size - size.total as i64;
        }
        if to_create <= 0 {
            return;
        }

        for _ in 0..to_create {
            let conn = Connection::new(self.self_weak.clone());
            conns.push(conn.clone());
            let target = self.target.clone();
            let secret = self.secret_key.clone();
            let config = self.client.config.clone();
            let pool_idle_size = self.client.config.pool_idle_size;
            let proxy_id = self.client.config.id.clone();
            let http_client = self.client.http_client.clone();
            let conn_c = conn.clone();
            tokio::spawn(async move {
                if let Err(e) = conn_c
                    .connect(
                        &target,
                        &secret,
                        pool_idle_size,
                        proxy_id,
                        http_client,
                        config,
                    )
                    .await
                {
                    log::log(format!("Unable to connect to {} : {}", target, e));
                    conn_c.shutdown();
                }
            });
        }
    }

    /// Remove a connection from the pool (by pointer identity).
    pub fn remove(&self, conn: &Connection) {
        let mut conns = self.connections.lock().unwrap();
        let addr = conn as *const Connection;
        conns.retain(|c| Arc::as_ptr(c) != addr);
    }

    /// Shutdown close all connection in the pool.
    pub fn shutdown(&self) {
        self.done.cancel();
        // 先克隆连接列表并在语句末尾释放锁：下方 `conn.shutdown()` 会回调
        // `pool.remove()` 重新获取同一把 `std::sync::Mutex`，若持锁迭代，
        // 同一线程重入不可重入的互斥锁会永久阻塞（死锁）。
        let conns = self.connections.lock().unwrap().clone();
        for conn in conns.iter() {
            conn.shutdown();
        }
    }

    fn size_locked(&self, conns: &[Arc<Connection>]) -> PoolSize {
        let mut connecting = 0;
        let mut idle = 0;
        let mut running = 0;
        for connection in conns {
            match connection.status() {
                Status::Connecting => connecting += 1,
                Status::Idle => idle += 1,
                Status::Running => running += 1,
                Status::Closed => {}
            }
        }
        PoolSize {
            connecting,
            idle,
            running,
            total: conns.len(),
        }
    }

    /// Expose the shared config (used by the connection at connect time).
    pub fn config(&self) -> &ClientConfig {
        &self.client.config
    }
}

#[cfg(test)]
mod tests {
    //! Tests for the connector's failure backoff state machine. A broken /
    //! unavailable server (connect error, or accepts-then-immediately-closes)
    //! must make the connector back off exponentially instead of hammering it
    //! once a second; a stable connection resets the backoff.

    use super::*;
    use crate::client::{new_config, ClientInner};
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;

    /// 构造一个指向不可达地址的测试用连接池（不发起真实拨号）。
    fn dummy_pool() -> Arc<Pool> {
        let config = Arc::new(new_config());
        let inner = Arc::new(ClientInner {
            config,
            http_client: reqwest::Client::new(),
        });
        Pool::new(
            inner,
            "ws://test.invalid/register".to_string(),
            "k".to_string(),
            CancellationToken::new(),
        )
    }

    /// 读取当前连续失败次数（测试辅助）。
    fn failures(p: &Pool) -> u32 {
        *p.failures.lock().unwrap()
    }

    /// 验证连续失败会指数级拉长退避并阻断连接器，且退避封顶在 60 秒。
    #[test]
    fn failure_grows_backoff_and_blocks_connector() {
        let pool = dummy_pool();
        assert!(!pool.in_backoff());
        assert_eq!(failures(&pool), 0);

        // One failure: 2s backoff, connector is blocked.
        pool.note_outcome(false);
        assert_eq!(failures(&pool), 1);
        assert!(pool.in_backoff());

        // Repeated failures grow the counter; backoff caps at 60s.
        for _ in 0..10 {
            pool.note_outcome(false);
        }
        assert!(failures(&pool) >= 10);
        assert!(pool.in_backoff());
        // Verify the cap: the scheduled deadline is at most 60s out.
        let until = *pool.backoff_until.lock().unwrap();
        let until = until.expect("backoff set after failures");
        let secs = until
            .checked_duration_since(Instant::now())
            .map(|d| d.as_secs())
            .unwrap_or(0);
        assert!(
            secs <= MAX_BACKOFF.as_secs() + 1,
            "backoff must cap at {}s, got {secs}s",
            MAX_BACKOFF.as_secs()
        );
    }

    /// 验证一条稳定连接会把失败计数清零并解除连接器的退避阻断。
    #[test]
    fn stable_connection_resets_backoff() {
        let pool = dummy_pool();
        pool.note_outcome(false);
        pool.note_outcome(false);
        assert_eq!(failures(&pool), 2);
        assert!(pool.in_backoff());

        // A stable connection resets everything and unblocks the connector.
        pool.note_outcome(true);
        assert_eq!(failures(&pool), 0);
        assert!(!pool.in_backoff());
    }

    /// 验证退避期间 `connector()` 完全不动池子（不新增 Connecting 条目）。
    #[test]
    fn connector_is_noop_while_backing_off() {
        // While in backoff, connector() must not touch the pool (no new
        // Connecting entries), so a down server isn't hammered every second.
        let pool = dummy_pool();
        pool.note_outcome(false); // 2s backoff
        assert!(pool.in_backoff());
        let before = pool.connections.lock().unwrap().len();
        pool.connector();
        assert_eq!(
            pool.connections.lock().unwrap().len(),
            before,
            "connector must not dial while in backoff"
        );
    }

    /// 验证退避被稳定连接重置后，下一次失败从 2 秒重新开始而非延续旧计数。
    #[test]
    fn failures_restart_after_reset() {
        // After a stable connection resets the counter, the next failure starts
        // a fresh backoff (failures=1 -> 2s), not continued growth.
        let pool = dummy_pool();
        pool.note_outcome(false);
        pool.note_outcome(false);
        pool.note_outcome(false); // failures=3
        pool.note_outcome(true); // reset -> 0
        pool.note_outcome(false); // failures=1 again
        assert_eq!(failures(&pool), 1);
        assert!(pool.in_backoff());
    }

    /// 验证 `Connection::shutdown()` 的打分接线（打分逻辑在
    /// `connection.rs::is_stable_lifetime`，此处测真实链路）：从未完成
    /// 握手的连接（connect 阶段失败，`connected_at` 为 None）关闭时必须
    /// 向池子记一次失败并进入退避；幂等保护确保重复 shutdown 不会重复记分。
    /// 只有 pool 模块的测试能读到私有的 failures/backoff 状态，故此测试
    /// 放在这里。
    #[test]
    fn shutdown_scores_never_connected_failure_once() {
        let pool = dummy_pool();
        let conn = crate::client::connection::Connection::new(Arc::downgrade(&pool));
        conn.shutdown();
        conn.shutdown(); // idempotent: must not double-score
        assert_eq!(failures(&pool), 1);
        assert!(pool.in_backoff());
    }
}
