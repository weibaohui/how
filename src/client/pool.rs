//! Client-side connection pool to a single WSP server target.

use crate::client::connection::{
    Connection, ProbeOutcome, Status, PROBE_DEADLINE, RUNNING_STALE_FLOOR,
};
use crate::client::{ClientConfig, ClientInner};
use crate::log;
use futures::future::join_all;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

/// Cap for the connector's exponential failure backoff (2, 4, 8, 16, 32, 60s).
const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// Run `f` in an isolated task so a panic inside it cannot kill the calling
/// loop (tokio aborts only the panicking child). The caller is told via
/// `JoinError` but keeps ticking.
async fn panic_safe<F>(label: &str, f: F) -> Result<(), tokio::task::JoinError>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    match tokio::spawn(f).await {
        Ok(()) => Ok(()),
        Err(e) => {
            log::log(format!("{label} panicked ({e}); continuing next interval"));
            Err(e)
        }
    }
}

/// How long a Connecting tunnel may live before the health round reaps it.
/// With `tunneltimeout` set, the three sequential dial phases (TCP connect,
/// websocket handshake, greeting) are each bounded by it, so a legitimate
/// connect always completes within 3x the budget (+ scheduling margin).
/// Unset means "no per-phase limit" — but NOT "no limit forever": a fixed
/// 60s cap keeps a peer that accepts TCP and then goes silent from leaking
/// pool capacity indefinitely (the Connecting-zombie case).
fn connecting_reap_after(config: &ClientConfig) -> Duration {
    if config.tunnel_timeout > 0 {
        let ms = (config.tunnel_timeout as u64)
            .saturating_mul(3)
            .saturating_add(10_000);
        Duration::from_millis(ms)
    } else {
        Duration::from_secs(60)
    }
}

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

    /// Start the pool: connect once, then refresh every second, and run the
    /// health round on its own cadence (`healthcheckinterval`, default 30s).
    pub fn start(self: Arc<Self>) {
        // connector() FIRST, then the stats snapshot: the freshly created
        // `Connecting` entries are captured by this tick's line instead of
        // appearing only after they resolve (a sub-second connect would
        // otherwise never show a connecting>0 state at all).
        self.connector();
        self.log_stats_if_changed();
        {
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
        // Periodic health supervisor: actively verify every idle tunnel,
        // close the unresponsive ones, and refill the pool immediately (see
        // `health_round`). This is the guarantee the demand-driven connector
        // alone cannot give: while a half-open tunnel still LOOKS idle, no
        // replacement is dialed — the server then has nothing usable and the
        // only fix used to be restarting the client.
        {
            let this = self.clone();
            let interval =
                Duration::from_millis(self.client.config.health_check_interval.max(1) as u64);
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = this.done.cancelled() => break,
                        _ = tokio::time::sleep(interval) => {
                            // Panic isolation: with the per-connection passive
                            // reaper gone, the health round is the ONLY
                            // dead-tunnel detection. A panic must not silently
                            // kill this loop for the whole pool.
                            let round = this.clone();
                            let _ = panic_safe("pool health round", async move {
                                round.health_round(PROBE_DEADLINE).await;
                            })
                            .await;
                        }
                    }
                }
            });
        }
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

    /// One periodic health pass over the pool (run by the health loop spawned
    /// in `start`, every `healthcheckinterval`):
    ///
    /// 1. Reap stale CONNECTING tunnels by age (a dial that never completes
    ///    would hold pool capacity forever when `tunneltimeout` is unset).
    /// 2. Reap wedged RUNNING tunnels via the dual-watermark check (a
    ///    healthy Running tunnel always makes progress in at least one
    ///    direction — reads, writes, or the keepalive ping getting out;
    ///    both stale = the driver is wedged on a half-open link).
    /// 3. Actively probe every Idle tunnel (ping + wait for the pong within
    ///    `probe_deadline`) — concurrently, so the round takes ~RTT, not
    ///    tunnels × RTT. Close and remove the tunnels that do not answer,
    ///    and print each tunnel's status (verdict + pong age) — a wedged
    ///    pool is then visible in the log instead of silently showing
    ///    "10 idle".
    /// 4. Dial the deficit to the target total so the pool always holds
    ///    `pool_idle_size` VERIFIED-available tunnels (see the note at the
    ///    dial site for why this is not delegated to the demand-driven
    ///    connector).
    ///
    /// The pool log line prints every round — the "is my pool actually
    /// alive" heartbeat — while reaped tunnels get their own actionable
    /// lines.
    pub(crate) async fn health_round(&self, probe_deadline: Duration) {
        // Snapshot under the lock, probe without it (probes sleep for up to
        // `probe_deadline` and must not hold the pool hostage).
        let mut idle_conns: Vec<Arc<Connection>> = Vec::new();
        let mut running_conns: Vec<Arc<Connection>> = Vec::new();
        let mut connecting_conns: Vec<Arc<Connection>> = Vec::new();
        {
            let conns = self.connections.lock().unwrap();
            for conn in conns.iter() {
                match conn.status() {
                    Status::Idle => idle_conns.push(conn.clone()),
                    Status::Connecting => connecting_conns.push(conn.clone()),
                    Status::Running => running_conns.push(conn.clone()),
                    Status::Closed => {}
                }
            }
        }
        let started = Instant::now();
        // Wedge backstop: `livenesstimeout` is now only the staleness
        // threshold for tunnels whose write queue is too full to be probed
        // (see `Connection::probe`).
        let stale_after = Duration::from_millis(self.client.config.liveness_timeout.max(0) as u64);

        // (1) Connecting-age reap. shutdown() scores the tunnel a failure
        // (never usable), feeding the connector backoff — exactly right: a
        // server that never completes registrations IS failing. The reaped
        // entry frees its pool slot; `connect()` races the cancel token in
        // every dial phase, so the wedged dial task unwinds instead of
        // leaking.
        let connecting_reap = connecting_reap_after(&self.client.config);
        let mut connecting = 0usize;
        let mut reaped_connecting = 0usize;
        for conn in connecting_conns {
            if conn.created_at().elapsed() > connecting_reap {
                reaped_connecting += 1;
                log::log(format!(
                    "pool health: tunnel#{} STUCK-CONNECTING for {}s (budget {}s); closing",
                    conn.id(),
                    conn.created_at().elapsed().as_secs(),
                    connecting_reap.as_secs()
                ));
                conn.shutdown();
            } else {
                connecting += 1;
            }
        }

        // (2) Running dual-watermark reap. The threshold is clamped up to
        // RUNNING_STALE_FLOOR: a healthy Running tunnel silently waiting
        // for upstream first bytes only SENDS at the 30s keepalive cadence,
        // so its write watermark legitimately ages up to one ping interval
        // — a lower threshold would false-reap healthy waiting tunnels.
        let running_stale = stale_after.max(RUNNING_STALE_FLOOR);
        let mut running = 0usize;
        let mut reaped_running = 0usize;
        for conn in running_conns {
            let silent = conn.last_activity().elapsed();
            let unsent = conn.last_write().elapsed();
            if silent > running_stale && unsent > running_stale {
                reaped_running += 1;
                log::log(format!(
                    "pool health: tunnel#{} WEDGED-RUNNING — no frame received for {}s, none \
                     sent for {}s (threshold {}s); closing",
                    conn.id(),
                    silent.as_secs(),
                    unsent.as_secs(),
                    running_stale.as_secs()
                ));
                conn.shutdown();
            } else {
                running += 1;
            }
        }

        // (3) Probe the idle tunnels.
        let outcomes = join_all(idle_conns.iter().map(|conn| async move {
            (conn.clone(), conn.probe(probe_deadline, stale_after).await)
        }))
        .await;

        let mut ok = 0usize;
        let mut dead = 0usize;
        let mut skipped = 0usize;
        let mut status_line = String::new();
        for (conn, outcome) in outcomes {
            match outcome {
                ProbeOutcome::Ok => {
                    ok += 1;
                    let pong_age = conn
                        .last_pong()
                        .map(|t| format!("{}s", t.elapsed().as_secs()))
                        .unwrap_or_else(|| "-".to_string());
                    status_line.push_str(&format!(" tunnel#{}:ok({pong_age})", conn.id()));
                }
                ProbeOutcome::Skipped => skipped += 1,
                ProbeOutcome::Dead => {
                    dead += 1;
                    let last_pong = conn
                        .last_pong()
                        .map(|t| format!("{}s ago", t.elapsed().as_secs()))
                        .unwrap_or_else(|| "never".to_string());
                    log::log(format!(
                        "pool health: tunnel#{} DEAD — no pong within {}ms (last pong: {last_pong}); \
                         closing, connector re-establishing",
                        conn.id(),
                        probe_deadline.as_millis()
                    ));
                    // shutdown() removes the tunnel from the pool and scores
                    // its lifecycle (a long-lived tunnel scores "stable", so
                    // this normally also clears the dial backoff).
                    conn.shutdown();
                }
            }
        }
        let target = (self.client.config.pool_idle_size.max(0) as usize)
            .min(self.client.config.pool_max_size.max(0) as usize);
        log::log(format!(
            "pool health: idle={} ok={ok} dead={dead} skipped={skipped} connecting={connecting} \
             running={running} reaped_connecting={reaped_connecting} \
             reaped_running={reaped_running} target={target} |{} |round {}ms",
            ok + dead + skipped,
            status_line.trim_start(),
            started.elapsed().as_millis()
        ));
        // (4) Enforce the minimum AVAILABLE tunnel count here instead of
        // delegating to the demand-driven connector: by design the connector
        // never dials while ANY idle tunnel exists, so without this the pool
        // would drift down one tunnel at a time as they die and only refill
        // at zero — the "server has no connection" window this task exists
        // to close. Available = verified-ok idle + skipped (not confirmed
        // dead; the `stale_after` backstop bounds them) + live connecting +
        // live running (busy tunnels return to idle).
        let available = ok + skipped + connecting + running;
        let deficit = target.saturating_sub(available);
        if deficit == 0 {
            return;
        }
        if self.in_backoff() {
            // The backoff (up to 60s) throttles the 1s demand loop after
            // consecutive dial failures — but if the pool has NOTHING left,
            // waiting out the backoff leaves the server connection-less for
            // up to a minute after it comes back. The health cadence (>=10s)
            // is itself a gentle retry rate, so dial exactly ONE rescue
            // tunnel per round: a failure retries no faster than the health
            // interval; a success stabilizes after STABLE_LIFETIME, clears
            // the backoff and lets the normal refill take over.
            if available == 0 {
                log::log(
                    "pool health: pool empty while in dial backoff — dialing one rescue tunnel \
                     at the health cadence"
                        .to_string(),
                );
                self.dial(1);
            }
            return;
        }
        log::log(format!(
            "pool health: {available}/{target} tunnels available — dialing {deficit} to restore \
             the minimum"
        ));
        self.dial(deficit);
    }

    /// Demand-driven connection creation (deliberately diverging from Go's
    /// standing-reserve `pool.connector`): prefer the connections already in
    /// the pool — while any idle tunnel exists, nothing is dialed. New
    /// tunnels appear only when the pool is cold (below the
    /// `pool_idle_size` TOTAL — busy tunnels count toward it) or when every
    /// tunnel is busy (one per pass, capped by `pool_max_size`).
    ///
    /// No-op while backing off after consecutive connection failures, so a
    /// broken/unavailable server is not hammered once a second.
    pub fn connector(&self) {
        if self.in_backoff() {
            return;
        }
        // Scope the lock to the decision: dial() takes the same lock itself
        // (a guard held across the call would self-deadlock on this
        // non-reentrant Mutex — same pitfall as pool.shutdown).
        let to_create: i64 = {
            let conns = self.connections.lock().unwrap();
            let size = self.size_locked(&conns);

            // Prefer the connections already in the pool (demand-driven, not
            // a standing reserve):
            //   - ANY idle tunnel exists      -> never dial a new one; requests
            //                                   are served from the pool.
            //   - pool below the target TOTAL -> warm up. Busy tunnels count
            //                                   toward the target, so concurrent
            //                                   traffic no longer triggers
            //                                   top-ups while idle ones remain.
            //   - every tunnel busy, below    -> add one (capacity expansion on
            //     pool_max_size                  real demand, capped).
            // The warm-up target is clamped to pool_max_size (a misconfigured
            // idle size above the max must not punch through the cap).
            // Note for the expansion case: the new tunnel takes ~1 tick + RTT
            // to register; callers wait at most the server's `timeout` for
            // it — raise that if peak concurrency approaches pool_idle_size.
            let max = self.client.config.pool_max_size.max(0) as usize;
            let idle_target = (self.client.config.pool_idle_size.max(0) as usize).min(max);
            if size.idle > 0 {
                0
            } else if size.total < idle_target {
                (idle_target - size.total) as i64
            } else if size.total < max {
                1
            } else {
                0
            }
        };
        if to_create <= 0 {
            return;
        }
        self.dial(to_create as usize);
    }

    /// Create `n` new tunnels: push a `Connecting` entry and spawn the
    /// asynchronous dial (used by the demand-driven `connector` and by the
    /// health round's minimum-availability top-up).
    fn dial(&self, n: usize) {
        let mut conns = self.connections.lock().unwrap();
        for _ in 0..n {
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

    /// The demand-driven connector policy. Seed connections directly into
    /// the pool (statuses included), call connector(), assert the dial
    /// count. current_thread runtime: the spawned dial tasks never execute
    /// before the assertions, so only the synchronous pushes are observed.
    fn seed(pool: &Arc<Pool>, n: usize, status: crate::client::connection::Status) {
        let mut conns = pool.connections.lock().unwrap();
        for _ in 0..n {
            let c = crate::client::connection::Connection::new(Arc::downgrade(pool));
            c.set_status(status);
            conns.push(c);
        }
    }

    fn total(pool: &Arc<Pool>) -> usize {
        pool.connections.lock().unwrap().len()
    }

    /// While ANY idle tunnel exists, nothing is dialed — requests must be
    /// served from the pool, not by growing it.
    #[tokio::test(flavor = "current_thread")]
    async fn connector_never_dials_while_idle_exists() {
        let pool = dummy_pool();
        seed(&pool, 2, Status::Idle);
        seed(&pool, 3, Status::Running);
        pool.connector();
        assert_eq!(total(&pool), 5, "idle=2 present: no new tunnels");
    }

    /// Cold pool (all busy, below target TOTAL): warm up to pool_idle_size
    /// — busy tunnels count toward the target.
    #[tokio::test(flavor = "current_thread")]
    async fn connector_warms_to_target_total() {
        let pool = dummy_pool();
        seed(&pool, 2, Status::Running);
        pool.connector();
        assert_eq!(total(&pool), 10, "2 running -> top up to idle_size total");
    }

    /// Every tunnel busy at the target: expand by exactly one per pass.
    #[tokio::test(flavor = "current_thread")]
    async fn connector_expands_one_when_all_busy_at_target() {
        let pool = dummy_pool();
        seed(&pool, 10, Status::Running);
        pool.connector();
        assert_eq!(
            total(&pool),
            11,
            "all busy at target: +1, not a refill burst"
        );
    }

    /// At pool_max_size with everything busy: no more dials.
    #[tokio::test(flavor = "current_thread")]
    async fn connector_respects_max_size() {
        let pool = dummy_pool();
        seed(&pool, 100, Status::Running);
        pool.connector();
        assert_eq!(total(&pool), 100, "at max: no expansion");
    }

    /// A misconfigured idle size ABOVE the max must not punch through the
    /// cap: the warm-up target is clamped to pool_max_size (regression: the
    /// first version of the warm-up branch dialed idle_size in one pass).
    #[tokio::test(flavor = "current_thread")]
    async fn connector_caps_warmup_at_max_when_idle_target_exceeds_max() {
        let mut config = new_config();
        config.pool_idle_size = 200;
        config.pool_max_size = 100;
        let inner = Arc::new(ClientInner {
            config: Arc::new(config),
            http_client: reqwest::Client::new(),
        });
        let pool = Pool::new(
            inner,
            "ws://test.invalid/register".to_string(),
            "k".to_string(),
            CancellationToken::new(),
        );
        pool.connector(); // cold pool: warm-up, clamped to max
        assert_eq!(total(&pool), 100, "warm-up must stop at pool_max_size");
    }

    // ------------------------------------------------------------------
    // Health round: the periodic pool supervisor (see `Pool::health_round`).
    // The client-side counterpart of "make sure the server ALWAYS has usable
    // tunnels": probe every idle tunnel, close the unresponsive ones, and
    // trigger an immediate connector pass so replacements are dialed now,
    // not on the next 1s tick.
    // ------------------------------------------------------------------

    /// Seed an ESTABLISHED idle tunnel (usable since long before now, so its
    /// death scores "stable" and does not engage the dial backoff — mirrors
    /// production, where a health-killed tunnel has usually lived for hours).
    /// Without a wired probe writer it still fails the probe (Dead).
    fn seed_established_idle(
        pool: &Arc<Pool>,
        n: usize,
        with_writer: bool,
    ) -> Vec<Arc<Connection>> {
        use tokio_tungstenite::tungstenite::protocol::Message;

        let mut seeded = Vec::new();
        let mut conns = pool.connections.lock().unwrap();
        for _ in 0..n {
            let c = Connection::new(Arc::downgrade(pool));
            c.set_status(Status::Idle);
            let long_alive = Instant::now()
                .checked_sub(Duration::from_secs(300))
                .unwrap_or_else(Instant::now);
            c.set_connected_at(Some(long_alive));
            if with_writer {
                let (tx, mut rx) = tokio::sync::mpsc::channel(8);
                *c.probe_tx.lock().unwrap() = Some(tx);
                // Fake server: answer every ping with a pong (recorded the
                // way the driver records Message::Pong).
                let responder = c.clone();
                tokio::spawn(async move {
                    while let Some(m) = rx.recv().await {
                        if matches!(m, Message::Ping(_)) {
                            responder.record_pong();
                        }
                    }
                });
            }
            conns.push(c.clone());
            seeded.push(c);
        }
        seeded
    }

    /// Unresponsive idle tunnels are closed and removed; running/connecting
    /// tunnels are left alone; the connector immediately refills the pool to
    /// the idle-size TOTAL (10) with new Connecting entries.
    #[tokio::test(flavor = "current_thread")]
    async fn health_round_reaps_dead_idle_and_refills() {
        let pool = dummy_pool();
        seed_established_idle(&pool, 3, false); // dead: no writer, no pong
        seed(&pool, 1, Status::Running);
        seed(&pool, 1, Status::Connecting);

        pool.health_round(Duration::from_millis(30)).await;

        let conns = pool.connections.lock().unwrap();
        let size = pool.size_locked(&conns);
        assert_eq!(size.idle, 0, "unresponsive idle tunnels must be closed");
        assert_eq!(size.running, 1, "running tunnels are never probed");
        assert_eq!(
            size.connecting,
            1 + 8,
            "connector must top the pool back up to idle_size total (10)"
        );
        assert_eq!(conns.len(), 10);
    }

    /// A tunnel that answers the probe is kept (this is the "never kill a
    /// healthy tunnel" invariant — a false Dead here would drain the pool
    /// every health round and reconnect-churn the server).
    #[tokio::test(flavor = "current_thread")]
    async fn health_round_keeps_verified_tunnel() {
        let pool = dummy_pool();
        let healthy = seed_established_idle(&pool, 1, true);

        // Generous probe deadline: a healthy tunnel answers in ~ms; the
        // margin only absorbs a loaded test runner (a too-tight deadline
        // would flake into a false Dead and kill the healthy tunnel).
        pool.health_round(Duration::from_secs(5)).await;

        assert_eq!(
            healthy[0].status(),
            Status::Idle,
            "a tunnel that pongs must survive the health round"
        );
        let conns = pool.connections.lock().unwrap();
        assert!(conns.iter().any(|c| Arc::ptr_eq(c, &healthy[0])));
        // Healthy idle exists -> connector only warms the total up to 10.
        assert_eq!(conns.len(), 10);
    }

    /// The wedge backstop end-to-end through a health round: an idle tunnel
    /// whose write queue is full (cannot be probed) AND that has been silent
    /// for longer than `livenesstimeout` is closed and replaced — the one
    /// case the removed per-connection passive reaper uniquely covered.
    #[tokio::test(flavor = "current_thread")]
    async fn health_round_kills_wedged_tunnel_with_full_queue_and_stale_activity() {
        use tokio_tungstenite::tungstenite::protocol::Message;

        let pool = dummy_pool();
        let conn = Connection::new(Arc::downgrade(&pool));
        conn.set_status(Status::Idle);
        let long_alive = Instant::now()
            .checked_sub(Duration::from_secs(300))
            .unwrap_or_else(Instant::now);
        conn.set_connected_at(Some(long_alive));
        let (tx, _rx) = tokio::sync::mpsc::channel(1); // never drained
        *conn.probe_tx.lock().unwrap() = Some(tx.clone());
        tx.try_send(Message::text("fill")).unwrap(); // queue full
        conn.set_last_activity(long_alive); // silent ever since
        pool.connections.lock().unwrap().push(conn.clone());

        pool.health_round(Duration::from_millis(20)).await;

        assert_eq!(
            conn.status(),
            Status::Closed,
            "a wedged tunnel (full queue + silent) must be closed by the round"
        );
        let conns = pool.connections.lock().unwrap();
        assert_eq!(
            conns.len(),
            10,
            "0 available -> deficit dialed back to the target total"
        );
    }

    /// A panic inside a health round must not kill the health loop: with the
    /// passive reaper gone, the round is the ONLY dead-tunnel detection, so
    /// every round runs in an isolated child task (tokio aborts just the
    /// panicking child) and the loop keeps ticking.
    #[tokio::test(flavor = "current_thread")]
    async fn panic_safe_task_isolates_panics() {
        let boom = panic_safe("test round", async { panic!("boom") });
        assert!(
            boom.await.is_err(),
            "a panicking round must surface a JoinError to the loop, not abort it"
        );
        let fine = panic_safe("test round", async {});
        assert!(fine.await.is_ok());
    }

    /// While in dial backoff with an EMPTY pool (the server was down and has
    /// just come back), the health round still dials exactly ONE rescue
    /// tunnel: the exponential backoff (up to 60s) rightly throttles the 1s
    /// demand loop, but leaving the server connection-less until the backoff
    /// expires is exactly the outage this task exists to prevent. The health
    /// cadence (>= 10s) itself bounds the retry rate, so this cannot hammer
    /// the server.
    #[tokio::test(flavor = "current_thread")]
    async fn health_round_dials_one_rescue_tunnel_when_empty_in_backoff() {
        let pool = dummy_pool();
        pool.note_outcome(false); // 2s backoff, connector is blocked
        assert!(pool.in_backoff());

        pool.health_round(Duration::from_millis(10)).await;

        assert_eq!(
            total(&pool),
            1,
            "empty pool in backoff: exactly one rescue tunnel per health round"
        );
    }

    /// With tunnels still available the backoff is respected (no dial): the
    /// rescue path is only for a completely drained pool.
    #[tokio::test(flavor = "current_thread")]
    async fn health_round_respects_backoff_when_not_empty() {
        let pool = dummy_pool();
        seed(&pool, 1, Status::Running); // available (busy tunnels return)
        pool.note_outcome(false);
        assert!(pool.in_backoff());

        pool.health_round(Duration::from_millis(10)).await;

        assert_eq!(
            total(&pool),
            1,
            "backoff with tunnels available: no dial until the backoff expires"
        );
    }

    /// Killing dead tunnels during a health round must not leave the pool
    /// below the minimum: the health task itself dials the deficit (available
    /// = ok + skipped + connecting + running vs the target total) — the
    /// demand-driven connector would dial NOTHING here, because a healthy
    /// idle tunnel still exists (its "prefer existing connections" rule).
    #[tokio::test(flavor = "current_thread")]
    async fn health_round_refills_only_the_deficit() {
        let pool = dummy_pool();
        seed_established_idle(&pool, 2, true); // healthy
        seed_established_idle(&pool, 3, false); // dead

        pool.health_round(Duration::from_millis(30)).await;

        let conns = pool.connections.lock().unwrap();
        let size = pool.size_locked(&conns);
        assert_eq!(size.idle, 2, "verified tunnels survive");
        assert_eq!(
            size.connecting, 8,
            "deficit vs the 10-tunnel target with 2 available = 8 dialed"
        );
        assert_eq!(conns.len(), 10, "total restored to the idle-size target");
    }

    // ------------------------------------------------------------------
    // Health round: Running dual-watermark reap and Connecting-age reap.
    // Probes only cover Idle tunnels; these two checks close the remaining
    // "no usable tunnel, nothing dialed" holes — a tunnel wedged
    // mid-request on a half-open link, and a dial that never completes.
    // ------------------------------------------------------------------

    /// A Running tunnel with BOTH watermarks stale (no frame received, none
    /// successfully sent) is wedged on a half-open link: the health round
    /// closes it and dials the deficit. The DUAL watermark is the point —
    /// a Running tunnel streaming a response legitimately receives nothing
    /// for minutes, so silence alone must never convict; the write
    /// watermark is what separates "streaming" from "wedged".
    #[tokio::test(flavor = "current_thread")]
    async fn health_round_reaps_wedged_running_tunnel() {
        let pool = dummy_pool();
        let conn = crate::client::connection::Connection::new(Arc::downgrade(&pool));
        conn.set_status(Status::Running);
        let long_ago = Instant::now()
            .checked_sub(Duration::from_secs(300))
            .unwrap_or_else(Instant::now);
        conn.set_connected_at(Some(long_ago)); // death scores "stable" -> no backoff
        conn.set_last_activity(long_ago);
        conn.set_last_write(long_ago);
        pool.connections.lock().unwrap().push(conn.clone());

        pool.health_round(Duration::from_millis(20)).await;

        assert_eq!(
            conn.status(),
            Status::Closed,
            "both watermarks stale = wedged, must be closed"
        );
        let conns = pool.connections.lock().unwrap();
        assert_eq!(conns.len(), 10, "capacity must be restored to the target");
    }

    /// A Running tunnel silently waiting for upstream first bytes is
    /// HEALTHY and must survive: nothing received for a long time
    /// (legitimate — the server only speaks when the request is done), but
    /// the 30s keepalive ping still gets out, refreshing the write
    /// watermark. Only one stale watermark => not wedged.
    #[tokio::test(flavor = "current_thread")]
    async fn health_round_keeps_running_tunnel_with_fresh_write() {
        let pool = dummy_pool();
        let conn = crate::client::connection::Connection::new(Arc::downgrade(&pool));
        conn.set_status(Status::Running);
        let long_ago = Instant::now()
            .checked_sub(Duration::from_secs(300))
            .unwrap_or_else(Instant::now);
        conn.set_connected_at(Some(long_ago));
        conn.set_last_activity(long_ago); // nothing received for ages
                                          // last_write stays "now": the keepalive ping just got out.
        pool.connections.lock().unwrap().push(conn.clone());

        pool.health_round(Duration::from_millis(20)).await;

        assert_eq!(
            conn.status(),
            Status::Running,
            "a fresh write watermark proves the driver is alive — must not be reaped"
        );
    }

    /// A Connecting tunnel older than the reap budget (default 60s when
    /// `tunneltimeout` is unset) is a zombie: the peer accepted TCP but
    /// never completed the registration. The health round closes it — a
    /// never-usable tunnel scores a failure and engages the dial backoff —
    /// and, the pool being empty, dials exactly one rescue tunnel.
    #[tokio::test(flavor = "current_thread")]
    async fn health_round_reaps_stale_connecting_tunnel() {
        let pool = dummy_pool();
        let conn = crate::client::connection::Connection::new(Arc::downgrade(&pool));
        // Status starts Connecting; backdate its creation past the 60s budget.
        let long_ago = Instant::now()
            .checked_sub(Duration::from_secs(300))
            .unwrap_or_else(Instant::now);
        conn.set_created_at(long_ago);
        pool.connections.lock().unwrap().push(conn.clone());

        pool.health_round(Duration::from_millis(20)).await;

        assert_eq!(
            conn.status(),
            Status::Closed,
            "a stuck Connecting tunnel must be reaped by age"
        );
        assert!(
            pool.in_backoff(),
            "a never-usable tunnel scores a failure and engages the backoff"
        );
        let conns = pool.connections.lock().unwrap();
        assert_eq!(
            conns.len(),
            1,
            "empty pool in backoff: exactly one rescue tunnel per round"
        );
    }

    /// A young Connecting tunnel (within the reap budget) is left alone;
    /// the round only tops the pool up to the target total.
    #[tokio::test(flavor = "current_thread")]
    async fn health_round_keeps_young_connecting_tunnel() {
        let pool = dummy_pool();
        seed(&pool, 1, Status::Connecting);

        pool.health_round(Duration::from_millis(20)).await;

        let conns = pool.connections.lock().unwrap();
        let size = pool.size_locked(&conns);
        assert_eq!(
            size.connecting,
            1 + 9,
            "young connecting kept; only the deficit to the target is dialed"
        );
    }

    /// The Connecting reap budget: unset `tunneltimeout` (0 = no per-phase
    /// limit) still bounds a stuck dial at 60s; a configured value budgets
    /// the three sequential dial phases plus a scheduling margin.
    #[test]
    fn connecting_reap_budgets() {
        let mut config = new_config();
        config.tunnel_timeout = 0;
        assert_eq!(connecting_reap_after(&config), Duration::from_secs(60));
        config.tunnel_timeout = 8000;
        assert_eq!(
            connecting_reap_after(&config),
            Duration::from_millis(34_000),
            "3 phases x 8s + 10s margin"
        );
    }
}
