//! 有界缓冲、事件优先级与合并(§17.6)。
//!
//! 三类优先级:
//! 1. 不可静默丢弃(Critical):问题、审批、turn 生命周期、摘要变化、队列状态。
//! 2. 可由 snapshot 恢复(Recoverable):计划、命令状态、文件/Git 摘要、item 可见内容。
//! 3. 可合并(Mergeable):高频输出增量、Token 用量更新、presence、心跳。
//!
//! 压力策略:先合并第 3 类(相邻同 key 且偏移连续的输出增量无损合并、
//! ReplaceLatest 只保留最新);仍不足时向慢 consumer 发 ResyncRequired 并断开,
//! 绝不阻塞 Bridge 上游、绝不丢第 1 类。

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError},
    time::Instant,
};

use agent_console_protocol::{
    codec::{encode_envelope, MAX_FRAME_BYTES},
    v1::{domain_event, envelope, Envelope},
};
use prost::Message;

/// 线上字节(廉价克隆,供多订阅者扇出)。
pub type WireBytes = bytes::Bytes;

// ---------------------------------------------------------------------------
// 帧元数据
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameKind {
    /// 第 1 优先级:不可静默丢弃。
    Critical,
    /// 第 2 优先级:可由 snapshot 恢复;上游缓冲压力下可被挤出。
    Recoverable,
    /// 第 3 优先级:可合并。
    Mergeable,
}

/// 合并信息;由事件分类时提取,避免合并时反复解码。
#[derive(Clone, Debug)]
pub enum MergeInfo {
    /// 输出增量:同 key 且偏移连续时可无损合并。
    OutputAppend {
        key: String,
        offset: u64,
        len: usize,
    },
    /// 同 key 只保留最新(token 用量、presence、心跳、后台命令状态)。
    ReplaceLatest { key: String },
}

/// 待发送/缓冲的流帧:保留解码后的 Envelope 以便重编号与合并,编码结果缓存一次。
pub struct Frame {
    pub env: Envelope,
    pub kind: FrameKind,
    pub merge: Option<MergeInfo>,
    pub ts: Instant,
    pub bytes_len: usize,
    encoded: OnceLock<WireBytes>,
}

impl Frame {
    pub fn new(env: Envelope, kind: FrameKind, merge: Option<MergeInfo>) -> Arc<Frame> {
        let bytes_len = env.encoded_len();
        Arc::new(Frame {
            env,
            kind,
            merge,
            ts: Instant::now(),
            bytes_len,
            encoded: OnceLock::new(),
        })
    }

    /// 编码后的帧(缓存)。编码失败(理论不可达:源帧已过 1 MiB 校验)返回空帧。
    pub fn encoded(&self) -> WireBytes {
        self.encoded
            .get_or_init(|| {
                encode_envelope(&self.env)
                    .map(Into::into)
                    .unwrap_or_default()
            })
            .clone()
    }

    /// 重编号(epoch/sequence 变化)后的新帧(合并缓存)。
    pub fn renumbered(&self, epoch: u64, sequence: u64) -> Arc<Frame> {
        let mut env = self.env.clone();
        env.stream_epoch = epoch;
        env.sequence = sequence;
        Frame::new(env, self.kind, self.merge.clone())
    }
}

/// 从领域事件分类优先级与合并信息(§17.6)。
pub fn classify(event: &domain_event::Event) -> (FrameKind, Option<MergeInfo>) {
    use domain_event::Event as E;
    match event {
        // 第 1 优先级:问题、审批、turn 生命周期、摘要变化、队列状态。
        E::TurnLifecycle(_)
        | E::SessionSummaryChanged(_)
        | E::PendingAttentionAdded(_)
        | E::PendingAttentionRemoved(_)
        | E::QueueStateChanged(_) => (FrameKind::Critical, None),
        // presence 心跳:第 3 优先级,同设备只保留最新。
        E::DevicePresenceChanged(p) => (
            FrameKind::Mergeable,
            Some(MergeInfo::ReplaceLatest {
                key: format!(
                    "presence:{}",
                    p.presence
                        .as_ref()
                        .map(|x| x.device_id.as_str())
                        .unwrap_or("")
                ),
            }),
        ),
        E::CapabilityChanged(_) => (
            FrameKind::Recoverable,
            Some(MergeInfo::ReplaceLatest {
                key: "capability".into(),
            }),
        ),
        E::BackgroundCommandChanged(c) => (
            FrameKind::Recoverable,
            Some(MergeInfo::ReplaceLatest {
                key: format!(
                    "bg:{}",
                    c.command
                        .as_ref()
                        .map(|x| x.command_id.as_str())
                        .unwrap_or("")
                ),
            }),
        ),
        E::ItemUpsert(up) => {
            let item_key = up
                .item
                .as_ref()
                .and_then(|i| i.item_id.as_ref())
                .map(|id| id.id.clone())
                .unwrap_or_default();
            match up.item.as_ref().and_then(|i| i.content.as_ref()) {
                Some(item::Content::TokenUsage(_)) => (
                    FrameKind::Mergeable,
                    Some(MergeInfo::ReplaceLatest {
                        key: format!("token:{item_key}"),
                    }),
                ),
                Some(item::Content::Question(_)) | Some(item::Content::Approval(_)) => {
                    (FrameKind::Critical, None)
                }
                // 其余 item 内容(消息、计划、工具、命令状态、文件变化、子 Agent)
                // 都可由 history/snapshot 重新读取:第 2 优先级。
                _ => (FrameKind::Recoverable, None),
            }
        }
        // 输出增量:第 3 优先级;同 item+channel 且偏移连续时可无损合并。
        E::OutputAppend(a) => (
            FrameKind::Mergeable,
            Some(MergeInfo::OutputAppend {
                key: format!(
                    "out:{}:{}",
                    a.item_id.as_ref().map(|i| i.id.as_str()).unwrap_or(""),
                    a.channel
                ),
                offset: a.expected_offset,
                len: a.bytes.len(),
            }),
        ),
        // 输出校正/定稿可经 snapshot/查询恢复:第 2 优先级。
        E::OutputReplace(_) | E::OutputFinal(_) => (FrameKind::Recoverable, None),
    }
}

use agent_console_protocol::v1::item;

// ---------------------------------------------------------------------------
// 帧合并
// ---------------------------------------------------------------------------

/// 相邻两帧合并:仅当两者都是同 key 且偏移连续的输出增量时成立。
fn merge_pair(a: &Frame, b: &Frame) -> Option<Arc<Frame>> {
    let MergeInfo::OutputAppend {
        key: ka,
        offset: oa,
        len: la,
    } = a.merge.as_ref()?
    else {
        return None;
    };
    let MergeInfo::OutputAppend {
        key: kb,
        offset: ob,
        len: lb,
    } = b.merge.as_ref()?
    else {
        return None;
    };
    if ka != kb || oa + *la as u64 != *ob {
        return None;
    }
    let mut env = a.env.clone();
    let app_a = match env.payload.as_mut()? {
        envelope::Payload::EventBatch(batch) => match batch.events.last_mut()?.event.as_mut()? {
            domain_event::Event::OutputAppend(app) => app,
            _ => return None,
        },
        _ => return None,
    };
    let bytes_b: &[u8] = match b.env.payload.as_ref()? {
        envelope::Payload::EventBatch(batch) => match batch.events.last()?.event.as_ref()? {
            domain_event::Event::OutputAppend(app) => &app.bytes,
            _ => return None,
        },
        _ => return None,
    };
    if app_a.bytes.len() + bytes_b.len() > MAX_FRAME_BYTES {
        return None;
    }
    app_a.bytes.extend_from_slice(bytes_b);
    Some(Frame::new(
        env,
        a.kind,
        Some(MergeInfo::OutputAppend {
            key: ka.clone(),
            offset: *oa,
            len: la + lb,
        }),
    ))
}

/// 单次从左到右的合并扫描:相邻连续输出增量合并;ReplaceLatest 同 key 只留最新。
pub fn coalesce(items: &mut VecDeque<Arc<Frame>>, bytes: &mut usize) {
    // ReplaceLatest:同 key 保留最后一份(有界回看窗口)。
    const REPLACE_WINDOW: usize = 64;
    let n = items.len();
    if n > 1 {
        let mut remove = vec![false; n];
        for i in 0..n {
            let Some(MergeInfo::ReplaceLatest { key: ki }) = items[i].merge.as_ref() else {
                continue;
            };
            for j in (i + 1)..n.min(i + 1 + REPLACE_WINDOW) {
                if let Some(MergeInfo::ReplaceLatest { key: kj }) = items[j].merge.as_ref() {
                    if ki == kj {
                        remove[i] = true;
                        break;
                    }
                }
            }
        }
        if remove.iter().any(|r| *r) {
            let mut kept: VecDeque<Arc<Frame>> = VecDeque::with_capacity(n);
            let mut total = 0usize;
            for (i, f) in items.drain(..).enumerate() {
                if remove[i] {
                    continue;
                }
                total += f.bytes_len;
                kept.push_back(f);
            }
            *bytes = total;
            *items = kept;
        }
    }
    // 相邻连续输出增量合并。
    let mut result: VecDeque<Arc<Frame>> = VecDeque::with_capacity(items.len());
    let mut total = 0usize;
    for f in items.drain(..) {
        let can_merge = result
            .back()
            .map(|prev| merge_pair(prev, &f).is_some())
            .unwrap_or(false);
        if can_merge {
            let merged = merge_pair(result.back().unwrap(), &f).unwrap();
            let prev = result.pop_back().unwrap();
            total = total - prev.bytes_len + merged.bytes_len;
            result.push_back(merged);
        } else {
            total += f.bytes_len;
            result.push_back(f);
        }
    }
    *bytes = total;
    *items = result;
}

// ---------------------------------------------------------------------------
// 有界流缓冲(§17.6 三重上限:时长/条数/字节,取先到者)
// ---------------------------------------------------------------------------

pub struct StreamBuffer {
    pub items: VecDeque<Arc<Frame>>,
    pub bytes: usize,
    max_events: usize,
    max_bytes: usize,
    retention: std::time::Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertOutcome {
    Buffered,
    /// 合并/淘汰后仍超限(只剩第 1 类),缓冲被整体重置;
    /// 调用方必须对订阅者发 ResyncRequired 并重取 snapshot。
    OverflowReset,
}

impl StreamBuffer {
    pub fn new(max_events: usize, max_bytes: usize, retention: std::time::Duration) -> Self {
        Self {
            items: VecDeque::new(),
            bytes: 0,
            max_events,
            max_bytes,
            retention,
        }
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// 插入一帧;超限时先合并第 3 类,再从头挤出第 3、第 2 类;
    /// 仍超限(只剩第 1 类)时整体重置并返回 `OverflowReset`。
    pub fn insert(&mut self, frame: Arc<Frame>) -> InsertOutcome {
        // 惰性按保留时长挤出过期帧(§18:操作时顺带有界清理)。
        let now = Instant::now();
        while let Some(front) = self.items.front() {
            if now.duration_since(front.ts) > self.retention {
                if let Some(f) = self.items.pop_front() {
                    self.bytes = self.bytes.saturating_sub(f.bytes_len);
                }
            } else {
                break;
            }
        }

        // 插入时合并:同 key 连续输出增量直接并入队尾帧;ReplaceLatest 同 key 去掉旧帧。
        let mergeable_tail = {
            let prev = self.items.back();
            prev.and_then(|p| merge_pair(p, &frame))
        };
        match mergeable_tail {
            Some(merged) => {
                if let Some(prev) = self.items.pop_back() {
                    self.bytes = self.bytes.saturating_sub(prev.bytes_len);
                }
                self.bytes += merged.bytes_len;
                self.items.push_back(merged);
            }
            None => {
                if let Some(MergeInfo::ReplaceLatest { key }) = frame.merge.as_ref() {
                    let key = key.clone();
                    let back = self.items.iter().rev().take(32).position(|f| {
                        matches!(f.merge.as_ref(), Some(MergeInfo::ReplaceLatest { key: k }) if *k == key)
                    });
                    if let Some(pos) = back.map(|p| self.items.len() - 1 - p) {
                        if let Some(old) = self.items.remove(pos) {
                            self.bytes = self.bytes.saturating_sub(old.bytes_len);
                        }
                    }
                }
                self.items.push_back(frame.clone());
                self.bytes += frame.bytes_len;
            }
        }

        if self.items.len() <= self.max_events && self.bytes <= self.max_bytes {
            return InsertOutcome::Buffered;
        }

        // 压力路径:先合并第 3 类。
        coalesce(&mut self.items, &mut self.bytes);
        if self.items.len() <= self.max_events && self.bytes <= self.max_bytes {
            return InsertOutcome::Buffered;
        }
        // 从头挤出第 3 类,再第 2 类;第 1 类绝不静默丢弃。
        for allowed in [FrameKind::Mergeable, FrameKind::Recoverable] {
            while self.items.len() > self.max_events || self.bytes > self.max_bytes {
                match self.items.iter().position(|f| f.kind == allowed) {
                    Some(i) => {
                        if let Some(f) = self.items.remove(i) {
                            self.bytes = self.bytes.saturating_sub(f.bytes_len);
                        }
                    }
                    None => break,
                }
            }
            if self.items.len() <= self.max_events && self.bytes <= self.max_bytes {
                return InsertOutcome::Buffered;
            }
        }
        self.items.clear();
        self.bytes = 0;
        InsertOutcome::OverflowReset
    }

    /// 取出 sequence >= from 的所有帧(窗口内补发,§17.5)。
    pub fn frames_from(&self, from_sequence: u64) -> Vec<Arc<Frame>> {
        self.items
            .iter()
            .filter(|f| f.env.sequence >= from_sequence)
            .cloned()
            .collect()
    }

    pub fn clear(&mut self) {
        self.items.clear();
        self.bytes = 0;
    }
}

// ---------------------------------------------------------------------------
// Browser 发送队列(有界;慢 consumer 处理)
// ---------------------------------------------------------------------------

pub enum OutItem {
    /// 订阅流帧(受优先级/合并约束)。
    Frame(Arc<Frame>),
    /// 直发帧(命令回执、ResyncRequired、握手):不允许因队列满被丢弃。
    Direct(WireBytes),
    /// 关闭连接并携带稳定 close reason。
    Close(&'static str),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushError {
    /// 队列满且合并后仍满:该 subscriber 是慢 consumer。
    SlowConsumer,
    /// 队列已关闭。
    Closed,
}

/// 每 Browser 有界发送队列(§17.6)。
pub struct Outbox {
    capacity: usize,
    inner: Mutex<OutboxInner>,
    notify: tokio::sync::Notify,
}

struct OutboxInner {
    items: VecDeque<OutItem>,
    frames: usize,
    closed: bool,
}

impl Outbox {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            inner: Mutex::new(OutboxInner {
                items: VecDeque::new(),
                frames: 0,
                closed: false,
            }),
            notify: tokio::sync::Notify::new(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, OutboxInner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn len(&self) -> usize {
        self.lock().frames
    }

    pub fn is_closed(&self) -> bool {
        self.lock().closed
    }

    /// 推送流帧。满时先对队列做一次合并扫描;仍满则返回 `SlowConsumer`,
    /// 由调用方发送 ResyncRequired 并断开该 subscriber,不阻塞上游。
    pub fn push_frame(&self, frame: Arc<Frame>) -> Result<(), PushError> {
        let mut g = self.lock();
        if g.closed {
            return Err(PushError::Closed);
        }
        g.items.push_back(OutItem::Frame(frame));
        g.frames += 1;
        if g.frames > self.capacity {
            let OutboxInner { items, frames, .. } = &mut *g;
            coalesce_outbox(items, frames);
            if g.frames > self.capacity {
                // 撤回刚推入的帧,由调用方走 resync+断开路径。
                if let Some(OutItem::Frame(_)) = g.items.pop_back() {
                    g.frames -= 1;
                }
                return Err(PushError::SlowConsumer);
            }
        }
        drop(g);
        self.notify.notify_waiters();
        Ok(())
    }

    /// 推送直发帧(P1 语义):关闭后丢弃;容量不足时仍入队(直发帧低频且关键)。
    pub fn push_direct(&self, bytes: WireBytes) -> bool {
        {
            let mut g = self.lock();
            if g.closed {
                return false;
            }
            g.items.push_back(OutItem::Direct(bytes));
        }
        self.notify.notify_waiters();
        true
    }

    pub fn close(&self, reason: &'static str) {
        {
            let mut g = self.lock();
            if !g.closed {
                g.closed = true;
                g.items.push_back(OutItem::Close(reason));
            }
        }
        self.notify.notify_waiters();
    }

    /// 写循环取帧;队列空且已关闭时返回 None。
    pub async fn recv(&self) -> Option<OutItem> {
        loop {
            // 先注册唤醒兴趣(enable)再检查队列,避免错过 push 的 notify_waiters。
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut g = self.lock();
                match g.items.pop_front() {
                    Some(item) => {
                        if matches!(item, OutItem::Frame(_)) {
                            g.frames -= 1;
                        }
                        return Some(item);
                    }
                    None => {
                        if g.closed {
                            return None;
                        }
                    }
                }
            }
            notified.await;
        }
    }
}

/// 对 outbox 队列做一次合并扫描(只处理 Frame 项,Direct 项保持原位)。
fn coalesce_outbox(items: &mut VecDeque<OutItem>, frames: &mut usize) {
    let mut buf: VecDeque<Arc<Frame>> = items
        .iter()
        .filter_map(|i| match i {
            OutItem::Frame(f) => Some(f.clone()),
            _ => None,
        })
        .collect();
    let before = buf.len();
    let mut bytes = buf.iter().map(|f| f.bytes_len).sum();
    coalesce(&mut buf, &mut bytes);
    if buf.len() == before {
        return;
    }
    let mut new_items: VecDeque<OutItem> = VecDeque::with_capacity(items.len());
    let mut it = buf.into_iter();
    for item in items.drain(..) {
        match item {
            OutItem::Frame(_) => {
                if let Some(f) = it.next() {
                    new_items.push_back(OutItem::Frame(f));
                }
            }
            other => new_items.push_back(other),
        }
    }
    *frames = new_items
        .iter()
        .filter(|i| matches!(i, OutItem::Frame(_)))
        .count();
    *items = new_items;
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_console_protocol::v1::{
        DomainEvent, EventBatch, ItemId, OutputAppend, OutputChannel,
    };

    fn append_env(seq: u64, offset: u64, len: usize, item: &str) -> Envelope {
        let event = DomainEvent {
            emitted_at: None,
            event: Some(domain_event::Event::OutputAppend(OutputAppend {
                item_id: Some(ItemId {
                    id: item.into(),
                    synthetic: false,
                }),
                expected_offset: offset,
                bytes: vec![b'x'; len],
                channel: OutputChannel::Combined as i32,
            })),
        };
        Envelope {
            protocol_version: 1,
            message_id: "m".into(),
            correlation_id: String::new(),
            sent_at: None,
            device_id: "d".into(),
            agent_kind: 1,
            stream_id: "s".into(),
            stream_epoch: 1,
            sequence: seq,
            payload: Some(envelope::Payload::EventBatch(EventBatch {
                stream_id: "s".into(),
                events: vec![event],
            })),
            provider_extension: None,
        }
    }

    fn frame_of(env: &Envelope) -> Arc<Frame> {
        let event = match env.payload.as_ref().unwrap() {
            envelope::Payload::EventBatch(b) => &b.events[0],
            _ => unreachable!(),
        };
        let (kind, merge) = classify(event.event.as_ref().unwrap());
        Frame::new(env.clone(), kind, merge)
    }

    fn critical_env(seq: u64) -> Envelope {
        let event = DomainEvent {
            emitted_at: None,
            event: Some(domain_event::Event::QueueStateChanged(
                agent_console_protocol::v1::QueueStateChanged { queue: None },
            )),
        };
        Envelope {
            protocol_version: 1,
            message_id: "m".into(),
            correlation_id: String::new(),
            sent_at: None,
            device_id: "d".into(),
            agent_kind: 1,
            stream_id: "s".into(),
            stream_epoch: 1,
            sequence: seq,
            payload: Some(envelope::Payload::EventBatch(EventBatch {
                stream_id: "s".into(),
                events: vec![event],
            })),
            provider_extension: None,
        }
    }

    #[test]
    fn contiguous_appends_coalesce() {
        let mut buf = StreamBuffer::new(100, 1 << 20, std::time::Duration::from_secs(60));
        for i in 0..5u64 {
            buf.insert(frame_of(&append_env(i, i * 10, 10, "item-1")));
        }
        assert_eq!(buf.len(), 1);
        match buf.items.front().unwrap().merge.as_ref() {
            Some(MergeInfo::OutputAppend { offset, len, .. }) => {
                assert_eq!(*offset, 0);
                assert_eq!(*len, 50);
            }
            other => panic!("expected merged output append, got {other:?}"),
        }
    }

    #[test]
    fn non_contiguous_appends_do_not_coalesce() {
        let mut buf = StreamBuffer::new(100, 1 << 20, std::time::Duration::from_secs(60));
        for i in 0..5u64 {
            buf.insert(frame_of(&append_env(i, i * 100, 10, "item-1")));
        }
        assert_eq!(buf.len(), 5);
    }

    #[test]
    fn event_count_limit_evicts_mergeable_first() {
        let mut buf = StreamBuffer::new(10, usize::MAX, std::time::Duration::from_secs(60));
        for i in 0..12u64 {
            let out = buf.insert(frame_of(&append_env(i, i * 100, 10, "item-1")));
            assert_eq!(out, InsertOutcome::Buffered);
        }
        assert!(buf.len() <= 10);
        assert!(buf.items.front().unwrap().env.sequence >= 2);
    }

    #[test]
    fn critical_events_are_never_evicted() {
        let mut buf = StreamBuffer::new(4, usize::MAX, std::time::Duration::from_secs(60));
        buf.insert(frame_of(&critical_env(0)));
        for i in 1..40u64 {
            let out = buf.insert(frame_of(&append_env(i, i * 100, 10, "item-1")));
            assert_eq!(out, InsertOutcome::Buffered, "critical 必须保留,其余被挤出");
        }
        assert_eq!(buf.items.front().unwrap().kind, FrameKind::Critical);
    }

    #[test]
    fn replace_latest_keeps_only_newest() {
        let mut buf = StreamBuffer::new(100, usize::MAX, std::time::Duration::from_secs(60));
        for i in 0..6u64 {
            // 同一 presence key 的 ReplaceLatest 帧只保留最新。
            let event = DomainEvent {
                emitted_at: None,
                event: Some(domain_event::Event::DevicePresenceChanged(
                    agent_console_protocol::v1::DevicePresenceChanged {
                        presence: Some(agent_console_protocol::v1::DevicePresence {
                            device_id: "d1".into(),
                            ..Default::default()
                        }),
                    },
                )),
            };
            let mut env = critical_env(i);
            env.payload = Some(envelope::Payload::EventBatch(EventBatch {
                stream_id: "s".into(),
                events: vec![event],
            }));
            buf.insert(frame_of(&env));
        }
        assert_eq!(buf.len(), 1);
    }

    #[test]
    fn byte_limit_triggers_overflow_reset_when_only_critical_left() {
        let mut buf = StreamBuffer::new(10_000, 8, std::time::Duration::from_secs(60));
        let out = buf.insert(frame_of(&critical_env(0)));
        assert_eq!(out, InsertOutcome::OverflowReset);
        assert!(buf.is_empty());
    }

    #[tokio::test]
    async fn outbox_slow_consumer_detected() {
        let outbox = Outbox::new(4);
        for i in 0..4u64 {
            assert!(outbox
                .push_frame(frame_of(&append_env(i, i * 100, 10, "item-1")))
                .is_ok());
        }
        let r = outbox.push_frame(frame_of(&append_env(5, 500, 10, "item-1")));
        assert_eq!(r, Err(PushError::SlowConsumer));
        outbox.close("RESYNC_REQUIRED");
        assert!(outbox.recv().await.is_some());
    }

    #[tokio::test]
    async fn outbox_recv_returns_items_then_none_after_close() {
        let outbox = Outbox::new(8);
        outbox
            .push_frame(frame_of(&append_env(0, 0, 4, "i")))
            .unwrap();
        outbox.close("AUTH_EXPIRED");
        let mut count = 0;
        while let Some(item) = outbox.recv().await {
            if matches!(item, OutItem::Close(_)) {
                break;
            }
            count += 1;
        }
        assert_eq!(count, 1);
        assert!(outbox.recv().await.is_none());
    }
}
