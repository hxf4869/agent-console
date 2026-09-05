export type DeviceConnection = 'CONNECTING' | 'ONLINE' | 'DEGRADED' | 'OFFLINE'
export type ControlMode = 'FULL_CONTROL' | 'LIMITED_CONTROL' | 'READ_ONLY' | 'UNAVAILABLE'
export type CompatibilityState = 'VERIFIED' | 'DEGRADED' | 'UNSUPPORTED'
export type ActiveTurnPhase = 'IDLE' | 'RUNNING' | 'FINISHING'
export type QueueStatus = 'EMPTY' | 'QUEUED' | 'PAUSED'
export type LastTurnOutcome = 'COMPLETED' | 'FAILED' | 'INTERRUPTED' | 'UNKNOWN'
export type BackgroundCommandState = 'RUNNING' | 'COMPLETED' | 'FAILED' | 'STOPPED' | 'UNKNOWN'
export type AttentionKind = 'USER_QUESTION' | 'RISK_APPROVAL'
export type OutputAuthority = 'LIVE_PREVIEW' | 'AUTHORITATIVE_FINAL' | 'FINAL_OUTPUT_UNAVAILABLE'

export type ControlOperation =
  | 'START_TURN'
  | 'SET_QUEUE'
  | 'REPLACE_QUEUE'
  | 'CANCEL_QUEUE'
  | 'STEER'
  | 'INTERRUPT'
  | 'ANSWER_QUESTION'
  | 'ANSWER_APPROVAL'
  | 'UPDATE_SETTINGS'
  | 'STOP_BACKGROUND_COMMAND'
  | 'STOP_ALL_BACKGROUND_COMMANDS'
  | 'OPEN_FILE'
  | 'READ_GIT_DIFF'

export interface SelectOption {
  id: string
  label: string
  description?: string
  disabled?: boolean
  disabledReason?: string
}

export interface CapabilitySnapshot {
  revision: number
  operations: Record<ControlOperation, boolean>
  codexVersion?: string
  models: SelectOption[]
  thinkingDepths: SelectOption[]
  serviceTiers: SelectOption[]
  permissionModes: SelectOption[]
  collaborationModes: SelectOption[]
  transferLimits?: TransferLimits
}

export interface TransferLimits {
  textInlineMaxBytes: number
  imageInlineMaxBytes: number
  pdfRangeMaxBytes: number
  downloadMaxBytes: number
  uploadMaxBytes: number
  maxConcurrentPerBrowser: number
  maxConcurrentPerDevice: number
  previewableMimePrefixes: string[]
}

export interface DevicePresence {
  id: string
  displayName: string
  connection: DeviceConnection
  controlMode: ControlMode
  compatibility: CompatibilityState
  lastSeenAt: string
  bridgeVersion: string
  platform: string
  architecture: string
  degradedReason?: string
}

export interface SessionSummary {
  id: string
  nativeSessionId: string
  title: string
  projectDisplay: string
  branch: string
  updatedAt: string
  deviceId: string
  deviceConnection: DeviceConnection
  deviceLastSeenAt?: string
  degradedReason?: string
  controlMode: ControlMode
  compatibility: CompatibilityState
  phase: ActiveTurnPhase
  lastOutcome: LastTurnOutcome
  attentionCount: number
  queueStatus: QueueStatus
  pinned: boolean
  muted: boolean
  archived: boolean
}

export interface AttentionOption {
  id: string
  label: string
  emphasis: 'primary' | 'secondary' | 'danger'
}

export interface AttentionItem {
  id: string
  kind: AttentionKind
  sessionId: string
  turnId: string
  title: string
  description: string
  createdAt: string
  valid: boolean
  requestAction?: string
  risk?: string
  allowFreeText?: boolean
  options: AttentionOption[]
}

export interface PlanStep {
  id: string
  label: string
  status: 'COMPLETED' | 'RUNNING' | 'PENDING'
}

export interface OutputState {
  itemId: string
  revision: number
  text: string
  byteLength: number
  isFinal: boolean
  authority: OutputAuthority
  hasGap: boolean
  loadState?: 'DEFERRED' | 'LOADING' | 'FAILED'
}

export type OutputEvent =
  | { type: 'append'; itemId: string; expectedOffset: number; text: string }
  | { type: 'replace'; itemId: string; revision: number; text: string }
  | { type: 'final'; itemId: string; revision: number; byteLength: number }
  | { type: 'unavailable'; itemId: string; revision: number }

interface TimelineBase {
  id: string
  createdAt: string
}

export type TimelineItem =
  | (TimelineBase & { type: 'commentary'; body: string; author?: string })
  | (TimelineBase & { type: 'plan'; steps: PlanStep[] })
  | (TimelineBase & {
      type: 'command'
      command: string
      cwdDisplay: string
      status: 'RUNNING' | 'COMPLETED' | 'FAILED' | 'STOPPED'
      elapsed: string
      output: OutputState
    })
  | (TimelineBase & {
      type: 'background-command'
      commandId: string
      command: string
      status: BackgroundCommandState
      elapsed: string
    })
  | (TimelineBase & { type: 'attention'; attention: AttentionItem })
  | (TimelineBase & {
      type: 'outcome-unknown'
      requestId: string
      description: string
      output: OutputState
    })

export interface QueueState {
  status: QueueStatus
  afterTurnId?: string
  text?: string
  pausedReason?: string
}

export interface RuntimeSettings {
  model: string
  thinkingDepth: string
  serviceTier: string
  permissionMode: string
  collaborationMode: string
}

export interface RuntimeSnapshot {
  sessionId: string
  runtimeRevision: number
  activeTurnId?: string
  phase: ActiveTurnPhase
  attention: AttentionItem[]
  queue: QueueState
  capabilities: CapabilitySnapshot
  settings: RuntimeSettings
  contextUsed: number
  contextWindow: number
  timeline: TimelineItem[]
  historyNextCursor?: string
  backgroundCommandCount: number
  outputCursors: Array<{
    itemId: string
    revision: number
    byteLength: number
    isFinal: boolean
    finalUnavailable?: boolean
  }>
}

export interface DeviceSummary extends DevicePresence {
  pairedAt: string
  revoked: boolean
  privacyHideTitles: boolean
}

export interface PairingChallenge {
  challengeId: string
  deviceName: string
  platform: string
  architecture: string
  bridgeVersion: string
  expiresAt: string
}

export interface GitStatusEntry {
  relativePath: string
  status: string
  staged: boolean
}

export interface GitSummary {
  branch: string
  detachedHead: boolean
  headShort: string
  headFull: string
  rootDisplayName: string
  entries: GitStatusEntry[]
  insertions: number
  deletions: number
  binaryFiles: string[]
}

export interface GitFileDiff {
  relativePath: string
  staged: boolean
  patchText: string
  truncated: boolean
  totalBytes: number
  binary: boolean
}

export interface FileMetadata {
  displayName: string
  mimeType: string
  sizeBytes: number
  previewKind: 'text' | 'image' | 'pdf' | 'none'
  notPreviewableReason?: string
  fileHandle: string
}

export interface UploadResult {
  transferId: string
  outcome: string
  errorCode?: string
  uploadFileHandle?: string
}

export interface Page<T> {
  items: T[]
  nextCursor?: string
}

export interface CommandRequest {
  requestId: string
  operation: ControlOperation
  sessionId: string
  expectedTurnId?: string
  expectedRuntimeRevision: number
  payload: Record<string, unknown>
}

export type ReceiptStatus =
  | 'RECEIVED'
  | 'ACCEPTED_BY_BRIDGE'
  | 'DISPATCHED_TO_CODEX'
  | 'COMPLETED'
  | 'REJECTED'
  | 'OUTCOME_UNKNOWN'

export interface CommandReceipt {
  requestId: string
  status: ReceiptStatus
  errorCode?: string
}

export type ConsoleEvent =
  | { type: 'connection'; state: DeviceConnection }
  | { type: 'auth-expired' }
  | { type: 'sessions'; sessions: SessionSummary[]; snapshot: boolean }
  | { type: 'runtime-snapshot'; sessionId: string; runtime: RuntimeSnapshot }
  | { type: 'timeline-upsert'; sessionId: string; item: TimelineItem }
  | { type: 'turn-lifecycle'; sessionId: string; phase: ActiveTurnPhase; outcome?: LastTurnOutcome; turnId?: string }
  | { type: 'attention-added'; sessionId: string; attention: AttentionItem }
  | { type: 'attention-removed'; sessionId: string; attentionId: string }
  | { type: 'queue-changed'; sessionId: string; queue: QueueState }
  | { type: 'capabilities-changed'; sessionId: string; capabilities: CapabilitySnapshot }
  | { type: 'device-presence'; deviceId: string; connection: DeviceConnection; lastSeenAt?: string; degradedReason?: string }
  | { type: 'output'; sessionId: string; event: OutputEvent }
  | { type: 'resync-required'; sessionId: string; reason: string }
  | { type: 'receipt'; receipt: CommandReceipt }

export interface ConsoleTransport {
  connect(onEvent: (event: ConsoleEvent) => void): Promise<() => void>
  listSessions(cursor?: string): Promise<Page<SessionSummary>>
  getRuntimeSnapshot(
    sessionId: string,
    options?: { includeHistory?: boolean },
  ): Promise<RuntimeSnapshot>
  getHistory(sessionId: string, cursor?: string): Promise<Page<TimelineItem>>
  getDevices(): Promise<DeviceSummary[]>
  lookupPairing(shortCode: string): Promise<PairingChallenge>
  approvePairing(shortCode: string, challengeId?: string): Promise<void>
  revokeDevice(deviceId: string): Promise<void>
  getGitSummary(sessionId: string): Promise<GitSummary>
  getGitDiff(sessionId: string, path: string, staged?: boolean): Promise<GitFileDiff>
  getFileMetadata(sessionId: string, handle: string): Promise<FileMetadata>
  previewFile(sessionId: string, handle: string, fileName?: string): Promise<Response>
  downloadFile(sessionId: string, handle: string, fileName?: string): Promise<Response>
  uploadFile(sessionId: string, file: File): Promise<UploadResult>
  getOutputText(sessionId: string, itemId: string, cursor?: string): Promise<string>
  requestResync(sessionId: string): void
  sendCommand(request: CommandRequest): Promise<CommandReceipt>
}
