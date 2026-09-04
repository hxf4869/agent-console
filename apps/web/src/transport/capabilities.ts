import type {
  CapabilitySnapshot,
  CompatibilityState,
  ControlMode,
  ControlOperation,
  DeviceConnection,
} from './types'

export interface OperationAvailability {
  enabled: boolean
  reason?: string
  code?: 'DEVICE_OFFLINE' | 'CONTROL_READ_ONLY' | 'CODEX_VERSION_UNVERIFIED' | 'CAPABILITY_UNSUPPORTED'
}

export function getOperationAvailability(
  operation: ControlOperation,
  connection: DeviceConnection,
  controlMode: ControlMode,
  compatibility: CompatibilityState,
  capabilities: CapabilitySnapshot,
): OperationAvailability {
  if (connection !== 'ONLINE') {
    return {
      enabled: false,
      reason: '设备未在线，当前操作不可提交。',
      code: 'DEVICE_OFFLINE',
    }
  }

  if (controlMode === 'READ_ONLY' || controlMode === 'UNAVAILABLE') {
    return {
      enabled: false,
      reason: '当前兼容性只允许读取，不能控制 Codex Desktop。',
      code: 'CONTROL_READ_ONLY',
    }
  }

  if (compatibility === 'UNSUPPORTED') {
    return {
      enabled: false,
      reason: '当前 Codex Desktop 版本尚未验证此操作。',
      code: 'CODEX_VERSION_UNVERIFIED',
    }
  }

  if (!capabilities.operations[operation]) {
    return {
      enabled: false,
      reason: '当前 Desktop owner 未提供此能力。',
      code: 'CAPABILITY_UNSUPPORTED',
    }
  }

  return { enabled: true }
}
