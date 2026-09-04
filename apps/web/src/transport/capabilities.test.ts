import { describe, expect, it } from 'vitest'

import { onlineCapabilities } from '@/fixtures/console-fixture'

import { getOperationAvailability } from './capabilities'

describe('getOperationAvailability', () => {
  it('enables an advertised operation for a verified online owner', () => {
    expect(
      getOperationAvailability(
        'SET_QUEUE',
        'ONLINE',
        'FULL_CONTROL',
        'VERIFIED',
        onlineCapabilities,
      ),
    ).toEqual({ enabled: true })
  })

  it('blocks every control operation while the device is offline', () => {
    expect(
      getOperationAvailability(
        'ANSWER_APPROVAL',
        'OFFLINE',
        'FULL_CONTROL',
        'VERIFIED',
        onlineCapabilities,
      ),
    ).toMatchObject({ enabled: false, code: 'DEVICE_OFFLINE' })
  })

  it('blocks writes in read-only control mode', () => {
    expect(
      getOperationAvailability(
        'UPDATE_SETTINGS',
        'ONLINE',
        'READ_ONLY',
        'VERIFIED',
        onlineCapabilities,
      ),
    ).toMatchObject({ enabled: false, code: 'CONTROL_READ_ONLY' })
  })

  it('blocks operations omitted by the Desktop owner capability snapshot', () => {
    const capabilities = structuredClone(onlineCapabilities)
    capabilities.operations.STOP_BACKGROUND_COMMAND = false

    expect(
      getOperationAvailability(
        'STOP_BACKGROUND_COMMAND',
        'ONLINE',
        'LIMITED_CONTROL',
        'VERIFIED',
        capabilities,
      ),
    ).toMatchObject({ enabled: false, code: 'CAPABILITY_UNSUPPORTED' })
  })
})
