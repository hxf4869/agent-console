import { nextTick, ref } from 'vue'

export function useBottomSheetFocus(fallbackFocus?: () => HTMLElement | null, sheetSelector?: string) {
  const sheetOpen = ref(false)
  const sheetElement = ref<HTMLElement | null>(null)
  let triggerElement: HTMLElement | null = null

  async function openSheet(trigger?: EventTarget | null): Promise<void> {
    if (trigger instanceof HTMLElement) triggerElement = trigger
    sheetOpen.value = true
    await nextTick()
    const focusTarget = sheetSelector ? document.querySelector<HTMLElement>(sheetSelector) : sheetElement.value
    focusTarget?.focus({ preventScroll: true })
  }

  async function closeSheet(): Promise<void> {
    if (!sheetOpen.value) return
    sheetOpen.value = false
    await nextTick()
    const focusTarget = triggerElement?.isConnected ? triggerElement : fallbackFocus?.()
    focusTarget?.focus({ preventScroll: true })
  }

  function onSheetKeydown(event: KeyboardEvent): void {
    if (event.key !== 'Escape') return
    event.preventDefault()
    event.stopPropagation()
    void closeSheet()
  }

  return { sheetElement, sheetOpen, openSheet, closeSheet, onSheetKeydown }
}
