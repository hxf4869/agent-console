import { readonly, ref } from 'vue'

export type Theme = 'dark' | 'light'

const storedTheme = localStorage.getItem('agent-console-theme')
const initialTheme: Theme = storedTheme === 'dark' || storedTheme === 'light' ? storedTheme : 'dark'
const theme = ref<Theme>(initialTheme)

function applyTheme(nextTheme: Theme): void {
  theme.value = nextTheme
  document.documentElement.dataset.theme = nextTheme
  document
    .querySelector('meta[name="theme-color"]')
    ?.setAttribute('content', nextTheme === 'dark' ? '#151716' : '#f4f6f5')
  localStorage.setItem('agent-console-theme', nextTheme)
}

applyTheme(initialTheme)

export function useTheme() {
  return {
    theme: readonly(theme),
    toggleTheme: () => applyTheme(theme.value === 'dark' ? 'light' : 'dark'),
  }
}
