const FALLBACK_API_BASE = 'https://intern-alog-012-2.onrender.com'
const envBase = (import.meta.env.VITE_API_BASE || '').trim()
const normalizedEnvBase = envBase.replace(/\/+$/, '')

function resolveApiBase() {
  // When hosted on Vercel, force backend to Render unless a valid explicit API base is provided.
  const host = (typeof window !== 'undefined' && window.location?.hostname) || ''
  const runningOnVercel = host.endsWith('.vercel.app')

  if (normalizedEnvBase && normalizedEnvBase !== '/') return normalizedEnvBase
  if (runningOnVercel) return FALLBACK_API_BASE
  return FALLBACK_API_BASE
}

export const API_BASE = resolveApiBase()

function joinUrl(base, path) {
  if (!base) return path
  if (!path) return base
  if (path.startsWith('http://') || path.startsWith('https://')) return path
  if (!path.startsWith('/')) return `${base}/${path}`
  return `${base}${path}`
}

export async function apiFetch(path, options = {}) {
  const url = joinUrl(API_BASE, path)
  return fetch(url, options)
}

