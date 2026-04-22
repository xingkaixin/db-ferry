import { apiPut } from './client';

export async function fetchConfig(): Promise<string> {
  const res = await fetch('/api/config', {
    headers: { Authorization: 'Basic ' + btoa((localStorage.getItem('db-ferry-user') || 'admin') + ':' + (localStorage.getItem('db-ferry-pass') || 'admin')) },
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.text();
}

export async function saveConfig(toml: string): Promise<void> {
  return apiPut('/api/config', toml);
}

export async function validateConfig(toml: string): Promise<{ valid: string; error?: string }> {
  const res = await fetch('/api/config/validate', {
    method: 'POST',
    headers: {
      Authorization: 'Basic ' + btoa((localStorage.getItem('db-ferry-user') || 'admin') + ':' + (localStorage.getItem('db-ferry-pass') || 'admin')),
      'Content-Type': 'text/plain',
    },
    body: toml,
  });
  return res.json();
}
