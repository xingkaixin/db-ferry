const authHeader = () => {
  const user = localStorage.getItem('db-ferry-user') || 'admin';
  const pass = localStorage.getItem('db-ferry-pass') || 'admin';
  return 'Basic ' + btoa(user + ':' + pass);
};

export async function apiGet<T>(path: string): Promise<T> {
  const res = await fetch(path, {
    headers: { Authorization: authHeader() },
  });
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`HTTP ${res.status}: ${text}`);
  }
  return res.json();
}

export async function apiPost<T>(path: string, body?: unknown): Promise<T> {
  const res = await fetch(path, {
    method: 'POST',
    headers: {
      Authorization: authHeader(),
      'Content-Type': 'application/json',
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`HTTP ${res.status}: ${text}`);
  }
  return res.json();
}

export async function apiPut(path: string, body: string): Promise<void> {
  const res = await fetch(path, {
    method: 'PUT',
    headers: {
      Authorization: authHeader(),
      'Content-Type': 'text/plain',
    },
    body,
  });
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`HTTP ${res.status}: ${text}`);
  }
}
