import { apiGet } from './client';
import type { DaemonStatus } from '../types';

export async function fetchDaemonStatus(): Promise<DaemonStatus> {
  return apiGet<DaemonStatus>('/api/daemon/status');
}
