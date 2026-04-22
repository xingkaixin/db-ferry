import { apiGet } from './client';
import type { MigrationRecord } from '../types';

export async function fetchHistory(limit = 50): Promise<MigrationRecord[]> {
  return apiGet<MigrationRecord[]>(`/api/history?limit=${limit}`);
}

export async function compareHistory(id1: string, id2: string): Promise<{ left: MigrationRecord; right: MigrationRecord }> {
  return apiGet<{ left: MigrationRecord; right: MigrationRecord }>(`/api/history/compare?id1=${encodeURIComponent(id1)}&id2=${encodeURIComponent(id2)}`);
}
