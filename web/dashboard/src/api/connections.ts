import { apiGet, apiPost } from './client';
import type { DatabaseConfig, TableSchema, IndexInfo } from '../types';

export async function fetchDatabases(): Promise<DatabaseConfig[]> {
  return apiGet<DatabaseConfig[]>('/api/databases');
}

export async function testConnection(name: string): Promise<{ status: string; message: string }> {
  return apiPost<{ status: string; message: string }>(`/api/databases/${encodeURIComponent(name)}/test`);
}

export async function fetchTables(name: string): Promise<string[]> {
  return apiGet<string[]>(`/api/databases/${encodeURIComponent(name)}/tables`);
}

export async function fetchTableSchema(dbName: string, table: string): Promise<TableSchema> {
  return apiGet<TableSchema>(`/api/databases/${encodeURIComponent(dbName)}/tables/${encodeURIComponent(table)}/schema`);
}

export async function fetchTableIndexes(dbName: string, table: string): Promise<IndexInfo[]> {
  return apiGet<IndexInfo[]>(`/api/databases/${encodeURIComponent(dbName)}/tables/${encodeURIComponent(table)}/indexes`);
}
