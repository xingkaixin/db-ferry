import { apiGet, apiPost } from './client';
import type { TaskResponse } from '../types';

export async function fetchTasks(): Promise<TaskResponse[]> {
  return apiGet<TaskResponse[]>('/api/tasks');
}

export async function fetchTask(name: string): Promise<TaskResponse> {
  return apiGet<TaskResponse>(`/api/tasks/${encodeURIComponent(name)}`);
}

export async function triggerTask(): Promise<{ status: string }> {
  return apiPost<{ status: string }>('/api/tasks/trigger');
}
