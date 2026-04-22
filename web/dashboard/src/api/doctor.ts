import { apiPost } from './client';
import type { CheckResult } from '../types';

export async function runDoctor(): Promise<CheckResult[]> {
  return apiPost<CheckResult[]>('/api/doctor');
}
