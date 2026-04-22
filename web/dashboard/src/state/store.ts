import { create } from 'zustand';
import type { TaskProgressData, TaskResponse, DatabaseConfig, DaemonStatus } from '../types';

interface DashboardState {
  tasks: TaskResponse[];
  taskStates: Record<string, TaskProgressData>;
  databases: DatabaseConfig[];
  daemonStatus: DaemonStatus | null;
  isConnected: boolean;
  setTasks: (tasks: TaskResponse[]) => void;
  updateTaskState: (task: string, state: TaskProgressData) => void;
  setDatabases: (dbs: DatabaseConfig[]) => void;
  setDaemonStatus: (status: DaemonStatus) => void;
  setIsConnected: (connected: boolean) => void;
}

export const useStore = create<DashboardState>((set) => ({
  tasks: [],
  taskStates: {},
  databases: [],
  daemonStatus: null,
  isConnected: false,
  setTasks: (tasks) => set({ tasks }),
  updateTaskState: (task, state) =>
    set((s) => ({
      taskStates: { ...s.taskStates, [task]: state },
    })),
  setDatabases: (databases) => set({ databases }),
  setDaemonStatus: (daemonStatus) => set({ daemonStatus }),
  setIsConnected: (isConnected) => set({ isConnected }),
}));
