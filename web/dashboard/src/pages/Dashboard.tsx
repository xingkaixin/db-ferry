import { useQuery, useMutation } from '@tanstack/react-query';
import { useSSE } from '../hooks/useSSE';
import { useStore } from '../state/store';
import { fetchTasks, triggerTask } from '../api/tasks';
import { fetchDaemonStatus } from '../api/daemon';

function StatusBadge({ status }: { status: string }) {
  const colors: Record<string, string> = {
    running: 'bg-info/20 text-info',
    completed: 'bg-success/20 text-success',
    error: 'bg-danger/20 text-danger',
    pending: 'bg-text-muted/20 text-text-muted',
  };
  return (
    <span className={`px-2 py-0.5 rounded text-xs font-medium ${colors[status] || colors.pending}`}>
      {status || 'pending'}
    </span>
  );
}

function ProgressBar({ percentage }: { percentage: number }) {
  return (
    <div className="w-full h-2 bg-bg-tertiary rounded-full overflow-hidden">
      <div
        className="h-full bg-accent transition-all duration-300 rounded-full"
        style={{ width: `${Math.min(percentage, 100)}%` }}
      />
    </div>
  );
}

export default function Dashboard() {
  useSSE();
  const taskStates = useStore((s) => s.taskStates);

  const { data: tasks, isLoading } = useQuery({
    queryKey: ['tasks'],
    queryFn: fetchTasks,
  });

  const { data: daemonStatus } = useQuery({
    queryKey: ['daemon-status'],
    queryFn: fetchDaemonStatus,
    refetchInterval: 5000,
  });

  const triggerMutation = useMutation({
    mutationFn: triggerTask,
  });

  if (isLoading) {
    return <div className="text-text-secondary">Loading tasks...</div>;
  }

  const mergedTasks = (tasks || []).map((t) => {
    const state = taskStates[t.table_name];
    if (state) {
      return {
        ...t,
        processed: state.processed,
        percentage: state.percentage,
        duration_ms: state.duration_ms,
        status: state.error ? 'error' : state.percentage >= 100 ? 'completed' : state.processed > 0 ? 'running' : t.status,
      };
    }
    return t;
  });

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h2 className="text-2xl font-bold">Task Dashboard</h2>
        <div className="flex items-center gap-4">
          <div className="flex items-center gap-2 text-sm text-text-secondary">
            <span className={`w-2 h-2 rounded-full ${daemonStatus?.running ? 'bg-success' : 'bg-danger'}`} />
            Daemon {daemonStatus?.running ? 'running' : 'stopped'}
          </div>
          <button
            onClick={() => triggerMutation.mutate()}
            disabled={triggerMutation.isPending}
            className="px-4 py-2 bg-accent text-bg-primary rounded-md text-sm font-medium hover:bg-accent-hover disabled:opacity-50 transition-colors"
          >
            {triggerMutation.isPending ? 'Triggering...' : 'Trigger Round'}
          </button>
        </div>
      </div>

      <div className="grid gap-4">
        {mergedTasks.map((task) => (
          <div key={task.table_name} className="border border-border rounded-lg p-4 bg-bg-secondary">
            <div className="flex items-center justify-between mb-3">
              <div className="flex items-center gap-3">
                <h3 className="font-semibold text-text-primary">{task.table_name}</h3>
                <StatusBadge status={task.status} />
                <span className="text-xs text-text-muted">{task.mode}</span>
              </div>
              <div className="text-sm text-text-secondary">
                {task.processed > 0 && `${task.processed.toLocaleString()} rows`}
              </div>
            </div>
            <ProgressBar percentage={task.percentage} />
            <div className="flex gap-4 mt-2 text-xs text-text-muted">
              <span>Source: {task.source_db}</span>
              <span>Target: {task.target_db}</span>
              {task.duration_ms > 0 && <span>Duration: {(task.duration_ms / 1000).toFixed(1)}s</span>}
            </div>
            {task.status === 'error' && taskStates[task.table_name]?.error && (
              <div className="mt-2 text-xs text-danger">{taskStates[task.table_name]?.error}</div>
            )}
          </div>
        ))}
        {mergedTasks.length === 0 && (
          <div className="text-text-secondary text-center py-12">No tasks configured</div>
        )}
      </div>
    </div>
  );
}
