import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { fetchHistory, compareHistory } from '../api/history';
import type { MigrationRecord } from '../types';

function RecordCard({ rec, selected, onClick }: { rec: MigrationRecord; selected: boolean; onClick: () => void }) {
  return (
    <div
      onClick={onClick}
      className={`border rounded-lg p-4 cursor-pointer transition-colors ${
        selected ? 'border-accent bg-accent/5' : 'border-border bg-bg-secondary hover:bg-bg-tertiary'
      }`}
    >
      <div className="flex items-center justify-between mb-2">
        <span className="font-semibold">{rec.task_name}</span>
        <span className={`text-xs px-2 py-0.5 rounded ${
          rec.error_message ? 'bg-danger/20 text-danger' : 'bg-success/20 text-success'
        }`}>
          {rec.error_message ? 'Failed' : 'Success'}
        </span>
      </div>
      <div className="text-sm text-text-secondary space-y-1">
        <div>{new Date(rec.started_at).toLocaleString()}</div>
        <div>Rows: {rec.rows_processed.toLocaleString()} processed, {rec.rows_failed.toLocaleString()} failed</div>
        <div>Mode: {rec.mode} | Source: {rec.source_db} → Target: {rec.target_db}</div>
        {rec.validation_result && <div>Validation: {rec.validation_result}</div>}
      </div>
    </div>
  );
}

export default function History() {
  const [selectedIds, setSelectedIds] = useState<string[]>([]);
  const [compareData, setCompareData] = useState<{ left: MigrationRecord; right: MigrationRecord } | null>(null);

  const { data: records, isLoading } = useQuery({
    queryKey: ['history'],
    queryFn: () => fetchHistory(50),
  });

  const handleSelect = (id: string) => {
    setSelectedIds((prev) => {
      if (prev.includes(id)) {
        return prev.filter((x) => x !== id);
      }
      if (prev.length >= 2) {
        return [prev[1], id];
      }
      return [...prev, id];
    });
  };

  const handleCompare = async () => {
    if (selectedIds.length !== 2) return;
    const data = await compareHistory(selectedIds[0], selectedIds[1]);
    setCompareData(data);
  };

  if (isLoading) {
    return <div className="text-text-secondary">Loading history...</div>;
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h2 className="text-2xl font-bold">Migration History</h2>
        <button
          onClick={handleCompare}
          disabled={selectedIds.length !== 2}
          className="px-4 py-2 bg-accent text-bg-primary rounded-md text-sm font-medium hover:bg-accent-hover disabled:opacity-50 transition-colors"
        >
          Compare Selected ({selectedIds.length}/2)
        </button>
      </div>

      {compareData && (
        <div className="border border-border rounded-lg p-4 bg-bg-secondary">
          <h3 className="font-semibold mb-3">Comparison</h3>
          <div className="grid grid-cols-2 gap-4 text-sm">
            <div>
              <div className="text-text-muted mb-1">{new Date(compareData.left.started_at).toLocaleString()}</div>
              <div>Rows: {compareData.left.rows_processed.toLocaleString()}</div>
              <div>Failed: {compareData.left.rows_failed.toLocaleString()}</div>
              <div>Validation: {compareData.left.validation_result || '-'}</div>
            </div>
            <div>
              <div className="text-text-muted mb-1">{new Date(compareData.right.started_at).toLocaleString()}</div>
              <div>Rows: {compareData.right.rows_processed.toLocaleString()}</div>
              <div>Failed: {compareData.right.rows_failed.toLocaleString()}</div>
              <div>Validation: {compareData.right.validation_result || '-'}</div>
            </div>
          </div>
          <button
            onClick={() => setCompareData(null)}
            className="mt-3 text-sm text-accent hover:underline"
          >
            Close comparison
          </button>
        </div>
      )}

      <div className="grid gap-3">
        {(records || []).map((rec) => (
          <RecordCard
            key={rec.id}
            rec={rec}
            selected={selectedIds.includes(rec.id)}
            onClick={() => handleSelect(rec.id)}
          />
        ))}
        {(records || []).length === 0 && (
          <div className="text-text-secondary text-center py-12">No migration history found</div>
        )}
      </div>
    </div>
  );
}
