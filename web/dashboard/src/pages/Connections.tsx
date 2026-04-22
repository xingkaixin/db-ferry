import { useState } from 'react';
import { useQuery, useMutation } from '@tanstack/react-query';
import { fetchDatabases, testConnection, fetchTables, fetchTableSchema } from '../api/connections';
import type { TableSchema, DatabaseConfig } from '../types';

function SchemaView({ schema }: { schema: TableSchema }) {
  return (
    <div className="space-y-4">
      <div>
        <h4 className="font-semibold text-sm mb-2">Columns</h4>
        <table className="w-full text-sm">
          <thead className="text-text-muted border-b border-border">
            <tr>
              <th className="text-left py-1">Name</th>
              <th className="text-left py-1">Type</th>
              <th className="text-left py-1">Nullable</th>
            </tr>
          </thead>
          <tbody>
            {schema.columns.map((col) => (
              <tr key={col.name} className="border-b border-border/50">
                <td className="py-1">{col.name}</td>
                <td className="py-1 text-text-secondary">{col.database_type}</td>
                <td className="py-1 text-text-secondary">{col.nullable ? 'Yes' : 'No'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {schema.primary_key.length > 0 && (
        <div>
          <h4 className="font-semibold text-sm mb-1">Primary Key</h4>
          <div className="text-text-secondary text-sm">{schema.primary_key.join(', ')}</div>
        </div>
      )}
      {schema.indexes.length > 0 && (
        <div>
          <h4 className="font-semibold text-sm mb-2">Indexes</h4>
          <div className="space-y-1">
            {schema.indexes.map((idx) => (
              <div key={idx.name} className="text-sm">
                <span className="text-text-secondary">{idx.name}</span>
                <span className="text-text-muted ml-2">({idx.columns.join(', ')})</span>
                {idx.unique && <span className="text-accent ml-2 text-xs">UNIQUE</span>}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

export default function Connections() {
  const [expandedDb, setExpandedDb] = useState<string | null>(null);
  const [selectedTable, setSelectedTable] = useState<string | null>(null);
  const [schema, setSchema] = useState<TableSchema | null>(null);

  const { data: databases, isLoading } = useQuery({
    queryKey: ['databases'],
    queryFn: fetchDatabases,
  });

  const testMutation = useMutation({
    mutationFn: testConnection,
  });

  const tablesQuery = useQuery({
    queryKey: ['tables', expandedDb],
    queryFn: () => fetchTables(expandedDb!),
    enabled: !!expandedDb,
  });

  const handleViewSchema = async (dbName: string, table: string) => {
    setSelectedTable(table);
    const data = await fetchTableSchema(dbName, table);
    setSchema(data);
  };

  if (isLoading) {
    return <div className="text-text-secondary">Loading databases...</div>;
  }

  return (
    <div className="space-y-6">
      <h2 className="text-2xl font-bold">Connections</h2>

      <div className="grid gap-4">
        {(databases || []).map((db: DatabaseConfig) => (
          <div key={db.name} className="border border-border rounded-lg bg-bg-secondary">
            <div
              className="p-4 flex items-center justify-between cursor-pointer"
              onClick={() => setExpandedDb(expandedDb === db.name ? null : db.name)}
            >
              <div className="flex items-center gap-3">
                <span className="font-semibold">{db.name}</span>
                <span className="text-xs text-text-muted bg-bg-tertiary px-2 py-0.5 rounded">{db.type}</span>
                <span className="text-xs text-text-muted">{db.host}{db.port ? `:${db.port}` : ''}</span>
              </div>
              <div className="flex items-center gap-2">
                <button
                  onClick={(e) => {
                    e.stopPropagation();
                    testMutation.mutate(db.name);
                  }}
                  className="px-3 py-1 text-xs border border-border rounded hover:bg-bg-tertiary transition-colors"
                >
                  {testMutation.variables === db.name && testMutation.isPending ? 'Testing...' : 'Test'}
                </button>
                <span className="text-text-muted text-lg">{expandedDb === db.name ? '▼' : '▶'}</span>
              </div>
            </div>

            {testMutation.variables === db.name && testMutation.data && (
              <div className={`px-4 pb-2 text-sm ${testMutation.data.status === 'ok' ? 'text-success' : 'text-danger'}`}>
                {testMutation.data.status === 'ok' ? 'Connected' : testMutation.data.message}
              </div>
            )}

            {expandedDb === db.name && (
              <div className="border-t border-border p-4">
                {tablesQuery.isLoading ? (
                  <div className="text-text-secondary text-sm">Loading tables...</div>
                ) : (
                  <div className="space-y-2">
                    <h4 className="text-sm font-medium text-text-muted mb-2">Tables</h4>
                    <div className="grid grid-cols-3 gap-2">
                      {(tablesQuery.data || []).map((table) => (
                        <button
                          key={table}
                          onClick={() => handleViewSchema(db.name, table)}
                          className={`text-left text-sm px-3 py-2 rounded border transition-colors ${
                            selectedTable === table && schema
                              ? 'border-accent bg-accent/5'
                              : 'border-border hover:bg-bg-tertiary'
                          }`}
                        >
                          {table}
                        </button>
                      ))}
                    </div>
                    {(tablesQuery.data || []).length === 0 && (
                      <div className="text-text-secondary text-sm">No tables found</div>
                    )}
                  </div>
                )}

                {schema && selectedTable && (
                  <div className="mt-4 border-t border-border pt-4">
                    <h4 className="font-semibold mb-3">Schema: {selectedTable}</h4>
                    <SchemaView schema={schema} />
                  </div>
                )}
              </div>
            )}
          </div>
        ))}
        {(databases || []).length === 0 && (
          <div className="text-text-secondary text-center py-12">No databases configured</div>
        )}
      </div>
    </div>
  );
}
