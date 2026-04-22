import { useState, useCallback } from 'react';
import { useQuery, useMutation } from '@tanstack/react-query';
import CodeMirror from '@uiw/react-codemirror';
import { StreamLanguage } from '@codemirror/stream-parser';
import { toml } from '@codemirror/legacy-modes/mode/toml';
import { fetchConfig, saveConfig, validateConfig } from '../api/config';

export default function ConfigEditor() {
  const [saveMsg, setSaveMsg] = useState('');
  const [validateMsg, setValidateMsg] = useState('');

  const { data: configText, isLoading } = useQuery({
    queryKey: ['config'],
    queryFn: fetchConfig,
  });

  const [editorValue, setEditorValue] = useState(configText || '');

  // Update editor when config loads
  if (configText && editorValue === '' && configText !== '') {
    setEditorValue(configText);
  }

  const saveMutation = useMutation({
    mutationFn: saveConfig,
    onSuccess: () => {
      setSaveMsg('Saved successfully');
      setTimeout(() => setSaveMsg(''), 3000);
    },
    onError: (err: Error) => {
      setSaveMsg(`Error: ${err.message}`);
    },
  });

  const validateMutation = useMutation({
    mutationFn: validateConfig,
    onSuccess: (data) => {
      if (data.valid === 'true') {
        setValidateMsg('Configuration is valid');
      } else {
        setValidateMsg(`Invalid: ${data.error || ''}`);
      }
      setTimeout(() => setValidateMsg(''), 5000);
    },
    onError: (err: Error) => {
      setValidateMsg(`Error: ${err.message}`);
    },
  });

  const handleSave = useCallback(() => {
    saveMutation.mutate(editorValue);
  }, [editorValue, saveMutation]);

  const handleValidate = useCallback(() => {
    validateMutation.mutate(editorValue);
  }, [editorValue, validateMutation]);

  if (isLoading) {
    return <div className="text-text-secondary">Loading config...</div>;
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-2xl font-bold">Configuration Editor</h2>
        <div className="flex items-center gap-3">
          {validateMsg && (
            <span className={`text-sm ${validateMsg.includes('valid') && !validateMsg.includes('Invalid') ? 'text-success' : 'text-danger'}`}>
              {validateMsg}
            </span>
          )}
          {saveMsg && (
            <span className={`text-sm ${saveMsg.includes('Error') ? 'text-danger' : 'text-success'}`}>
              {saveMsg}
            </span>
          )}
          <button
            onClick={handleValidate}
            disabled={validateMutation.isPending}
            className="px-4 py-2 border border-border rounded-md text-sm hover:bg-bg-tertiary transition-colors"
          >
            {validateMutation.isPending ? 'Validating...' : 'Validate'}
          </button>
          <button
            onClick={handleSave}
            disabled={saveMutation.isPending}
            className="px-4 py-2 bg-accent text-bg-primary rounded-md text-sm font-medium hover:bg-accent-hover disabled:opacity-50 transition-colors"
          >
            {saveMutation.isPending ? 'Saving...' : 'Save'}
          </button>
        </div>
      </div>

      <div className="border border-border rounded-lg overflow-hidden">
        <CodeMirror
          value={editorValue}
          height="calc(100vh - 200px)"
          theme="dark"
          extensions={[StreamLanguage.define(toml as any)]}
          onChange={(value) => setEditorValue(value)}
          basicSetup={{
            lineNumbers: true,
            highlightActiveLineGutter: true,
            highlightActiveLine: true,
            foldGutter: true,
          }}
        />
      </div>
    </div>
  );
}
