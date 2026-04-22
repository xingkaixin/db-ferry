import { useEffect, useRef } from 'react';
import { useStore } from '../state/store';
import type { TaskProgressData } from '../types';

export function useSSE() {
  const updateTaskState = useStore((s) => s.updateTaskState);
  const setIsConnected = useStore((s) => s.setIsConnected);
  const esRef = useRef<EventSource | null>(null);

  useEffect(() => {
    const user = localStorage.getItem('db-ferry-user') || 'admin';
    const pass = localStorage.getItem('db-ferry-pass') || 'admin';
    const auth = btoa(`${user}:${pass}`);

    const es = new EventSource(`/api/events?auth=${auth}`);
    esRef.current = es;

    es.onopen = () => {
      setIsConnected(true);
    };

    es.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data) as TaskProgressData;
        if (data.task) {
          updateTaskState(data.task, data);
        }
      } catch {
        // ignore parse errors
      }
    };

    es.onerror = () => {
      setIsConnected(false);
    };

    return () => {
      es.close();
      esRef.current = null;
    };
  }, [updateTaskState, setIsConnected]);
}
