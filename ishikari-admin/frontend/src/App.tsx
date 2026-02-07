import { useCallback, useState } from 'react';
import WorkflowBuilder from './components/WorkflowBuilder';

// Configuration injected by the server-side template
declare global {
  interface Window {
    __WORKFLOW_BUILDER__?: {
      definitionId: number | null;
      isNew: boolean;
      apiBase: string;
    };
  }
}

function App() {
  const config = window.__WORKFLOW_BUILDER__;
  const [definitionId, setDefinitionId] = useState<number | undefined>(
    config?.definitionId ?? undefined
  );

  const handleSaved = useCallback((id: number) => {
    // Update URL if this was a new definition
    if (!definitionId) {
      window.history.replaceState(null, '', `/definitions/${id}/edit`);
      setDefinitionId(id);
    }
  }, [definitionId]);

  return <WorkflowBuilder definitionId={definitionId} onSaved={handleSaved} />;
}

export default App;
