import { useCallback, useState } from 'react';
import type { Node, NodeMouseHandler } from '@xyflow/react';
import { ReactFlowProvider } from '@xyflow/react';

import Canvas from './Canvas';
import NodePalette from './NodePalette';
import PropertiesPanel from './PropertiesPanel';
import WorkflowSettingsModal from './WorkflowSettingsModal';
import { useWorkflow } from '../hooks/useWorkflow';
import type { FlowNodeData, NodeTypeInfo } from '../types/workflow';

type FlowNode = Node<FlowNodeData>;

interface WorkflowBuilderProps {
  definitionId?: number;
  onSaved?: (id: number) => void;
}

function WorkflowBuilderInner({ definitionId, onSaved }: WorkflowBuilderProps) {
  const {
    nodes,
    edges,
    onNodesChange,
    onEdgesChange,
    onConnect,
    selectedNode,
    setSelectedNode,
    workflowMeta,
    setWorkflowMeta,
    loading,
    saving,
    error,
    save,
    addNode,
    updateNodeData,
    deleteNode,
    autoLayout,
  } = useWorkflow(definitionId);

  const [showSettingsModal, setShowSettingsModal] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);

  const handleNodeClick: NodeMouseHandler<FlowNode> = useCallback(
    (_event, node) => {
      setSelectedNode(node);
    },
    [setSelectedNode]
  );

  const handlePaneClick = useCallback(() => {
    setSelectedNode(null);
  }, [setSelectedNode]);

  const handleDragStart = useCallback(
    (nodeType: NodeTypeInfo, event: React.DragEvent) => {
      event.dataTransfer.setData(
        'application/reactflow',
        JSON.stringify(nodeType)
      );
      event.dataTransfer.effectAllowed = 'move';
    },
    []
  );

  const handleSave = useCallback(async () => {
    setSaveError(null);
    try {
      const result = await save();
      if (onSaved) {
        onSaved(result.id);
      }
    } catch (err) {
      setSaveError(err instanceof Error ? err.message : 'Failed to save');
    }
  }, [save, onSaved]);

  if (loading) {
    return (
      <div className="h-screen flex items-center justify-center bg-bg-primary">
        <div className="text-text-secondary">Loading workflow...</div>
      </div>
    );
  }

  // Count inputs/outputs for display
  const inputCount = Object.keys(workflowMeta.inputs).length;
  const outputCount = Object.keys(workflowMeta.outputs).length;

  return (
    <div className="h-screen flex flex-col bg-bg-primary">
      {/* Header */}
      <header className="flex items-center justify-between px-4 py-3 bg-bg-secondary border-b border-border shrink-0">
        <div className="flex items-center gap-4">
          <a
            href="/definitions"
            className="text-text-secondary hover:text-text-primary transition-colors"
          >
            ← Definitions
          </a>
          <div>
            <h1 className="text-lg font-semibold text-text-primary">
              {workflowMeta.name}
              {workflowMeta.id && (
                <span className="ml-2 text-sm text-text-muted">
                  v{workflowMeta.version}
                </span>
              )}
            </h1>
            <div className="flex items-center gap-3 text-sm text-text-secondary">
              {workflowMeta.description && (
                <span>{workflowMeta.description}</span>
              )}
              {inputCount > 0 && (
                <span className="text-xs bg-blue-900/50 text-blue-300 px-2 py-0.5 rounded">
                  {inputCount} input{inputCount !== 1 ? 's' : ''}
                </span>
              )}
              {outputCount > 0 && (
                <span className="text-xs bg-green-900/50 text-green-300 px-2 py-0.5 rounded">
                  {outputCount} output{outputCount !== 1 ? 's' : ''}
                </span>
              )}
            </div>
          </div>
        </div>

        <div className="flex items-center gap-2">
          <button
            onClick={autoLayout}
            className="px-3 py-1.5 text-sm bg-bg-tertiary hover:bg-border text-text-primary rounded transition-colors"
          >
            Auto Layout
          </button>
          <button
            onClick={() => setShowSettingsModal(true)}
            className="px-3 py-1.5 text-sm bg-bg-tertiary hover:bg-border text-text-primary rounded transition-colors"
          >
            Settings
          </button>
          <button
            onClick={handleSave}
            disabled={saving}
            className="px-4 py-1.5 text-sm bg-accent hover:bg-accent-hover text-white rounded transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {saving ? 'Saving...' : 'Save'}
          </button>
        </div>
      </header>

      {/* Error banner */}
      {(error || saveError) && (
        <div className="px-4 py-2 bg-red-900/50 border-b border-red-700 text-red-300 text-sm shrink-0">
          {error || saveError}
        </div>
      )}

      {/* Main content */}
      <div className="flex-1 flex overflow-hidden">
        {/* Left sidebar - Node palette */}
        <aside className="w-64 bg-bg-secondary border-r border-border overflow-hidden shrink-0">
          <NodePalette onDragStart={handleDragStart} />
        </aside>

        {/* Center - Canvas */}
        <Canvas
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          onNodeClick={handleNodeClick}
          onPaneClick={handlePaneClick}
          onAddNode={addNode}
        />

        {/* Right sidebar - Properties */}
        <aside className="w-80 bg-bg-secondary border-l border-border overflow-hidden shrink-0">
          <PropertiesPanel
            node={selectedNode}
            nodes={nodes}
            workflowInputs={workflowMeta.inputs}
            onUpdate={updateNodeData}
            onDelete={deleteNode}
          />
        </aside>
      </div>

      {/* Settings modal */}
      {showSettingsModal && (
        <WorkflowSettingsModal
          workflowMeta={workflowMeta}
          nodes={nodes}
          onUpdate={setWorkflowMeta}
          onClose={() => setShowSettingsModal(false)}
        />
      )}
    </div>
  );
}

export default function WorkflowBuilder(props: WorkflowBuilderProps) {
  return (
    <ReactFlowProvider>
      <WorkflowBuilderInner {...props} />
    </ReactFlowProvider>
  );
}
