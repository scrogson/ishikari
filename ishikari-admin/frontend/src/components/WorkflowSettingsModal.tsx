import { useState } from 'react';
import type { Node } from '@xyflow/react';
import type { FlowNodeData, SchemaField, WorkflowDefinition } from '../types/workflow';
import { useNodeTypes } from '../hooks/useNodeTypes';

type FlowNode = Node<FlowNodeData>;

interface WorkflowMeta {
  id: number | null;
  name: string;
  description: string;
  version: number;
  inputs: WorkflowDefinition['inputs'];
  outputs: WorkflowDefinition['outputs'];
  metadata: WorkflowDefinition['metadata'];
}

interface WorkflowSettingsModalProps {
  workflowMeta: WorkflowMeta;
  nodes: FlowNode[];
  onUpdate: React.Dispatch<React.SetStateAction<WorkflowMeta>>;
  onClose: () => void;
}

type TabId = 'general' | 'inputs' | 'outputs';

const FIELD_TYPES = ['string', 'number', 'boolean', 'object', 'array'];

export default function WorkflowSettingsModal({
  workflowMeta,
  nodes,
  onUpdate,
  onClose,
}: WorkflowSettingsModalProps) {
  const [activeTab, setActiveTab] = useState<TabId>('general');
  const [newInputName, setNewInputName] = useState('');
  const [newOutputName, setNewOutputName] = useState('');
  const { nodeTypes } = useNodeTypes();

  // Get available node outputs for the outputs tab
  const getNodeOutputs = (): { nodeId: string; outputs: string[] }[] => {
    return nodes.map((node) => {
      const nodeTypeInfo = nodeTypes.find((nt) => nt.name === node.data.nodeType);
      const outputs = nodeTypeInfo?.output_schema
        ? Object.keys(nodeTypeInfo.output_schema)
        : ['result']; // Default output
      return { nodeId: node.id, outputs };
    });
  };

  const addInput = () => {
    if (!newInputName.trim()) return;
    const name = newInputName.trim().toLowerCase().replace(/\s+/g, '_');
    if (workflowMeta.inputs[name]) return; // Already exists

    onUpdate((prev) => ({
      ...prev,
      inputs: {
        ...prev.inputs,
        [name]: { type: 'string', required: false },
      },
    }));
    setNewInputName('');
  };

  const updateInput = (name: string, field: Partial<SchemaField>) => {
    onUpdate((prev) => ({
      ...prev,
      inputs: {
        ...prev.inputs,
        [name]: { ...prev.inputs[name], ...field },
      },
    }));
  };

  const removeInput = (name: string) => {
    onUpdate((prev) => {
      const { [name]: _, ...rest } = prev.inputs;
      return { ...prev, inputs: rest };
    });
  };

  const addOutput = () => {
    if (!newOutputName.trim()) return;
    const name = newOutputName.trim().toLowerCase().replace(/\s+/g, '_');
    if (workflowMeta.outputs[name]) return; // Already exists

    onUpdate((prev) => ({
      ...prev,
      outputs: {
        ...prev.outputs,
        [name]: '',
      },
    }));
    setNewOutputName('');
  };

  const updateOutput = (name: string, value: string) => {
    onUpdate((prev) => ({
      ...prev,
      outputs: {
        ...prev.outputs,
        [name]: value,
      },
    }));
  };

  const removeOutput = (name: string) => {
    onUpdate((prev) => {
      const { [name]: _, ...rest } = prev.outputs;
      return { ...prev, outputs: rest };
    });
  };

  const tabs: { id: TabId; label: string; count?: number }[] = [
    { id: 'general', label: 'General' },
    { id: 'inputs', label: 'Inputs', count: Object.keys(workflowMeta.inputs).length },
    { id: 'outputs', label: 'Outputs', count: Object.keys(workflowMeta.outputs).length },
  ];

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
      <div className="bg-bg-secondary rounded-lg shadow-xl w-full max-w-2xl mx-4 max-h-[80vh] flex flex-col">
        {/* Header */}
        <div className="px-6 py-4 border-b border-border shrink-0">
          <h2 className="text-lg font-semibold text-text-primary">
            Workflow Settings
          </h2>
        </div>

        {/* Tabs */}
        <div className="px-6 border-b border-border shrink-0">
          <div className="flex gap-4">
            {tabs.map((tab) => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`py-3 text-sm font-medium border-b-2 transition-colors ${
                  activeTab === tab.id
                    ? 'border-accent text-accent'
                    : 'border-transparent text-text-secondary hover:text-text-primary'
                }`}
              >
                {tab.label}
                {tab.count !== undefined && tab.count > 0 && (
                  <span className="ml-1.5 px-1.5 py-0.5 text-xs bg-bg-tertiary rounded">
                    {tab.count}
                  </span>
                )}
              </button>
            ))}
          </div>
        </div>

        {/* Content */}
        <div className="p-6 overflow-y-auto flex-1">
          {activeTab === 'general' && (
            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-text-secondary mb-1">
                  Name
                </label>
                <input
                  type="text"
                  value={workflowMeta.name}
                  onChange={(e) =>
                    onUpdate((prev) => ({ ...prev, name: e.target.value }))
                  }
                  className="w-full px-3 py-2 bg-bg-tertiary border border-border rounded text-text-primary focus:outline-none focus:border-accent"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-text-secondary mb-1">
                  Description
                </label>
                <textarea
                  value={workflowMeta.description}
                  onChange={(e) =>
                    onUpdate((prev) => ({
                      ...prev,
                      description: e.target.value,
                    }))
                  }
                  rows={3}
                  className="w-full px-3 py-2 bg-bg-tertiary border border-border rounded text-text-primary focus:outline-none focus:border-accent"
                />
              </div>
            </div>
          )}

          {activeTab === 'inputs' && (
            <div className="space-y-4">
              <p className="text-sm text-text-secondary">
                Define the inputs your workflow accepts. These can be referenced in node
                inputs using <code className="text-purple-400">{'{{'+'inputs.name}}'}</code>.
              </p>

              {/* Existing inputs */}
              {Object.entries(workflowMeta.inputs).map(([name, schema]) => (
                <div
                  key={name}
                  className="p-4 bg-bg-tertiary rounded-lg border border-border"
                >
                  <div className="flex items-start justify-between gap-4">
                    <div className="flex-1 space-y-3">
                      <div className="flex items-center gap-2">
                        <code className="text-purple-400 text-sm">{name}</code>
                        {schema.required && (
                          <span className="text-xs text-red-400">required</span>
                        )}
                      </div>

                      <div className="grid grid-cols-2 gap-3">
                        <div>
                          <label className="block text-xs text-text-muted mb-1">
                            Type
                          </label>
                          <select
                            value={schema.type}
                            onChange={(e) =>
                              updateInput(name, { type: e.target.value })
                            }
                            className="w-full px-2 py-1.5 bg-bg-secondary border border-border rounded text-sm text-text-primary focus:outline-none focus:border-accent"
                          >
                            {FIELD_TYPES.map((t) => (
                              <option key={t} value={t}>
                                {t}
                              </option>
                            ))}
                          </select>
                        </div>
                        <div>
                          <label className="block text-xs text-text-muted mb-1">
                            Default
                          </label>
                          <input
                            type="text"
                            value={String(schema.default ?? '')}
                            onChange={(e) =>
                              updateInput(name, {
                                default: e.target.value || undefined,
                              })
                            }
                            placeholder="No default"
                            className="w-full px-2 py-1.5 bg-bg-secondary border border-border rounded text-sm text-text-primary placeholder-text-muted focus:outline-none focus:border-accent"
                          />
                        </div>
                      </div>

                      <div>
                        <label className="block text-xs text-text-muted mb-1">
                          Description
                        </label>
                        <input
                          type="text"
                          value={schema.description ?? ''}
                          onChange={(e) =>
                            updateInput(name, {
                              description: e.target.value || undefined,
                            })
                          }
                          placeholder="Describe this input..."
                          className="w-full px-2 py-1.5 bg-bg-secondary border border-border rounded text-sm text-text-primary placeholder-text-muted focus:outline-none focus:border-accent"
                        />
                      </div>

                      <label className="flex items-center gap-2 text-sm text-text-secondary">
                        <input
                          type="checkbox"
                          checked={schema.required ?? false}
                          onChange={(e) =>
                            updateInput(name, { required: e.target.checked })
                          }
                          className="rounded bg-bg-secondary border-border text-accent focus:ring-accent"
                        />
                        Required
                      </label>
                    </div>

                    <button
                      onClick={() => removeInput(name)}
                      className="p-1 text-red-400 hover:text-red-300 hover:bg-red-900/30 rounded transition-colors"
                      title="Remove input"
                    >
                      <svg
                        className="w-4 h-4"
                        fill="none"
                        stroke="currentColor"
                        viewBox="0 0 24 24"
                      >
                        <path
                          strokeLinecap="round"
                          strokeLinejoin="round"
                          strokeWidth={2}
                          d="M6 18L18 6M6 6l12 12"
                        />
                      </svg>
                    </button>
                  </div>
                </div>
              ))}

              {/* Add new input */}
              <div className="flex gap-2">
                <input
                  type="text"
                  value={newInputName}
                  onChange={(e) => setNewInputName(e.target.value)}
                  onKeyDown={(e) => e.key === 'Enter' && addInput()}
                  placeholder="New input name..."
                  className="flex-1 px-3 py-2 bg-bg-tertiary border border-border rounded text-sm text-text-primary placeholder-text-muted focus:outline-none focus:border-accent"
                />
                <button
                  onClick={addInput}
                  disabled={!newInputName.trim()}
                  className="px-4 py-2 text-sm bg-accent hover:bg-accent-hover text-white rounded transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  Add Input
                </button>
              </div>
            </div>
          )}

          {activeTab === 'outputs' && (
            <div className="space-y-4">
              <p className="text-sm text-text-secondary">
                Define what the workflow outputs. Map output names to node results.
              </p>

              {/* Existing outputs */}
              {Object.entries(workflowMeta.outputs).map(([name, value]) => (
                <div
                  key={name}
                  className="p-4 bg-bg-tertiary rounded-lg border border-border"
                >
                  <div className="flex items-start justify-between gap-4">
                    <div className="flex-1 space-y-3">
                      <code className="text-green-400 text-sm">{name}</code>

                      <div>
                        <label className="block text-xs text-text-muted mb-1">
                          Value (node output reference)
                        </label>
                        <select
                          value={value}
                          onChange={(e) => updateOutput(name, e.target.value)}
                          className="w-full px-2 py-1.5 bg-bg-secondary border border-border rounded text-sm text-text-primary focus:outline-none focus:border-accent"
                        >
                          <option value="">Select a node output...</option>
                          {getNodeOutputs().map(({ nodeId, outputs }) =>
                            outputs.map((output) => (
                              <option
                                key={`${nodeId}.${output}`}
                                value={`{{nodes.${nodeId}.${output}}}`}
                              >
                                {nodeId}.{output}
                              </option>
                            ))
                          )}
                        </select>
                        {value && (
                          <code className="block mt-1 text-xs text-purple-400">
                            {value}
                          </code>
                        )}
                      </div>
                    </div>

                    <button
                      onClick={() => removeOutput(name)}
                      className="p-1 text-red-400 hover:text-red-300 hover:bg-red-900/30 rounded transition-colors"
                      title="Remove output"
                    >
                      <svg
                        className="w-4 h-4"
                        fill="none"
                        stroke="currentColor"
                        viewBox="0 0 24 24"
                      >
                        <path
                          strokeLinecap="round"
                          strokeLinejoin="round"
                          strokeWidth={2}
                          d="M6 18L18 6M6 6l12 12"
                        />
                      </svg>
                    </button>
                  </div>
                </div>
              ))}

              {/* Add new output */}
              <div className="flex gap-2">
                <input
                  type="text"
                  value={newOutputName}
                  onChange={(e) => setNewOutputName(e.target.value)}
                  onKeyDown={(e) => e.key === 'Enter' && addOutput()}
                  placeholder="New output name..."
                  className="flex-1 px-3 py-2 bg-bg-tertiary border border-border rounded text-sm text-text-primary placeholder-text-muted focus:outline-none focus:border-accent"
                />
                <button
                  onClick={addOutput}
                  disabled={!newOutputName.trim()}
                  className="px-4 py-2 text-sm bg-accent hover:bg-accent-hover text-white rounded transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  Add Output
                </button>
              </div>

              {nodes.length === 0 && (
                <p className="text-sm text-text-muted italic">
                  Add some nodes to your workflow first to map their outputs.
                </p>
              )}
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="px-6 py-4 border-t border-border flex justify-end gap-2 shrink-0">
          <button
            onClick={onClose}
            className="px-4 py-2 text-sm bg-bg-tertiary hover:bg-border text-text-primary rounded transition-colors"
          >
            Close
          </button>
        </div>
      </div>
    </div>
  );
}
