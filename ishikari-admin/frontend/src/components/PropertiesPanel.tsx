import { useState, useEffect } from 'react';
import type { Node } from '@xyflow/react';
import type { FlowNodeData, SchemaField } from '../types/workflow';
import { useNodeTypes } from '../hooks/useNodeTypes';

type FlowNode = Node<FlowNodeData>;

interface PropertiesPanelProps {
  node: FlowNode | null;
  nodes: FlowNode[];
  workflowInputs: Record<string, SchemaField>;
  onUpdate: (nodeId: string, data: Partial<FlowNodeData>) => void;
  onDelete: (nodeId: string) => void;
}

function ExpressionHelper({
  workflowInputs,
  nodes,
  currentNodeId,
  onInsert,
}: {
  workflowInputs: Record<string, SchemaField>;
  nodes: FlowNode[];
  currentNodeId: string;
  onInsert: (expr: string) => void;
}) {
  const { nodeTypes } = useNodeTypes();
  const [expanded, setExpanded] = useState(false);

  // Get other nodes (not the current one)
  const otherNodes = nodes.filter((n) => n.id !== currentNodeId);

  // Get outputs for each node
  const getNodeOutputs = (node: FlowNode): string[] => {
    const nodeTypeInfo = nodeTypes.find((nt) => nt.name === node.data.nodeType);
    return nodeTypeInfo?.output_schema
      ? Object.keys(nodeTypeInfo.output_schema)
      : ['result'];
  };

  const hasInputs = Object.keys(workflowInputs).length > 0;
  const hasNodes = otherNodes.length > 0;

  if (!hasInputs && !hasNodes) {
    return null;
  }

  return (
    <div className="mt-2">
      <button
        onClick={() => setExpanded(!expanded)}
        className="text-xs text-purple-400 hover:text-purple-300 flex items-center gap-1"
      >
        <svg
          className={`w-3 h-3 transition-transform ${expanded ? 'rotate-90' : ''}`}
          fill="none"
          stroke="currentColor"
          viewBox="0 0 24 24"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
            d="M9 5l7 7-7 7"
          />
        </svg>
        Insert expression
      </button>

      {expanded && (
        <div className="mt-2 p-2 bg-bg-primary rounded border border-border text-xs space-y-2">
          {hasInputs && (
            <div>
              <div className="text-text-muted mb-1">Workflow Inputs</div>
              <div className="flex flex-wrap gap-1">
                {Object.keys(workflowInputs).map((name) => (
                  <button
                    key={name}
                    onClick={() => onInsert(`{{inputs.${name}}}`)}
                    className="px-2 py-0.5 bg-blue-900/50 text-blue-300 rounded hover:bg-blue-800/50 transition-colors"
                  >
                    inputs.{name}
                  </button>
                ))}
              </div>
            </div>
          )}

          {hasNodes && (
            <div>
              <div className="text-text-muted mb-1">Node Outputs</div>
              <div className="space-y-1">
                {otherNodes.map((node) => (
                  <div key={node.id} className="flex flex-wrap gap-1">
                    <span className="text-text-secondary">{node.id}:</span>
                    {getNodeOutputs(node).map((output) => (
                      <button
                        key={`${node.id}.${output}`}
                        onClick={() => onInsert(`{{nodes.${node.id}.${output}}}`)}
                        className="px-2 py-0.5 bg-green-900/50 text-green-300 rounded hover:bg-green-800/50 transition-colors"
                      >
                        {output}
                      </button>
                    ))}
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function InputField({
  name,
  schema,
  value,
  onChange,
  workflowInputs,
  nodes,
  currentNodeId,
}: {
  name: string;
  schema: SchemaField;
  value: unknown;
  onChange: (value: unknown) => void;
  workflowInputs: Record<string, SchemaField>;
  nodes: FlowNode[];
  currentNodeId: string;
}) {
  const isExpression =
    typeof value === 'string' && (value.includes('{{inputs.') || value.includes('{{nodes.'));

  const insertExpression = (expr: string) => {
    const currentValue = String(value ?? '');
    onChange(currentValue + expr);
  };

  // Handle different field types
  switch (schema.type) {
    case 'boolean':
      return (
        <label className="flex items-center gap-2">
          <input
            type="checkbox"
            checked={Boolean(value)}
            onChange={(e) => onChange(e.target.checked)}
            className="rounded bg-bg-tertiary border-border text-accent focus:ring-accent"
          />
          <span className="text-sm text-text-secondary">{name}</span>
        </label>
      );

    case 'integer':
    case 'number':
      return (
        <div>
          <input
            type={isExpression ? 'text' : 'number'}
            value={isExpression ? String(value) : (typeof value === 'number' ? value : '')}
            onChange={(e) =>
              isExpression || e.target.value.includes('{{')
                ? onChange(e.target.value)
                : onChange(Number(e.target.value))
            }
            placeholder={schema.description || name}
            className={`w-full px-3 py-2 bg-bg-tertiary border rounded text-sm text-text-primary placeholder-text-muted focus:outline-none focus:border-accent ${
              isExpression ? 'border-purple-500' : 'border-border'
            }`}
          />
          <ExpressionHelper
            workflowInputs={workflowInputs}
            nodes={nodes}
            currentNodeId={currentNodeId}
            onInsert={insertExpression}
          />
        </div>
      );

    case 'object':
    case 'array':
      return (
        <div>
          <textarea
            value={
              typeof value === 'string' ? value : JSON.stringify(value ?? {}, null, 2)
            }
            onChange={(e) => {
              try {
                onChange(JSON.parse(e.target.value));
              } catch {
                // Keep raw string if not valid JSON (might be an expression)
                onChange(e.target.value);
              }
            }}
            placeholder={schema.description || `${name} (JSON)`}
            rows={3}
            className={`w-full px-3 py-2 bg-bg-tertiary border rounded text-sm text-text-primary placeholder-text-muted focus:outline-none focus:border-accent font-mono ${
              isExpression ? 'border-purple-500' : 'border-border'
            }`}
          />
          <ExpressionHelper
            workflowInputs={workflowInputs}
            nodes={nodes}
            currentNodeId={currentNodeId}
            onInsert={insertExpression}
          />
        </div>
      );

    default:
      return (
        <div>
          <div className="relative">
            <input
              type="text"
              value={String(value ?? '')}
              onChange={(e) => onChange(e.target.value)}
              placeholder={schema.description || name}
              className={`w-full px-3 py-2 bg-bg-tertiary border rounded text-sm text-text-primary placeholder-text-muted focus:outline-none focus:border-accent ${
                isExpression ? 'border-purple-500 pr-12' : 'border-border'
              }`}
            />
            {isExpression && (
              <span className="absolute right-2 top-1/2 -translate-y-1/2 text-xs text-purple-400 bg-purple-900/50 px-1.5 py-0.5 rounded">
                expr
              </span>
            )}
          </div>
          <ExpressionHelper
            workflowInputs={workflowInputs}
            nodes={nodes}
            currentNodeId={currentNodeId}
            onInsert={insertExpression}
          />
        </div>
      );
  }
}

export default function PropertiesPanel({
  node,
  nodes,
  workflowInputs,
  onUpdate,
  onDelete,
}: PropertiesPanelProps) {
  const { nodeTypes } = useNodeTypes();
  const [localInputs, setLocalInputs] = useState<Record<string, unknown>>({});
  const [condition, setCondition] = useState('');
  const [timeout, setTimeout] = useState<number | undefined>();
  const [maxRetries, setMaxRetries] = useState<number | undefined>();
  const [nodeLabel, setNodeLabel] = useState('');

  // Find the schema for the current node type
  const nodeTypeInfo = nodeTypes.find((nt) => nt.name === node?.data.nodeType);

  // Sync local state when node changes
  useEffect(() => {
    if (node) {
      setLocalInputs(node.data.inputs || {});
      setCondition(node.data.condition || '');
      setTimeout(node.data.timeout);
      setMaxRetries(node.data.maxRetries);
      setNodeLabel(node.data.label);
    }
  }, [node]);

  if (!node) {
    return (
      <div className="h-full flex flex-col">
        <div className="p-4 border-b border-border">
          <h2 className="text-lg font-semibold text-text-primary">Properties</h2>
        </div>
        <div className="flex-1 flex items-center justify-center p-4">
          <p className="text-text-muted text-sm text-center">
            Select a node to view and edit its properties.
          </p>
        </div>

        {/* Expression reference guide */}
        <div className="p-4 border-t border-border">
          <h3 className="text-sm font-semibold text-text-primary mb-2">
            Expression Syntax
          </h3>
          <div className="text-xs text-text-secondary space-y-1">
            <p>
              <code className="text-blue-400">{`{{inputs.name}}`}</code> - Workflow input
            </p>
            <p>
              <code className="text-green-400">{`{{nodes.id.output}}`}</code> - Node output
            </p>
          </div>
        </div>
      </div>
    );
  }

  const handleInputChange = (name: string, value: unknown) => {
    const newInputs = { ...localInputs, [name]: value };
    setLocalInputs(newInputs);
    onUpdate(node.id, { inputs: newInputs });
  };

  const handleLabelChange = (value: string) => {
    setNodeLabel(value);
    onUpdate(node.id, { label: value });
  };

  const handleConditionChange = (value: string) => {
    setCondition(value);
    onUpdate(node.id, { condition: value || undefined });
  };

  const handleTimeoutChange = (value: number | undefined) => {
    setTimeout(value);
    onUpdate(node.id, { timeout: value });
  };

  const handleMaxRetriesChange = (value: number | undefined) => {
    setMaxRetries(value);
    onUpdate(node.id, { maxRetries: value });
  };

  return (
    <div className="h-full overflow-y-auto">
      <div className="p-4 border-b border-border">
        <div className="flex items-center justify-between mb-2">
          <h2 className="text-lg font-semibold text-text-primary">Properties</h2>
          <button
            onClick={() => onDelete(node.id)}
            className="px-2 py-1 text-xs text-red-400 hover:text-red-300 hover:bg-red-900/30 rounded transition-colors"
          >
            Delete
          </button>
        </div>
        <div className="text-sm text-text-secondary">{node.data.nodeType}</div>
      </div>

      <div className="p-4 space-y-4">
        {/* Node ID/Label */}
        <div>
          <label className="block text-sm font-medium text-text-secondary mb-1">
            Node ID
          </label>
          <input
            type="text"
            value={nodeLabel}
            onChange={(e) => handleLabelChange(e.target.value)}
            className="w-full px-3 py-2 bg-bg-tertiary border border-border rounded text-sm text-text-primary focus:outline-none focus:border-accent"
          />
          <p className="text-xs text-text-muted mt-1">
            Used to reference this node's outputs
          </p>
        </div>

        {/* Input fields based on schema */}
        {nodeTypeInfo?.input_schema &&
          Object.keys(nodeTypeInfo.input_schema).length > 0 && (
            <div>
              <h3 className="text-sm font-semibold text-text-primary mb-2">
                Inputs
              </h3>
              <div className="space-y-3">
                {Object.entries(nodeTypeInfo.input_schema).map(
                  ([name, schema]) => (
                    <div key={name}>
                      <label className="block text-sm font-medium text-text-secondary mb-1">
                        {name}
                        {schema.required && (
                          <span className="text-red-400 ml-1">*</span>
                        )}
                      </label>
                      <InputField
                        name={name}
                        schema={schema}
                        value={localInputs[name]}
                        onChange={(value) => handleInputChange(name, value)}
                        workflowInputs={workflowInputs}
                        nodes={nodes}
                        currentNodeId={node.id}
                      />
                      {schema.description && (
                        <p className="text-xs text-text-muted mt-1">
                          {schema.description}
                        </p>
                      )}
                    </div>
                  )
                )}
              </div>
            </div>
          )}

        {/* Advanced settings */}
        <div className="pt-4 border-t border-border">
          <h3 className="text-sm font-semibold text-text-primary mb-2">
            Advanced
          </h3>
          <div className="space-y-3">
            {/* Condition */}
            <div>
              <label className="block text-sm font-medium text-text-secondary mb-1">
                Condition (when)
              </label>
              <input
                type="text"
                value={condition}
                onChange={(e) => handleConditionChange(e.target.value)}
                placeholder="{{nodes.previous.result}} == 'success'"
                className="w-full px-3 py-2 bg-bg-tertiary border border-border rounded text-sm text-text-primary placeholder-text-muted focus:outline-none focus:border-accent font-mono"
              />
              <p className="text-xs text-text-muted mt-1">
                Expression that must be true for node to execute
              </p>
            </div>

            {/* Timeout */}
            <div>
              <label className="block text-sm font-medium text-text-secondary mb-1">
                Timeout (seconds)
              </label>
              <input
                type="number"
                value={timeout ?? ''}
                onChange={(e) =>
                  handleTimeoutChange(
                    e.target.value ? Number(e.target.value) : undefined
                  )
                }
                placeholder="No timeout"
                min={1}
                className="w-full px-3 py-2 bg-bg-tertiary border border-border rounded text-sm text-text-primary placeholder-text-muted focus:outline-none focus:border-accent"
              />
            </div>

            {/* Max retries */}
            <div>
              <label className="block text-sm font-medium text-text-secondary mb-1">
                Max Retries
              </label>
              <input
                type="number"
                value={maxRetries ?? ''}
                onChange={(e) =>
                  handleMaxRetriesChange(
                    e.target.value ? Number(e.target.value) : undefined
                  )
                }
                placeholder="Default (3)"
                min={0}
                className="w-full px-3 py-2 bg-bg-tertiary border border-border rounded text-sm text-text-primary placeholder-text-muted focus:outline-none focus:border-accent"
              />
            </div>
          </div>
        </div>

        {/* Output schema info */}
        {nodeTypeInfo?.output_schema &&
          Object.keys(nodeTypeInfo.output_schema).length > 0 && (
            <div className="pt-4 border-t border-border">
              <h3 className="text-sm font-semibold text-text-primary mb-2">
                Outputs
              </h3>
              <p className="text-xs text-text-muted mb-2">
                Reference these in other nodes or workflow outputs:
              </p>
              <div className="space-y-1">
                {Object.entries(nodeTypeInfo.output_schema).map(
                  ([name, schema]) => (
                    <div
                      key={name}
                      className="flex items-center justify-between text-sm p-2 bg-bg-tertiary rounded"
                    >
                      <code className="text-green-400 text-xs">
                        {`{{nodes.${node.data.label}.${name}}}`}
                      </code>
                      <span className="text-text-muted text-xs">{schema.type}</span>
                    </div>
                  )
                )}
              </div>
            </div>
          )}
      </div>
    </div>
  );
}
