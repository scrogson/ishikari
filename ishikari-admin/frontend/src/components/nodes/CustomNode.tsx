import { memo } from 'react';
import { Handle, Position, type NodeProps, type Node } from '@xyflow/react';
import type { FlowNodeData } from '../../types/workflow';

type FlowNode = Node<FlowNodeData>;

// Icon mapping for node types
const nodeIcons: Record<string, string> = {
  echo: '',
  uppercase: '',
  lowercase: '',
  'http/get': '',
  'http/post': '',
  'json/validate': '',
  'json/transform': '',
  'db/query': '',
  'db/insert': '',
  'email/send': '',
  'sms/send': '',
  delay: '',
  condition: '',
};

// Category colors
const categoryColors: Record<string, { bg: string; border: string }> = {
  core: { bg: 'bg-blue-900/50', border: 'border-blue-500' },
  transform: { bg: 'bg-purple-900/50', border: 'border-purple-500' },
  http: { bg: 'bg-green-900/50', border: 'border-green-500' },
  database: { bg: 'bg-amber-900/50', border: 'border-amber-500' },
  notification: { bg: 'bg-pink-900/50', border: 'border-pink-500' },
  control: { bg: 'bg-cyan-900/50', border: 'border-cyan-500' },
};

function getCategoryFromType(type: string): string {
  if (type.startsWith('http/')) return 'http';
  if (type.startsWith('json/')) return 'transform';
  if (type.startsWith('db/')) return 'database';
  if (type.startsWith('email/') || type.startsWith('sms/')) return 'notification';
  if (type === 'condition' || type === 'delay') return 'control';
  return 'core';
}

function CustomNode({ data, selected }: NodeProps<FlowNode>) {
  const category = getCategoryFromType(data.nodeType);
  const colors = categoryColors[category] || categoryColors.core;
  const icon = nodeIcons[data.nodeType] || '';

  return (
    <div
      className={`
        px-4 py-2 rounded-lg border-2 min-w-[180px]
        ${colors.bg} ${selected ? 'border-accent' : colors.border}
        transition-all duration-150 shadow-lg
        ${selected ? 'ring-2 ring-accent/30' : ''}
      `}
    >
      <Handle
        type="target"
        position={Position.Top}
        className="!bg-text-muted !border-bg-secondary"
      />

      <div className="flex items-center gap-2">
        <span className="text-lg">{icon}</span>
        <div className="flex-1 min-w-0">
          <div className="font-medium text-sm text-text-primary truncate">
            {data.label}
          </div>
          <div className="text-xs text-text-secondary truncate">
            {data.nodeType}
          </div>
        </div>
      </div>

      {data.condition && (
        <div className="mt-1 text-xs text-amber-400 truncate flex items-center gap-1">
          <span>Conditional</span>
        </div>
      )}

      <Handle
        type="source"
        position={Position.Bottom}
        className="!bg-text-muted !border-bg-secondary"
      />
    </div>
  );
}

export default memo(CustomNode);
