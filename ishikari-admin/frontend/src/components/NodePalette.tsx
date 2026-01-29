import { useNodeTypes } from '../hooks/useNodeTypes';
import type { NodeTypeInfo } from '../types/workflow';

// Icon mapping for node types
const nodeIcons: Record<string, string> = {
  echo: '💬',
  uppercase: '🔠',
  lowercase: '🔡',
  'http/get': '🌐',
  'http/post': '📤',
  'json/validate': '✅',
  'json/transform': '🔄',
  'db/query': '🔍',
  'db/insert': '➕',
  'email/send': '📧',
  'sms/send': '📱',
  delay: '⏱️',
  condition: '🔀',
};

// Category display names and order
const categoryInfo: Record<string, { label: string; order: number }> = {
  core: { label: 'Core', order: 0 },
  transform: { label: 'Transform', order: 1 },
  http: { label: 'HTTP', order: 2 },
  database: { label: 'Database', order: 3 },
  notification: { label: 'Notifications', order: 4 },
  control: { label: 'Control Flow', order: 5 },
  custom: { label: 'Custom', order: 10 },
};

interface NodePaletteProps {
  onDragStart: (nodeType: NodeTypeInfo, event: React.DragEvent) => void;
}

function NodeItem({
  nodeType,
  onDragStart,
}: {
  nodeType: NodeTypeInfo;
  onDragStart: (nodeType: NodeTypeInfo, event: React.DragEvent) => void;
}) {
  const icon = nodeIcons[nodeType.name] || '⚡';

  return (
    <div
      className="flex items-center gap-2 px-3 py-2 rounded bg-bg-tertiary hover:bg-bg-tertiary/70 cursor-grab active:cursor-grabbing transition-colors"
      draggable
      onDragStart={(e) => onDragStart(nodeType, e)}
    >
      <span className="text-base">{icon}</span>
      <div className="flex-1 min-w-0">
        <div className="text-sm font-medium text-text-primary truncate">
          {nodeType.name}
        </div>
        {nodeType.description && (
          <div className="text-xs text-text-muted truncate">
            {nodeType.description}
          </div>
        )}
      </div>
    </div>
  );
}

export default function NodePalette({ onDragStart }: NodePaletteProps) {
  const { categorized, loading, error } = useNodeTypes();

  if (loading) {
    return (
      <div className="p-4 text-text-secondary text-sm">Loading node types...</div>
    );
  }

  if (error) {
    return (
      <div className="p-4 text-red-400 text-sm">Error: {error}</div>
    );
  }

  // Sort categories by order
  const sortedCategories = Object.entries(categorized).sort((a, b) => {
    const orderA = categoryInfo[a[0]]?.order ?? 99;
    const orderB = categoryInfo[b[0]]?.order ?? 99;
    return orderA - orderB;
  });

  return (
    <div className="h-full overflow-y-auto">
      <div className="p-3">
        <h2 className="text-lg font-semibold text-text-primary mb-3">
          Node Types
        </h2>
        <p className="text-xs text-text-muted mb-4">
          Drag nodes onto the canvas to add them to your workflow.
        </p>

        {sortedCategories.map(([category, nodes]) => (
          <div key={category} className="mb-4">
            <h3 className="text-xs font-semibold uppercase tracking-wider text-text-secondary mb-2">
              {categoryInfo[category]?.label || category}
            </h3>
            <div className="space-y-1">
              {nodes.map((nodeType) => (
                <NodeItem
                  key={nodeType.name}
                  nodeType={nodeType}
                  onDragStart={onDragStart}
                />
              ))}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
