import { useCallback, useRef } from 'react';
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  useReactFlow,
  type Node,
  type Edge,
  type OnNodesChange,
  type OnEdgesChange,
  type OnConnect,
  type NodeMouseHandler,
  BackgroundVariant,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';

import { nodeTypes } from './nodes/nodeTypes';
import type { FlowNodeData, NodeTypeInfo } from '../types/workflow';

type FlowNode = Node<FlowNodeData>;

interface CanvasProps {
  nodes: FlowNode[];
  edges: Edge[];
  onNodesChange: OnNodesChange<FlowNode>;
  onEdgesChange: OnEdgesChange;
  onConnect: OnConnect;
  onNodeClick: NodeMouseHandler<FlowNode>;
  onAddNode: (nodeType: string, label: string, position: { x: number; y: number }) => void;
  onPaneClick: () => void;
}

export default function Canvas({
  nodes,
  edges,
  onNodesChange,
  onEdgesChange,
  onConnect,
  onNodeClick,
  onAddNode,
  onPaneClick,
}: CanvasProps) {
  const reactFlowWrapper = useRef<HTMLDivElement>(null);
  const { screenToFlowPosition } = useReactFlow();

  const onDragOver = useCallback((event: React.DragEvent) => {
    event.preventDefault();
    event.dataTransfer.dropEffect = 'move';
  }, []);

  const onDrop = useCallback(
    (event: React.DragEvent) => {
      event.preventDefault();

      const nodeTypeData = event.dataTransfer.getData('application/reactflow');
      if (!nodeTypeData) {
        return;
      }

      try {
        const nodeType: NodeTypeInfo = JSON.parse(nodeTypeData);

        // Convert screen coordinates to flow coordinates (accounting for zoom/pan)
        const position = screenToFlowPosition({
          x: event.clientX,
          y: event.clientY,
        });

        // Offset to center the node on the cursor
        position.x -= 90;
        position.y -= 25;

        onAddNode(nodeType.name, nodeType.name, position);
      } catch {
        console.error('Failed to parse dropped node data');
      }
    },
    [onAddNode, screenToFlowPosition]
  );

  return (
    <div ref={reactFlowWrapper} className="flex-1 h-full">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        onNodeClick={onNodeClick}
        onPaneClick={onPaneClick}
        onDragOver={onDragOver}
        onDrop={onDrop}
        nodeTypes={nodeTypes}
        fitView={nodes.length > 0}
        fitViewOptions={{ padding: 0.3, maxZoom: 1 }}
        minZoom={0.2}
        maxZoom={2}
        defaultViewport={{ x: 100, y: 100, zoom: 1 }}
        defaultEdgeOptions={{
          type: 'smoothstep',
          animated: false,
          style: { stroke: '#64748b', strokeWidth: 2 },
        }}
        connectionLineStyle={{ stroke: '#3b82f6', strokeWidth: 2 }}
        snapToGrid
        snapGrid={[15, 15]}
        deleteKeyCode={['Backspace', 'Delete']}
        className="bg-bg-primary"
      >
        <Background
          variant={BackgroundVariant.Dots}
          gap={20}
          size={1}
          color="#334155"
        />
        <Controls
          showZoom
          showFitView
          showInteractive={false}
          className="!bg-bg-secondary !border-border"
        />
        <MiniMap
          nodeColor={() => '#3b82f6'}
          maskColor="rgba(15, 23, 42, 0.8)"
          className="!bg-bg-secondary !border-border"
        />
      </ReactFlow>
    </div>
  );
}
