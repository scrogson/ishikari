import { useState, useCallback, useEffect } from 'react';
import {
  useNodesState,
  useEdgesState,
  type Node,
  type Edge,
  addEdge,
  type Connection,
  type OnNodesChange,
  type OnEdgesChange,
} from '@xyflow/react';
import dagre from 'dagre';
import type {
  WorkflowDefinition,
  NodeDefinition,
  FlowNodeData,
  DefinitionDetail,
} from '../types/workflow';
import { fetchDefinition, saveDefinition, createDefinition } from '../api/client';

const NODE_WIDTH = 200;
const NODE_HEIGHT = 60;

type FlowNode = Node<FlowNodeData>;

// Auto-layout using dagre
function computeLayout(nodes: FlowNode[], edges: Edge[]): FlowNode[] {
  const g = new dagre.graphlib.Graph();
  g.setDefaultEdgeLabel(() => ({}));
  g.setGraph({ rankdir: 'TB', nodesep: 50, ranksep: 80 });

  nodes.forEach((node) => {
    g.setNode(node.id, { width: NODE_WIDTH, height: NODE_HEIGHT });
  });

  edges.forEach((edge) => {
    g.setEdge(edge.source, edge.target);
  });

  dagre.layout(g);

  return nodes.map((node) => {
    const nodeWithPosition = g.node(node.id);
    return {
      ...node,
      position: {
        x: nodeWithPosition.x - NODE_WIDTH / 2,
        y: nodeWithPosition.y - NODE_HEIGHT / 2,
      },
    };
  });
}

// Convert backend WorkflowDefinition to xyflow format
function definitionToFlow(def: WorkflowDefinition): {
  nodes: FlowNode[];
  edges: Edge[];
} {
  const nodes: FlowNode[] = [];
  const edges: Edge[] = [];

  for (const [nodeId, nodeDef] of Object.entries(def.nodes)) {
    nodes.push({
      id: nodeId,
      type: 'workflow',
      position: { x: 0, y: 0 }, // Will be laid out by dagre
      data: {
        nodeType: nodeDef.type,
        label: nodeId,
        inputs: nodeDef.inputs || {},
        condition: nodeDef.when,
        timeout: nodeDef.timeout_seconds,
        maxRetries: nodeDef.max_retries,
      },
    });

    for (const depId of nodeDef.depends_on || []) {
      edges.push({
        id: `${depId}->${nodeId}`,
        source: depId,
        target: nodeId,
      });
    }
  }

  // Apply auto-layout
  const layoutedNodes = computeLayout(nodes, edges);

  return { nodes: layoutedNodes, edges };
}

// Convert xyflow format to backend WorkflowDefinition
function flowToDefinition(
  nodes: FlowNode[],
  edges: Edge[],
  meta: {
    name: string;
    description?: string;
    version: number;
    inputs: WorkflowDefinition['inputs'];
    outputs: WorkflowDefinition['outputs'];
    metadata: WorkflowDefinition['metadata'];
  }
): WorkflowDefinition {
  const nodeDefs: Record<string, NodeDefinition> = {};

  for (const node of nodes) {
    const deps = edges.filter((e) => e.target === node.id).map((e) => e.source);

    nodeDefs[node.id] = {
      type: node.data.nodeType,
      depends_on: deps,
      inputs: node.data.inputs,
      when: node.data.condition,
      timeout_seconds: node.data.timeout,
      max_retries: node.data.maxRetries,
    };
  }

  return {
    name: meta.name,
    description: meta.description,
    version: meta.version,
    inputs: meta.inputs,
    nodes: nodeDefs,
    outputs: meta.outputs,
    metadata: meta.metadata,
  };
}

interface WorkflowMeta {
  id: number | null;
  name: string;
  description: string;
  version: number;
  inputs: WorkflowDefinition['inputs'];
  outputs: WorkflowDefinition['outputs'];
  metadata: WorkflowDefinition['metadata'];
}

interface UseWorkflowResult {
  nodes: FlowNode[];
  edges: Edge[];
  setNodes: React.Dispatch<React.SetStateAction<FlowNode[]>>;
  setEdges: React.Dispatch<React.SetStateAction<Edge[]>>;
  onNodesChange: OnNodesChange<FlowNode>;
  onEdgesChange: OnEdgesChange;
  onConnect: (connection: Connection) => void;
  selectedNode: FlowNode | null;
  setSelectedNode: (node: FlowNode | null) => void;
  workflowMeta: WorkflowMeta;
  setWorkflowMeta: React.Dispatch<React.SetStateAction<WorkflowMeta>>;
  loading: boolean;
  saving: boolean;
  error: string | null;
  load: (id: number) => Promise<void>;
  save: () => Promise<DefinitionDetail>;
  addNode: (
    nodeType: string,
    label: string,
    position: { x: number; y: number }
  ) => void;
  updateNodeData: (nodeId: string, data: Partial<FlowNodeData>) => void;
  deleteNode: (nodeId: string) => void;
  autoLayout: () => void;
  toDefinition: () => WorkflowDefinition;
}

export function useWorkflow(definitionId?: number): UseWorkflowResult {
  const [nodes, setNodes, onNodesChange] = useNodesState<FlowNode>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
  const [selectedNode, setSelectedNode] = useState<FlowNode | null>(null);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [workflowMeta, setWorkflowMeta] = useState<WorkflowMeta>({
    id: definitionId || null,
    name: 'New Workflow',
    description: '',
    version: 1,
    inputs: {},
    outputs: {},
    metadata: {},
  });

  // Load definition from API
  const load = useCallback(async (id: number) => {
    setLoading(true);
    setError(null);
    try {
      const detail = await fetchDefinition(id);
      const { nodes: flowNodes, edges: flowEdges } = definitionToFlow(
        detail.definition
      );
      setNodes(flowNodes);
      setEdges(flowEdges);
      setWorkflowMeta({
        id: detail.id,
        name: detail.definition.name,
        description: detail.definition.description || '',
        version: detail.definition.version,
        inputs: detail.definition.inputs || {},
        outputs: detail.definition.outputs || {},
        metadata: detail.definition.metadata || {},
      });
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load workflow');
    } finally {
      setLoading(false);
    }
  }, [setNodes, setEdges]);

  // Load on mount if definitionId provided
  useEffect(() => {
    if (definitionId) {
      load(definitionId);
    }
  }, [definitionId, load]);

  // Convert to definition
  const toDefinition = useCallback((): WorkflowDefinition => {
    return flowToDefinition(nodes, edges, {
      name: workflowMeta.name,
      description: workflowMeta.description || undefined,
      version: workflowMeta.version,
      inputs: workflowMeta.inputs,
      outputs: workflowMeta.outputs,
      metadata: workflowMeta.metadata,
    });
  }, [nodes, edges, workflowMeta]);

  // Save to API
  const save = useCallback(async (): Promise<DefinitionDetail> => {
    setSaving(true);
    setError(null);
    try {
      const definition = toDefinition();
      let result: DefinitionDetail;
      if (workflowMeta.id) {
        result = await saveDefinition(workflowMeta.id, definition);
      } else {
        result = await createDefinition(definition);
        setWorkflowMeta((prev) => ({ ...prev, id: result.id }));
      }
      return result;
    } catch (err) {
      const message =
        err instanceof Error ? err.message : 'Failed to save workflow';
      setError(message);
      throw err;
    } finally {
      setSaving(false);
    }
  }, [toDefinition, workflowMeta.id]);

  // Handle connection
  const onConnect = useCallback(
    (connection: Connection) => {
      // Validate: no self-connections
      if (connection.source === connection.target) {
        return;
      }
      // Validate: no duplicate edges
      const exists = edges.some(
        (e) => e.source === connection.source && e.target === connection.target
      );
      if (exists) {
        return;
      }
      setEdges((eds) =>
        addEdge(
          { ...connection, id: `${connection.source}->${connection.target}` },
          eds
        )
      );
    },
    [edges, setEdges]
  );

  // Add a new node
  const addNode = useCallback(
    (nodeType: string, label: string, position: { x: number; y: number }) => {
      const id = label.toLowerCase().replace(/\s+/g, '_');
      // Ensure unique ID
      let uniqueId = id;
      let counter = 1;
      while (nodes.some((n) => n.id === uniqueId)) {
        uniqueId = `${id}_${counter}`;
        counter++;
      }

      const newNode: FlowNode = {
        id: uniqueId,
        type: 'workflow',
        position,
        data: {
          nodeType,
          label: uniqueId,
          inputs: {},
        },
      };
      setNodes((nds) => [...nds, newNode]);
    },
    [nodes, setNodes]
  );

  // Update node data
  const updateNodeData = useCallback(
    (nodeId: string, data: Partial<FlowNodeData>) => {
      setNodes((nds) =>
        nds.map((node) =>
          node.id === nodeId
            ? { ...node, data: { ...node.data, ...data } }
            : node
        )
      );
      // Also update selectedNode if it's the same node
      setSelectedNode((prev) =>
        prev?.id === nodeId ? { ...prev, data: { ...prev.data, ...data } } : prev
      );
    },
    [setNodes]
  );

  // Delete a node and its edges
  const deleteNode = useCallback(
    (nodeId: string) => {
      setNodes((nds) => nds.filter((n) => n.id !== nodeId));
      setEdges((eds) =>
        eds.filter((e) => e.source !== nodeId && e.target !== nodeId)
      );
      if (selectedNode?.id === nodeId) {
        setSelectedNode(null);
      }
    },
    [selectedNode, setNodes, setEdges]
  );

  // Auto-layout all nodes
  const autoLayout = useCallback(() => {
    setNodes((nds) => computeLayout(nds, edges));
  }, [edges, setNodes]);

  return {
    nodes,
    edges,
    setNodes,
    setEdges,
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
    load,
    save,
    addNode,
    updateNodeData,
    deleteNode,
    autoLayout,
    toDefinition,
  };
}
