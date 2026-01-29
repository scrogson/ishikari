import { useState, useEffect } from 'react';
import { fetchNodeTypes } from '../api/client';
import type { NodeTypeInfo } from '../types/workflow';

interface UseNodeTypesResult {
  nodeTypes: NodeTypeInfo[];
  loading: boolean;
  error: string | null;
  categorized: Record<string, NodeTypeInfo[]>;
}

export function useNodeTypes(): UseNodeTypesResult {
  const [nodeTypes, setNodeTypes] = useState<NodeTypeInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchNodeTypes()
      .then((response) => {
        setNodeTypes(response.node_types);
        setLoading(false);
      })
      .catch((err) => {
        setError(err.message);
        setLoading(false);
      });
  }, []);

  // Group by category
  const categorized = nodeTypes.reduce<Record<string, NodeTypeInfo[]>>(
    (acc, nodeType) => {
      const category = nodeType.category || 'other';
      if (!acc[category]) {
        acc[category] = [];
      }
      acc[category].push(nodeType);
      return acc;
    },
    {}
  );

  return { nodeTypes, loading, error, categorized };
}
