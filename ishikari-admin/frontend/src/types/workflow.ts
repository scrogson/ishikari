// Types matching the backend WorkflowDefinition structure

export interface SchemaField {
  type: string;
  description?: string;
  default?: unknown;
  required?: boolean;
}

export interface NodeDefinition {
  type: string;
  depends_on: string[];
  inputs: Record<string, unknown>;
  when?: string;
  timeout_seconds?: number;
  max_retries?: number;
}

export interface WorkflowDefinition {
  name: string;
  description?: string;
  version: number;
  inputs: Record<string, SchemaField>;
  nodes: Record<string, NodeDefinition>;
  outputs: Record<string, string>;
  metadata: Record<string, unknown>;
}

export interface DefinitionDetail {
  id: number;
  name: string;
  version: number;
  description?: string;
  definition: WorkflowDefinition;
  yaml: string;
  created_at?: string;
  updated_at?: string;
}

// Node type registry types

export interface NodeTypeInfo {
  name: string;
  description?: string;
  category: string;
  input_schema: Record<string, SchemaField>;
  output_schema: Record<string, SchemaField>;
}

export interface NodeTypesResponse {
  node_types: NodeTypeInfo[];
}

// xyflow internal types - must extend Record<string, unknown> for xyflow compatibility

export interface FlowNodeData extends Record<string, unknown> {
  nodeType: string;
  label: string;
  inputs: Record<string, unknown>;
  condition?: string;
  timeout?: number;
  maxRetries?: number;
}

// Validation types

export interface ValidationError {
  node_id?: string;
  field?: string;
  message: string;
}

export interface ValidationResponse {
  valid: boolean;
  errors: ValidationError[];
}
