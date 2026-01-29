import type {
  DefinitionDetail,
  NodeTypesResponse,
  ValidationResponse,
  WorkflowDefinition,
} from '../types/workflow';

const API_BASE = '/api';

class ApiError extends Error {
  status: number;

  constructor(message: string, status: number) {
    super(message);
    this.status = status;
    this.name = 'ApiError';
  }
}

async function handleResponse<T>(response: Response): Promise<T> {
  if (!response.ok) {
    const text = await response.text();
    throw new ApiError(text || response.statusText, response.status);
  }
  return response.json();
}

export async function fetchNodeTypes(): Promise<NodeTypesResponse> {
  const response = await fetch(`${API_BASE}/node-types`);
  return handleResponse<NodeTypesResponse>(response);
}

export async function fetchDefinition(id: number): Promise<DefinitionDetail> {
  const response = await fetch(`${API_BASE}/definitions/${id}`);
  return handleResponse<DefinitionDetail>(response);
}

export async function saveDefinition(
  id: number,
  definition: WorkflowDefinition
): Promise<DefinitionDetail> {
  const response = await fetch(`${API_BASE}/definitions/${id}`, {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(definition),
  });
  return handleResponse<DefinitionDetail>(response);
}

export async function createDefinition(
  definition: WorkflowDefinition
): Promise<DefinitionDetail> {
  const response = await fetch(`${API_BASE}/definitions`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(definition),
  });
  return handleResponse<DefinitionDetail>(response);
}

export async function validateDefinition(
  definition: WorkflowDefinition
): Promise<ValidationResponse> {
  const response = await fetch(`${API_BASE}/definitions/validate`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(definition),
  });
  return handleResponse<ValidationResponse>(response);
}

export { ApiError };
