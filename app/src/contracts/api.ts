export type RunStatus =
  | "running"
  | "awaiting_input"
  | "completed"
  | "failed"
  | "cancelled"
  | "interrupted";

export interface ApiError {
  code: string;
  message: string;
  retryable: boolean;
  requestId?: string;
  runId?: string;
}

export interface Project {
  id: string;
  name: string;
  description: string | null;
  status: string;
  sessionCount: number;
  createdAt: string | null;
  updatedAt: string | null;
}

export interface Session {
  id: string;
  projectId: string;
  model: string | null;
}

export type DocumentFocus =
  | {
      targetType: "document";
      documentId: string;
    }
  | {
      targetType: "subtree";
      folderRootId: string;
      pathPrefix: string;
    }
  | {
      targetType: "folder_upload";
      folderRootId: string;
    };

export interface CreateRunRequest {
  content: string;
  model?: string;
  agentId?: string;
  enabledTools?: string[];
  documentFocus?: DocumentFocus;
}

export interface Message {
  id: number;
  sessionId: string;
  role: "user" | "assistant";
  content: string;
  timestamp: string;
}

export interface Citation {
  id?: string;
  kind: string;
  label: string;
  excerpt: string;
  documentId?: string;
  url?: string;
  locator?: unknown;
}

export interface Run {
  id: string;
  sessionId: string;
  status: RunStatus;
  userMessageId?: number;
  assistantMessageId?: number;
  createdAt: string;
  completedAt?: string;
  result?: RunCompletedData;
}

export interface RunEventEnvelope<T = unknown> {
  type:
    | "run.started"
    | "message.delta"
    | "tool.started"
    | "tool.completed"
    | "run.awaiting_input"
    | "run.completed"
    | "run.failed"
    | "run.cancelled";
  version: "v1";
  runId: string;
  sequence: number;
  timestamp: string;
  data: T;
}

export interface ToolStartedData {
  callId: string;
  tool: string;
  arguments: Record<string, string | number | boolean>;
}

export interface RunCompletedData {
  content: string;
  citations: Citation[];
  usage?: Record<string, number | boolean>;
  assistantMessageId?: number;
  sourceRefIds?: string[];
}
