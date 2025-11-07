import { useCallback, useRef, useState } from "react";

import { useRequestOptions } from "core/api/useRequestOptions";
import { useWebappConfig } from "core/config/webappConfig";
import { trackError } from "core/utils/datadog";

export type ChatMessageEventEvent = (typeof ChatMessageEventEvent)[keyof typeof ChatMessageEventEvent];

// eslint-disable-next-line @typescript-eslint/no-redeclare
export const ChatMessageEventEvent = {
  start: "start",
  thread_id: "thread_id",
  user: "user",
  assistant: "assistant",
  tool_call: "tool_call",
  tool_response: "tool_response",
  deferred_tool: "deferred_tool",
} as const;

export type ChatMessageEventData = string | MessageContent | ToolCallEvent | ToolResponseEvent | DeferredToolEvent;

export interface ToolResponseEvent {
  tool_name: string;
  tool_call_id: string;
  response: unknown;
}

export interface DeferredToolEvent {
  tool_name: string;
  tool_call_id: string;
  args: string;
}

export interface DeferredToolResult {
  tool_call_id: string;
  result: Record<string, unknown> | string;
}

/**
 * Format of chat message events sent to the browser.
 */
export interface ChatMessageEvent {
  event: ChatMessageEventEvent;
  data: ChatMessageEventData;
}

export type ToolCallEventArgsAnyOf = Record<string, unknown>;

export type ToolCallEventArgs = string | ToolCallEventArgsAnyOf | null;

/**
 * Format of tool call events sent to the browser.
 */
export interface ToolCallEvent {
  tool_name: string;
  tool_call_id: string;
  args: ToolCallEventArgs;
}

/**
 * Content of a chat message.
 */
export interface MessageContent {
  content: string;
}

export interface ChatPostRequest {
  prompt?: string;
  thread_id?: string;
  deferred_tool_results?: DeferredToolResult[];
  [key: string]: unknown; // Allow any additional properties
}

/**
 * This file contains the hooks for the agents API.
 * The agents API is used to stream the response from the agent.
 *
 * We are using the SSE (Server-Sent Events) protocol to stream the response from the agent.
 * Per https://tkdodo.eu/blog/using-web-sockets-with-react-query the custom hook pattern we
 * are using is preferable to streamed queries (https://tanstack.com/query/v5/docs/reference/streamedQuery)
 * "use custom hooks for real-time streaming, use ReactQuery for cacheable data fetches"
 */

export interface AgentStreamParams {
  prompt: string;
  threadId?: string;
  endpoint?: string;
  agentParams?: Record<string, unknown>;
  deferredToolResults?: DeferredToolResult[];
}

export interface AgentStreamHandlers {
  onThreadId?: (threadId: string) => void;
  onAssistantDelta?: (delta: string) => void;
  onToolCall?: (toolCall: ToolCallEvent) => void;
  onToolResponse?: (toolResponse: ToolResponseEvent) => void;
  onDeferredTool?: (deferredTool: DeferredToolEvent) => void;
  onMessage?: (event: ChatMessageEvent) => void;
  onComplete?: () => void;
}

interface AgentStreamReturn {
  stream: (params: AgentStreamParams, handlers?: AgentStreamHandlers) => Promise<void>;
  stop: () => void;
  isStreaming: boolean;
}

const parseSseEvent = (block: string): ChatMessageEvent | null => {
  if (!block.trim()) {
    return null;
  }

  // Check if the block starts with "data: " and has JSON content
  let jsonContent = block.trim();
  if (jsonContent.startsWith("data:")) {
    jsonContent = jsonContent.slice("data:".length).trim();
  }

  // Try parsing as JSON first (new format)
  try {
    const jsonEvent = JSON.parse(jsonContent);
    if (jsonEvent.event && jsonEvent.data !== undefined) {
      // Convert data to string if it's an object
      const data = typeof jsonEvent.data === "object" ? JSON.stringify(jsonEvent.data) : jsonEvent.data;
      const parsed = {
        event: jsonEvent.event as ChatMessageEventEvent,
        data,
      };
      return parsed;
    }
  } catch (e) {
    // Not JSON, fall through to SSE format parsing
  }

  // Fall back to standard SSE format parsing
  const lines = block.split("\n");
  let eventType: string | undefined;
  const dataLines: string[] = [];

  for (const line of lines) {
    if (line.startsWith("event:")) {
      eventType = line.slice("event:".length).trim();
    } else if (line.startsWith("data:")) {
      dataLines.push(line.slice("data:".length).trimStart());
    }
  }

  if (!eventType) {
    return null;
  }

  const data = dataLines.join("\n");
  const parsed = {
    event: eventType as ChatMessageEventEvent,
    data,
  };
  return parsed;
};

type ParsedEventReducer = (
  handled: boolean,
  context: { parsedEvent: ChatMessageEvent; handlers: AgentStreamHandlers }
) => boolean;

const parsedEventReducers: ParsedEventReducer[] = [
  (handled, { parsedEvent, handlers }) => {
    if (handled || parsedEvent.event !== ChatMessageEventEvent.start) {
      return handled;
    }
    if (typeof parsedEvent.data !== "string") {
      return handled;
    }
    try {
      const startData = JSON.parse(parsedEvent.data);
      if (startData.thread_id) {
        handlers.onThreadId?.(startData.thread_id);
      }
    } catch (error) {
      // Failed to parse start data
    }
    return true;
  },
  (handled, { parsedEvent, handlers }) => {
    if (handled || parsedEvent.event !== ChatMessageEventEvent.thread_id) {
      return handled;
    }
    if (typeof parsedEvent.data !== "string") {
      return handled;
    }
    handlers.onThreadId?.(parsedEvent.data);
    return true;
  },
  (handled, { parsedEvent, handlers }) => {
    if (handled || parsedEvent.event !== ChatMessageEventEvent.assistant) {
      return handled;
    }
    if (typeof parsedEvent.data !== "string") {
      return handled;
    }

    const rawData = parsedEvent.data;
    try {
      const messageData = JSON.parse(rawData);
      if (messageData && typeof messageData.content === "string") {
        handlers.onAssistantDelta?.(messageData.content);
      } else {
        handlers.onAssistantDelta?.(rawData);
      }
    } catch (e) {
      handlers.onAssistantDelta?.(rawData);
    }

    return true;
  },
  (handled, { parsedEvent, handlers }) => {
    if (handled || parsedEvent.event !== ChatMessageEventEvent.tool_call) {
      return handled;
    }
    if (typeof parsedEvent.data !== "string") {
      return handled;
    }

    try {
      const toolCallData = JSON.parse(parsedEvent.data) as ToolCallEvent;
      handlers.onToolCall?.(toolCallData);
    } catch (error) {
      console.error("[reducer:tool_call] Parse error:", error);
      trackError(error instanceof Error ? error : new Error("Failed to parse tool call event"), {
        rawPayload: parsedEvent.data,
      });
    }

    return true;
  },
  (handled, { parsedEvent, handlers }) => {
    if (handled || parsedEvent.event !== ChatMessageEventEvent.tool_response) {
      return handled;
    }
    if (typeof parsedEvent.data !== "string") {
      return handled;
    }

    try {
      const toolResponseData = JSON.parse(parsedEvent.data) as ToolResponseEvent;
      handlers.onToolResponse?.(toolResponseData);
    } catch (error) {
      console.error("[reducer:tool_response] Parse error:", error);
      trackError(error instanceof Error ? error : new Error("Failed to parse tool response event"), {
        rawPayload: parsedEvent.data,
      });
    }

    return true;
  },
  (handled, { parsedEvent, handlers }) => {
    if (handled || parsedEvent.event !== ChatMessageEventEvent.deferred_tool) {
      return handled;
    }
    if (typeof parsedEvent.data !== "string") {
      return handled;
    }

    try {
      const deferredToolData = JSON.parse(parsedEvent.data) as DeferredToolEvent;
      handlers.onDeferredTool?.(deferredToolData);
    } catch (error) {
      console.error("[reducer:deferred_tool] Parse error:", error);
      trackError(error instanceof Error ? error : new Error("Failed to parse deferred tool event"), {
        rawPayload: parsedEvent.data,
      });
    }

    return true;
  },
];

const handleParsedEvent = (parsedEvent: ChatMessageEvent, handlers: AgentStreamHandlers) => {
  handlers.onMessage?.(parsedEvent);

  parsedEventReducers.reduce((handled, reducer) => reducer(handled, { parsedEvent, handlers }), false);
};

// Function to send deferred tool results
export const useAgentStream = (): AgentStreamReturn => {
  const controllerRef = useRef<AbortController | null>(null);
  const [isStreaming, setIsStreaming] = useState(false);
  const requestOptions = useRequestOptions();
  const config = useWebappConfig();

  const stop = useCallback(() => {
    controllerRef.current?.abort();
  }, []);

  const stream = useCallback(
    async (
      { prompt, threadId, endpoint = "/agents/chat", agentParams, deferredToolResults }: AgentStreamParams,
      handlers: AgentStreamHandlers = {}
    ): Promise<void> => {
      // Allow empty prompts if agentParams is provided (for initialization) or if deferredToolResults are provided
      if (!prompt.trim() && (!agentParams || Object.keys(agentParams).length === 0) && !deferredToolResults) {
        return;
      }

      // Abort any in-flight stream before starting a new one
      stop();

      const controller = new AbortController();
      controllerRef.current = controller;
      setIsStreaming(true);

      try {
        const requestPayload: ChatPostRequest = {
          ...(prompt && { prompt }),
          thread_id: threadId,
          ...(agentParams || {}),
          ...(deferredToolResults && { deferred_tool_results: deferredToolResults }),
        };

        // Use dynamic endpoint
        const baseUrl = config.coralAgentsApiUrl || "";
        if (!baseUrl) {
          throw new Error("Coral Agents API URL is not configured");
        }
        const url = `${baseUrl}/api/v1${endpoint}`;

        // Get access token for authentication
        const accessToken = await requestOptions.getAccessToken();

        const requestHeaders: Record<string, string> = {
          "Content-Type": "application/json",
        };

        if (accessToken) {
          requestHeaders.Authorization = `Bearer ${accessToken}`;
        }

        const response = await fetch(url, {
          method: "POST",
          headers: requestHeaders,
          body: JSON.stringify(requestPayload),
          signal: controller.signal,
        });

        // Check response status
        if (!response.ok) {
          const errorText = await response.text().catch(() => "Unknown error");
          throw new Error(`Agent request failed with status ${response.status}: ${errorText}`);
        }

        const reader = response.body?.getReader();
        if (!reader) {
          throw new Error("No response body returned from agent");
        }

        const decoder = new TextDecoder();
        let buffer = "";

        try {
          while (true) {
            const { done, value } = await reader.read();
            if (done) {
              break;
            }

            buffer += decoder.decode(value, { stream: true });

            // Try splitting by single newline first (for JSON events)
            let boundary = buffer.indexOf("\n");
            while (boundary !== -1) {
              const rawEvent = buffer.slice(0, boundary).trim();
              buffer = buffer.slice(boundary + 1);

              // Skip empty lines
              if (rawEvent) {
                const parsedEvent = parseSseEvent(rawEvent);
                if (parsedEvent) {
                  handleParsedEvent(parsedEvent, handlers);
                }
              }

              boundary = buffer.indexOf("\n");
            }
          }

          // Process any remaining data in buffer
          if (buffer.trim()) {
            const parsedEvent = parseSseEvent(buffer.trim());
            if (parsedEvent) {
              handleParsedEvent(parsedEvent, handlers);
            }
          }
        } catch (streamError) {
          // Handle incomplete chunked encoding or other stream errors
          // Try to process any remaining buffer data before throwing
          if (buffer.trim()) {
            try {
              const parsedEvent = parseSseEvent(buffer.trim());
              if (parsedEvent) {
                handleParsedEvent(parsedEvent, handlers);
              }
            } catch {
              // Ignore parsing errors for incomplete data
            }
          }

          // Re-throw if it's not an incomplete encoding error
          if (streamError instanceof TypeError && streamError.message.includes("network")) {
            throw new Error("Network connection interrupted while streaming");
          }
          throw streamError;
        }

        handlers.onComplete?.();
      } catch (error) {
        if (error instanceof DOMException && error.name === "AbortError") {
          // Stream was intentionally aborted — swallow error
          return;
        }
        throw error;
      } finally {
        if (controllerRef.current === controller) {
          controllerRef.current = null;
          setIsStreaming(false);
        }
      }
    },
    [stop, requestOptions, config]
  );

  return {
    stream,
    stop,
    isStreaming,
  };
};
