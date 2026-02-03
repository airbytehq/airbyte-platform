import { useMutation } from "@tanstack/react-query";
import { useCallback, useEffect, useReducer, useRef, useState } from "react";

import {
  ToolCallEvent,
  ToolResponseEvent,
  DeferredToolEvent,
  DeferredToolResult,
  useAgentStream,
  type AgentStreamHandlers,
} from "core/api";

import { type ChatMessage } from "../Message";

export interface ClientToolHandler {
  toolName: string;
  execute: (args: unknown, sendResult: (result: string) => void) => void | Promise<void>;
}

export type ClientTools = Record<string, ClientToolHandler>;

interface UseChatMessagesReturn {
  messages: ChatMessage[];
  sendMessage: (content: string) => void;
  sendDeferredToolResult: (toolCallId: string, result: string) => void;
  isLoading: boolean;
  error: string | null;
  clearMessages: () => void;
  stopGenerating: () => void;
  isStreaming: boolean;
}

type MessageAction =
  | { type: "ADD_USER_MESSAGE"; message: ChatMessage }
  | { type: "ADD_ASSISTANT_MESSAGE"; message: ChatMessage }
  | { type: "UPDATE_STREAMING"; messageId: string; delta: string }
  | { type: "ADD_TOOL_CALL"; toolCall: ToolCallEvent; continuationMessageId: string }
  | { type: "ADD_TOOL_RESPONSE"; toolResponse: ToolResponseEvent }
  | { type: "FINALIZE_STREAMING"; messageId?: string }
  | { type: "REMOVE_STREAMING_MESSAGES" }
  | { type: "CLEAR_MESSAGES" };

const messageReducer = (state: ChatMessage[], action: MessageAction): ChatMessage[] => {
  switch (action.type) {
    case "ADD_USER_MESSAGE":
      return [...state, action.message];

    case "ADD_ASSISTANT_MESSAGE":
      return [...state, action.message];

    case "UPDATE_STREAMING":
      return state.map((msg) =>
        msg.id === action.messageId ? { ...msg, content: msg.content + action.delta, isStreaming: true } : msg
      );

    case "ADD_TOOL_CALL": {
      const finalized = state.map((msg) => (msg.isStreaming ? { ...msg, isStreaming: false } : msg));

      // Check if this tool call already exists to prevent duplicates
      const toolCallMessageId = `tool-call-message-${action.toolCall.tool_call_id}`;
      const alreadyExists = finalized.some((msg) => msg.id === toolCallMessageId);

      if (alreadyExists) {
        // Still create the continuation message for future assistant deltas
        const continuationMessage: ChatMessage = {
          id: action.continuationMessageId,
          content: "",
          role: "assistant",
          timestamp: new Date(),
          isStreaming: true,
        };
        return [...finalized, continuationMessage];
      }

      const toolCallMessage: ChatMessage = {
        id: toolCallMessageId,
        content: "",
        role: "tool",
        timestamp: new Date(),
        toolCall: {
          tool_name: action.toolCall.tool_name,
          args: action.toolCall.args,
          call_id: action.toolCall.tool_call_id,
        },
      };

      const continuationMessage: ChatMessage = {
        id: action.continuationMessageId,
        content: "",
        role: "assistant",
        timestamp: new Date(),
        isStreaming: true,
      };

      return [...finalized, toolCallMessage, continuationMessage];
    }

    case "ADD_TOOL_RESPONSE": {
      // Find the tool call message with matching tool_call_id
      const toolMessageIndex = state.findIndex(
        (msg) => msg.role === "tool" && msg.toolCall?.call_id === action.toolResponse.tool_call_id
      );

      if (toolMessageIndex === -1) {
        // Tool call not found - this shouldn't happen but log it
        console.warn(`Tool response received for unknown tool_call_id: ${action.toolResponse.tool_call_id}`);
        return state;
      }

      const updated = [...state];
      const targetMessage = updated[toolMessageIndex];

      updated[toolMessageIndex] = {
        ...targetMessage,
        toolResponse: {
          tool_name: action.toolResponse.tool_name,
          response: action.toolResponse.response,
          call_id: action.toolResponse.tool_call_id,
        },
      };
      return updated;
    }

    case "FINALIZE_STREAMING":
      if (action.messageId) {
        return state.map((msg) => (msg.id === action.messageId ? { ...msg, isStreaming: false } : msg));
      }
      return state.map((msg) => (msg.isStreaming ? { ...msg, isStreaming: false } : msg));

    case "REMOVE_STREAMING_MESSAGES":
      return state.filter((msg) => !msg.isStreaming);

    case "CLEAR_MESSAGES":
      return [];

    default:
      return state;
  }
};

export interface UseChatMessagesParams {
  endpoint: string;
  prompt?: string;
  agentParams?: Record<string, unknown>;
  clientTools?: ClientTools;
  onThreadIdChange?: (threadId: string) => void;
  skipInitialRequest?: boolean;
}

/**
 * Helper to build stream handlers with threadId management.
 * Reduces duplication between sendPrompt and sendDeferredToolResults.
 */
const buildStreamHandlers = (handlers: AgentStreamHandlers, onThreadIdReceived: (threadId: string) => void) => ({
  onThreadId: (newThreadId: string) => {
    onThreadIdReceived(newThreadId);
    handlers.onThreadId?.(newThreadId);
  },
  onAssistantDelta: handlers.onAssistantDelta,
  onToolCall: handlers.onToolCall,
  onToolResponse: handlers.onToolResponse,
  onDeferredTool: handlers.onDeferredTool,
  onMessage: handlers.onMessage,
  onComplete: handlers.onComplete,
});

export const useChatMessages = (params: UseChatMessagesParams): UseChatMessagesReturn => {
  const {
    endpoint,
    prompt = "",
    agentParams = {},
    clientTools = {},
    onThreadIdChange,
    skipInitialRequest = false,
  } = params;
  const [messages, dispatch] = useReducer(messageReducer, []);
  const [error, setError] = useState<string | null>(null);
  const [pendingDeferredTools, setPendingDeferredTools] = useState<Set<string>>(new Set());

  const { stream, stop, isStreaming } = useAgentStream();
  const [threadId, setThreadId] = useState<string>();
  const lastThreadIdRef = useRef<string>();
  const streamingMessageIdRef = useRef<string | null>(null);

  const handleThreadIdReceived = useCallback((newThreadId: string) => {
    lastThreadIdRef.current = newThreadId;
    setThreadId(newThreadId);
  }, []);

  const clearThread = useCallback(() => {
    lastThreadIdRef.current = undefined;
    setThreadId(undefined);
  }, []);

  // Use a ref to always access the latest clientTools, avoiding stale closures
  const clientToolsRef = useRef<ClientTools>(clientTools);
  useEffect(() => {
    clientToolsRef.current = clientTools;
  }, [clientTools]);

  // Notify parent when threadId changes
  useEffect(() => {
    if (threadId && onThreadIdChange) {
      onThreadIdChange(threadId);
    }
  }, [threadId, onThreadIdChange]);

  const updateStreamingMessage = useCallback((messageId: string, delta: string) => {
    dispatch({ type: "UPDATE_STREAMING", messageId, delta });
  }, []);

  const addToolCall = useCallback((toolCall: ToolCallEvent) => {
    const continuationMessageId = `assistant-continuation-${Date.now()}`;
    streamingMessageIdRef.current = continuationMessageId;
    dispatch({ type: "ADD_TOOL_CALL", toolCall, continuationMessageId });
  }, []);

  const addToolResponse = useCallback((toolResponse: ToolResponseEvent) => {
    dispatch({ type: "ADD_TOOL_RESPONSE", toolResponse });
  }, []);

  const finalizeAllStreamingMessages = useCallback(() => {
    streamingMessageIdRef.current = null;
    dispatch({ type: "FINALIZE_STREAMING" });
  }, []);

  const removeStreamingMessages = useCallback(() => {
    streamingMessageIdRef.current = null;
    dispatch({ type: "REMOVE_STREAMING_MESSAGES" });
  }, []);

  // Wrapper functions for sending prompts and deferred tool results
  const sendPrompt = useCallback(
    async (prompt: string, handlers: AgentStreamHandlers = {}) => {
      const activeThreadId = lastThreadIdRef.current;

      await stream(
        { prompt, threadId: activeThreadId, endpoint, agentParams },
        buildStreamHandlers(handlers, handleThreadIdReceived)
      );
    },
    [stream, endpoint, agentParams, handleThreadIdReceived]
  );

  const sendDeferredToolResults = useCallback(
    async (deferredToolResults: DeferredToolResult[], handlers: AgentStreamHandlers = {}) => {
      const activeThreadId = lastThreadIdRef.current;

      await stream(
        { prompt: "", threadId: activeThreadId, endpoint, agentParams, deferredToolResults },
        buildStreamHandlers(handlers, handleThreadIdReceived)
      );
    },
    [stream, endpoint, agentParams, handleThreadIdReceived]
  );

  // Use a ref to store the latest sendDeferredToolResultCallback to avoid circular dependencies
  const sendDeferredToolResultRef = useRef<(toolCallId: string, result: string) => void>();

  const handleDeferredTool = useCallback((deferredTool: DeferredToolEvent) => {
    const handler = clientToolsRef.current[deferredTool.tool_name];
    if (handler) {
      // Mark this tool as pending
      setPendingDeferredTools((prev) => new Set(prev).add(deferredTool.tool_call_id));

      try {
        const args = JSON.parse(deferredTool.args);

        const sendResult = (result: string) => {
          // Remove from pending when result is sent
          setPendingDeferredTools((prev) => {
            const next = new Set(prev);
            next.delete(deferredTool.tool_call_id);
            return next;
          });
          sendDeferredToolResultRef.current?.(deferredTool.tool_call_id, result);
        };

        const executeResult = handler.execute(args, sendResult);

        // If the handler returns a promise, handle completion/errors
        if (executeResult instanceof Promise) {
          executeResult.catch((error) => {
            console.error(`[useChatMessages] Failed to execute client tool ${deferredTool.tool_name}:`, error);
            setPendingDeferredTools((prev) => {
              const next = new Set(prev);
              next.delete(deferredTool.tool_call_id);
              return next;
            });
            sendDeferredToolResultRef.current?.(
              deferredTool.tool_call_id,
              JSON.stringify({
                success: false,
                message: "Failed to execute tool",
                error: error instanceof Error ? error.message : String(error),
              })
            );
          });
        }
      } catch (error) {
        console.error(`[useChatMessages] Failed to execute client tool ${deferredTool.tool_name}:`, error);
        setPendingDeferredTools((prev) => {
          const next = new Set(prev);
          next.delete(deferredTool.tool_call_id);
          return next;
        });
        sendDeferredToolResultRef.current?.(
          deferredTool.tool_call_id,
          JSON.stringify({
            success: false,
            message: "Failed to execute tool",
            error: error instanceof Error ? error.message : String(error),
          })
        );
      }
    } else {
      console.warn(`[useChatMessages] No handler found for deferred tool: ${deferredTool.tool_name}`);
    }
  }, []);

  const sendDeferredToolResultCallback = useCallback(
    (toolCallId: string, result: string) => {
      const deferredToolResult: DeferredToolResult = {
        tool_call_id: toolCallId,
        result,
      };

      sendDeferredToolResults([deferredToolResult], {
        onAssistantDelta: (chunk: string) => {
          const activeMessageId = streamingMessageIdRef.current;
          if (activeMessageId) {
            updateStreamingMessage(activeMessageId, chunk);
          }
        },
        onToolCall: (toolCall: ToolCallEvent) => addToolCall(toolCall),
        onToolResponse: (toolResponse: ToolResponseEvent) => addToolResponse(toolResponse),
        onDeferredTool: handleDeferredTool,
        onComplete: () => finalizeAllStreamingMessages(),
      }).catch((err: Error) => {
        if (err instanceof Error && err.name !== "AbortError") {
          setError(err.message);
          removeStreamingMessages();
        }
      });
    },
    [
      sendDeferredToolResults,
      updateStreamingMessage,
      addToolCall,
      addToolResponse,
      handleDeferredTool,
      finalizeAllStreamingMessages,
      removeStreamingMessages,
    ]
  );

  // Keep the ref up to date
  sendDeferredToolResultRef.current = sendDeferredToolResultCallback;

  const { mutate: sendMessage, isLoading: isPending } = useMutation({
    mutationFn: async (content: string) => {
      setError(null);

      const userMessage: ChatMessage = {
        id: `user-${Date.now()}`,
        content,
        role: "user",
        timestamp: new Date(),
      };

      dispatch({ type: "ADD_USER_MESSAGE", message: userMessage });

      const assistantMessageId = `assistant-${Date.now()}`;
      const assistantMessage: ChatMessage = {
        id: assistantMessageId,
        content: "",
        role: "assistant",
        timestamp: new Date(),
        isStreaming: true,
      };

      dispatch({ type: "ADD_ASSISTANT_MESSAGE", message: assistantMessage });
      streamingMessageIdRef.current = assistantMessageId;

      try {
        await sendPrompt(content, {
          onAssistantDelta: (chunk) => {
            const activeMessageId = streamingMessageIdRef.current ?? assistantMessageId;
            updateStreamingMessage(activeMessageId, chunk);
          },
          onToolCall: (toolCall) => addToolCall(toolCall),
          onToolResponse: (toolResponse) => addToolResponse(toolResponse),
          onDeferredTool: handleDeferredTool,
          onComplete: () => finalizeAllStreamingMessages(),
        });
      } catch (err) {
        if (err instanceof Error && err.name === "AbortError") {
          return;
        }
        throw err;
      }
    },
    onError: (err) => {
      setError(err instanceof Error ? err.message : "An error occurred");
      removeStreamingMessages();
    },
  });

  const clearMessages = useCallback(() => {
    dispatch({ type: "CLEAR_MESSAGES" });
    setError(null);
    clearThread();
    streamingMessageIdRef.current = null;
  }, [clearThread]);

  // Send initial request with agentParams when component mounts
  const hasInitializedRef = useRef(false);
  useEffect(() => {
    // Skip if explicitly requested
    if (skipInitialRequest) {
      return;
    }

    if (agentParams && Object.keys(agentParams).length > 0 && !hasInitializedRef.current) {
      hasInitializedRef.current = true;

      const assistantMessageId = `assistant-init-${Date.now()}`;
      const assistantMessage: ChatMessage = {
        id: assistantMessageId,
        content: "",
        role: "assistant",
        timestamp: new Date(),
        isStreaming: true,
      };

      dispatch({ type: "ADD_ASSISTANT_MESSAGE", message: assistantMessage });
      streamingMessageIdRef.current = assistantMessageId;

      sendPrompt(prompt, {
        onAssistantDelta: (chunk) => {
          const activeMessageId = streamingMessageIdRef.current ?? assistantMessageId;
          updateStreamingMessage(activeMessageId, chunk);
        },
        onToolCall: (toolCall) => addToolCall(toolCall),
        onToolResponse: (toolResponse) => addToolResponse(toolResponse),
        onDeferredTool: handleDeferredTool,
        onComplete: () => finalizeAllStreamingMessages(),
      }).catch((err) => {
        if (err instanceof Error && err.name !== "AbortError") {
          setError(err.message);
          removeStreamingMessages();
        }
      });
    }
  }, [
    skipInitialRequest,
    agentParams,
    prompt,
    handleDeferredTool,
    sendPrompt,
    updateStreamingMessage,
    addToolCall,
    addToolResponse,
    finalizeAllStreamingMessages,
    removeStreamingMessages,
  ]);

  return {
    messages,
    sendMessage,
    sendDeferredToolResult: sendDeferredToolResultCallback,
    isLoading: isPending || isStreaming || pendingDeferredTools.size > 0,
    error,
    clearMessages,
    stopGenerating: () => {
      stop();
      const lastStreaming = [...messages].reverse().find((m) => m.isStreaming);
      if (lastStreaming) {
        dispatch({ type: "FINALIZE_STREAMING", messageId: lastStreaming.id });
      }
      streamingMessageIdRef.current = null;
    },
    isStreaming,
  };
};
