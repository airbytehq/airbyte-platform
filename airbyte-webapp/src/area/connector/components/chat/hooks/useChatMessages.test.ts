import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, act } from "@testing-library/react";
import { createElement, type ReactNode } from "react";

import type { AgentStreamHandlers, AgentStreamParams, DeferredToolEvent } from "core/api";

import { useChatMessages, type UseChatMessagesParams } from "./useChatMessages";

// Capture the handlers passed to stream() so tests can drive SSE events
let capturedHandlers: AgentStreamHandlers = {};
const mockStream = jest.fn<Promise<void>, [AgentStreamParams, AgentStreamHandlers]>();
const mockStop = jest.fn();

jest.mock("core/api", () => ({
  useAgentStream: () => ({
    stream: mockStream,
    stop: mockStop,
    isStreaming: false,
  }),
}));

const defaultParams: UseChatMessagesParams = {
  endpoint: "/agents/connector_setup/chat",
  skipInitialRequest: true,
};

function createWrapper() {
  const queryClient = new QueryClient({
    defaultOptions: { mutations: { retry: false } },
  });
  return ({ children }: { children: ReactNode }) =>
    createElement(QueryClientProvider, { client: queryClient }, children);
}

beforeEach(() => {
  jest.clearAllMocks();
  capturedHandlers = {};
  mockStream.mockImplementation(async (_params, handlers) => {
    capturedHandlers = handlers ?? {};
  });
});

describe("useChatMessages – deferred-tool batching", () => {
  const toolA: DeferredToolEvent = {
    tool_name: "request_secret_input",
    tool_call_id: "tool-A",
    args: JSON.stringify({ field_path: ["access_key"] }),
  };
  const toolB: DeferredToolEvent = {
    tool_name: "request_secret_input",
    tool_call_id: "tool-B",
    args: JSON.stringify({ field_path: ["secret_key"] }),
  };

  function mountWithTools() {
    const executeA = jest.fn();
    const executeB = jest.fn();
    const clientTools = {
      request_secret_input: {
        toolName: "request_secret_input",
        execute: (args: unknown, sendResult: (r: string) => void) => {
          executeA(args, sendResult);
        },
      },
    };
    const { result } = renderHook(() => useChatMessages({ ...defaultParams, clientTools }), {
      wrapper: createWrapper(),
    });
    return { result, executeA, executeB };
  }

  it("accumulates two deferred tools in a single batch and sends them together", async () => {
    const { result, executeA } = mountWithTools();

    // 1. User sends a message → triggers stream
    await act(async () => {
      result.current.sendMessage("set up S3");
    });

    // 2. Simulate two deferred_tool SSE events in the same turn
    const sendResults: Array<(r: string) => void> = [];
    executeA.mockImplementation((_args: unknown, sendResult: (r: string) => void) => {
      sendResults.push(sendResult);
    });

    act(() => {
      capturedHandlers.onDeferredTool?.(toolA);
      capturedHandlers.onDeferredTool?.(toolB);
    });

    // Both tools should have triggered the handler
    expect(sendResults).toHaveLength(2);

    // Reset the mock so we can inspect the batch call
    mockStream.mockClear();
    mockStream.mockImplementation(async (_params, handlers) => {
      capturedHandlers = handlers ?? {};
    });

    // 3. Signal stream completion to open the gate
    act(() => {
      capturedHandlers.onComplete?.();
    });

    // Gate is open but results not yet submitted — batch should not fire
    expect(mockStream).not.toHaveBeenCalled();

    // 4. Submit both results
    act(() => {
      sendResults[0]("key-id-value");
      sendResults[1]("secret-value");
    });

    // Batch should fire with both results in a single call
    expect(mockStream).toHaveBeenCalledTimes(1);
    const [params] = mockStream.mock.calls[0];
    expect(params.deferredToolResults).toHaveLength(2);
    const ids = params.deferredToolResults!.map((r) => r.tool_call_id).sort();
    expect(ids).toEqual(["tool-A", "tool-B"]);
  });

  it("does not send the batch before onComplete (gate behaviour)", async () => {
    const { result, executeA } = mountWithTools();

    await act(async () => {
      result.current.sendMessage("set up S3");
    });

    const sendResults: Array<(r: string) => void> = [];
    executeA.mockImplementation((_args: unknown, sendResult: (r: string) => void) => {
      sendResults.push(sendResult);
    });

    act(() => {
      capturedHandlers.onDeferredTool?.(toolA);
      capturedHandlers.onDeferredTool?.(toolB);
    });

    mockStream.mockClear();
    mockStream.mockImplementation(async () => {});

    // Submit results BEFORE onComplete — gate should block
    act(() => {
      sendResults[0]("key-id-value");
      sendResults[1]("secret-value");
    });

    expect(mockStream).not.toHaveBeenCalled();

    // Now open the gate
    act(() => {
      capturedHandlers.onComplete?.();
    });

    // Batch should fire now
    expect(mockStream).toHaveBeenCalledTimes(1);
  });

  it("handles unregistered deferred tools by producing an error result", async () => {
    // Mount with NO client tools registered
    const { result } = renderHook(() => useChatMessages({ ...defaultParams, clientTools: {} }), {
      wrapper: createWrapper(),
    });

    await act(async () => {
      result.current.sendMessage("set up S3");
    });

    mockStream.mockClear();
    mockStream.mockImplementation(async () => {});

    const warnSpy = jest.spyOn(console, "warn").mockImplementation(() => {});

    // Fire an unregistered deferred tool
    act(() => {
      capturedHandlers.onDeferredTool?.(toolA);
    });

    // Open the gate
    act(() => {
      capturedHandlers.onComplete?.();
    });

    expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining("No handler found for deferred tool"));

    // The batch should still fire with an error result
    expect(mockStream).toHaveBeenCalledTimes(1);
    const [params] = mockStream.mock.calls[0];
    expect(params.deferredToolResults).toHaveLength(1);
    expect(params.deferredToolResults![0].tool_call_id).toBe("tool-A");
    const resultPayload = JSON.parse(params.deferredToolResults![0].result as string);
    expect(resultPayload.success).toBe(false);

    warnSpy.mockRestore();
  });

  it("gates the batch when an unregistered tool arrives first alongside a registered tool", async () => {
    const unregisteredTool: DeferredToolEvent = {
      tool_name: "unknown_tool",
      tool_call_id: "tool-unreg",
      args: JSON.stringify({}),
    };

    const executeFn = jest.fn();
    const clientTools = {
      request_secret_input: {
        toolName: "request_secret_input",
        execute: executeFn,
      },
    };

    const { result } = renderHook(() => useChatMessages({ ...defaultParams, clientTools }), {
      wrapper: createWrapper(),
    });

    await act(async () => {
      result.current.sendMessage("set up S3");
    });

    const sendResults: Array<(r: string) => void> = [];
    executeFn.mockImplementation((_args: unknown, sendResult: (r: string) => void) => {
      sendResults.push(sendResult);
    });

    mockStream.mockClear();
    mockStream.mockImplementation(async () => {});

    const warnSpy = jest.spyOn(console, "warn").mockImplementation(() => {});

    // Fire unregistered tool FIRST, then a registered tool
    act(() => {
      capturedHandlers.onDeferredTool?.(unregisteredTool);
      capturedHandlers.onDeferredTool?.(toolA);
    });

    // The unregistered tool's error result is sent synchronously, but the
    // gate must prevent a partial batch from firing mid-stream.
    expect(mockStream).not.toHaveBeenCalled();

    // Open the gate
    act(() => {
      capturedHandlers.onComplete?.();
    });

    // Still waiting for the registered tool's result
    expect(mockStream).not.toHaveBeenCalled();

    // Submit the registered tool's result
    act(() => {
      sendResults[0]("key-value");
    });

    // Now the batch fires with both results
    expect(mockStream).toHaveBeenCalledTimes(1);
    const [params] = mockStream.mock.calls[0];
    expect(params.deferredToolResults).toHaveLength(2);
    const ids = params.deferredToolResults!.map((r) => r.tool_call_id).sort();
    expect(ids).toEqual(["tool-A", "tool-unreg"]);

    warnSpy.mockRestore();
  });
});

describe("useChatMessages – error-path reset", () => {
  it("resets batching refs on sendMessage error so the next turn is clean", async () => {
    const toolA: DeferredToolEvent = {
      tool_name: "request_secret_input",
      tool_call_id: "tool-err-A",
      args: JSON.stringify({ field_path: ["key"] }),
    };

    const executeFn = jest.fn();
    const clientTools = {
      request_secret_input: {
        toolName: "request_secret_input",
        execute: executeFn,
      },
    };

    const { result } = renderHook(() => useChatMessages({ ...defaultParams, clientTools }), {
      wrapper: createWrapper(),
    });

    // First call succeeds normally
    mockStream.mockImplementation(async (_params, handlers) => {
      capturedHandlers = handlers ?? {};
    });

    await act(async () => {
      result.current.sendMessage("set up S3");
    });

    // Fire a deferred tool
    const sendResults: Array<(r: string) => void> = [];
    executeFn.mockImplementation((_args: unknown, sendResult: (r: string) => void) => {
      sendResults.push(sendResult);
    });

    act(() => {
      capturedHandlers.onDeferredTool?.(toolA);
    });

    // Now make the next stream call throw (simulating network error)
    mockStream.mockClear();
    mockStream.mockImplementation(async (_params, handlers) => {
      capturedHandlers = handlers ?? {};
    });

    act(() => {
      capturedHandlers.onComplete?.();
    });

    // Submit the result — triggers batch send
    mockStream.mockRejectedValueOnce(new Error("Network failure"));

    act(() => {
      sendResults[0]("some-value");
    });

    // Wait for the error to propagate
    await act(async () => {
      await new Promise((r) => setTimeout(r, 10));
    });

    // Now send a second message — it should work normally, not be blocked
    // by stale batching state
    mockStream.mockClear();
    mockStream.mockImplementation(async (_params, handlers) => {
      capturedHandlers = handlers ?? {};
      handlers?.onComplete?.();
    });

    await act(async () => {
      result.current.sendMessage("try again");
    });

    // The second send should succeed (stream was called)
    expect(mockStream).toHaveBeenCalledTimes(1);
    const [params] = mockStream.mock.calls[0];
    // It should be a prompt send, not a deferred-tool-results send
    expect(params.prompt).toBe("try again");
    expect(params.deferredToolResults).toBeUndefined();
  });

  it("discards stale sendResult calls so they do not leak into the next turn", async () => {
    const toolA: DeferredToolEvent = {
      tool_name: "request_secret_input",
      tool_call_id: "tool-stale-A",
      args: JSON.stringify({ field_path: ["key"] }),
    };
    const toolB: DeferredToolEvent = {
      tool_name: "request_secret_input",
      tool_call_id: "tool-fresh-B",
      args: JSON.stringify({ field_path: ["secret"] }),
    };

    const executeFn = jest.fn();
    const clientTools = {
      request_secret_input: {
        toolName: "request_secret_input",
        execute: executeFn,
      },
    };

    const { result } = renderHook(() => useChatMessages({ ...defaultParams, clientTools }), {
      wrapper: createWrapper(),
    });

    mockStream.mockImplementation(async (_params, handlers) => {
      capturedHandlers = handlers ?? {};
    });

    await act(async () => {
      result.current.sendMessage("set up S3");
    });

    // Turn 1: fire a deferred tool, capture sendResult
    const staleSendResults: Array<(r: string) => void> = [];
    executeFn.mockImplementation((_args: unknown, sendResult: (r: string) => void) => {
      staleSendResults.push(sendResult);
    });

    act(() => {
      capturedHandlers.onDeferredTool?.(toolA);
    });

    // Abort mid-turn — resets refs but the UI still holds the sendResult callback
    act(() => {
      result.current.stopGenerating();
    });

    // Stale sendResult fires after reset (e.g. user dismisses the secret prompt)
    act(() => {
      staleSendResults[0]("stale-value");
    });

    // Turn 2: new message triggers a fresh deferred tool
    mockStream.mockClear();
    mockStream.mockImplementation(async (_params, handlers) => {
      capturedHandlers = handlers ?? {};
    });

    await act(async () => {
      result.current.sendMessage("try again");
    });

    const freshSendResults: Array<(r: string) => void> = [];
    executeFn.mockImplementation((_args: unknown, sendResult: (r: string) => void) => {
      freshSendResults.push(sendResult);
    });

    act(() => {
      capturedHandlers.onDeferredTool?.(toolB);
    });

    mockStream.mockClear();
    mockStream.mockImplementation(async () => {});

    act(() => {
      capturedHandlers.onComplete?.();
    });

    act(() => {
      freshSendResults[0]("fresh-value");
    });

    // Batch should contain ONLY the fresh result, not the stale one
    expect(mockStream).toHaveBeenCalledTimes(1);
    const [params] = mockStream.mock.calls[0];
    expect(params.deferredToolResults).toHaveLength(1);
    expect(params.deferredToolResults![0].tool_call_id).toBe("tool-fresh-B");
  });

  it("resets batching refs when stopGenerating is called mid-turn", async () => {
    const toolA: DeferredToolEvent = {
      tool_name: "request_secret_input",
      tool_call_id: "tool-stop-A",
      args: JSON.stringify({ field_path: ["key"] }),
    };

    const executeFn = jest.fn();
    const clientTools = {
      request_secret_input: {
        toolName: "request_secret_input",
        execute: executeFn,
      },
    };

    const { result } = renderHook(() => useChatMessages({ ...defaultParams, clientTools }), {
      wrapper: createWrapper(),
    });

    mockStream.mockImplementation(async (_params, handlers) => {
      capturedHandlers = handlers ?? {};
    });

    await act(async () => {
      result.current.sendMessage("set up S3");
    });

    const sendResults: Array<(r: string) => void> = [];
    executeFn.mockImplementation((_args: unknown, sendResult: (r: string) => void) => {
      sendResults.push(sendResult);
    });

    act(() => {
      capturedHandlers.onDeferredTool?.(toolA);
    });

    // Stop mid-turn
    act(() => {
      result.current.stopGenerating();
    });

    expect(mockStop).toHaveBeenCalled();

    // Now send a new message — should not be blocked by stale state
    mockStream.mockClear();
    mockStream.mockImplementation(async (_params, handlers) => {
      capturedHandlers = handlers ?? {};
      handlers?.onComplete?.();
    });

    await act(async () => {
      result.current.sendMessage("start fresh");
    });

    expect(mockStream).toHaveBeenCalledTimes(1);
    const [params] = mockStream.mock.calls[0];
    expect(params.prompt).toBe("start fresh");
    expect(params.deferredToolResults).toBeUndefined();
  });
});
