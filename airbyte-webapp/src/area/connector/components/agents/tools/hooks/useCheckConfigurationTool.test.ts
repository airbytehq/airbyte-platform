import { renderHook } from "@testing-library/react";
import { type MutableRefObject } from "react";

import { type ConnectorFormValues } from "area/connector/components/ConnectorForm/types";
import { useTestConnectorCommand } from "core/api";
import { ActorType, type DestinationRead, type SourceRead } from "core/api/types/AirbyteClient";
import { type ConnectorT } from "core/domain/connector";

import { useCheckConfigurationTool } from "./useCheckConfigurationTool";

jest.mock("core/api", () => ({
  useTestConnectorCommand: jest.fn(),
}));

const mockUseTestConnectorCommand = useTestConnectorCommand as jest.Mock;

const formValues = {
  name: "Single-use credential connector",
  connectionConfiguration: { token: "single-use" },
  resourceAllocation: {},
} as ConnectorFormValues;

const actorCases: Array<{ actorType: ActorType; draft: ConnectorT }> = [
  {
    actorType: ActorType.source,
    draft: { sourceId: "source-draft-id", isDraft: true } as SourceRead,
  },
  {
    actorType: ActorType.destination,
    draft: { destinationId: "destination-draft-id", isDraft: true } as DestinationRead,
  },
];

describe.each(actorCases)("useCheckConfigurationTool for $actorType", ({ actorType, draft }) => {
  const testConnector = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
    mockUseTestConnectorCommand.mockReturnValue({ testConnector });
  });

  it("creates one draft, checks it by actor, and retains it after a failed check", async () => {
    const draftConnectorRef = { current: undefined } as MutableRefObject<ConnectorT | undefined>;
    const onSaveDraft = jest.fn(async (_values, existingDraft?: ConnectorT) => existingDraft ?? draft);
    const sendResult = jest.fn();
    const consoleErrorSpy = jest.spyOn(console, "error").mockImplementation(() => undefined);
    testConnector.mockRejectedValue(new Error("check failed"));

    const { result } = renderHook(() =>
      useCheckConfigurationTool({
        actorDefinitionId: "definition-id",
        actorType,
        getFormValues: () => formValues,
        onSaveDraft,
        draftConnectorRef,
      })
    );

    await result.current.execute({}, sendResult);
    await result.current.execute({}, sendResult);

    expect(onSaveDraft).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({
        createAsDraft: true,
        serviceType: "definition-id",
        connectionConfiguration: { token: "single-use" },
      }),
      undefined
    );
    expect(onSaveDraft).toHaveBeenNthCalledWith(2, expect.anything(), draft);
    expect(draftConnectorRef.current).toBe(draft);
    expect(testConnector).toHaveBeenCalledTimes(2);
    expect(testConnector).toHaveBeenNthCalledWith(1, undefined, draft);
    expect(testConnector).toHaveBeenNthCalledWith(2, undefined, draft);
    expect(JSON.parse(sendResult.mock.calls[0][0])).toEqual(expect.objectContaining({ success: false }));

    consoleErrorSpy.mockRestore();
  });

  it("reports a cancelled actor check as unsuccessful", async () => {
    const draftConnectorRef = { current: undefined } as MutableRefObject<ConnectorT | undefined>;
    const draftCheckSucceededRef = { current: true } as MutableRefObject<boolean>;
    const onSaveDraft = jest.fn().mockResolvedValue(draft);
    const onCheckComplete = jest.fn();
    const sendResult = jest.fn();
    testConnector.mockResolvedValue({ id: "command-id", status: "failed" });

    const { result } = renderHook(() =>
      useCheckConfigurationTool({
        actorDefinitionId: "definition-id",
        actorType,
        getFormValues: () => formValues,
        onCheckComplete,
        onSaveDraft,
        draftConnectorRef,
        draftCheckSucceededRef,
      })
    );

    await result.current.execute({}, sendResult);

    expect(draftConnectorRef.current).toBe(draft);
    expect(draftCheckSucceededRef.current).toBe(false);
    expect(JSON.parse(sendResult.mock.calls[0][0])).toEqual(
      expect.objectContaining({ success: false, status: "failed" })
    );
    expect(onCheckComplete).toHaveBeenCalledWith(false);
  });

  it("records a successful actor check", async () => {
    const draftConnectorRef = { current: undefined } as MutableRefObject<ConnectorT | undefined>;
    const draftCheckSucceededRef = { current: false } as MutableRefObject<boolean>;
    testConnector.mockResolvedValue({ id: "command-id", status: "succeeded" });

    const { result } = renderHook(() =>
      useCheckConfigurationTool({
        actorDefinitionId: "definition-id",
        actorType,
        getFormValues: () => formValues,
        onSaveDraft: jest.fn().mockResolvedValue(draft),
        draftConnectorRef,
        draftCheckSucceededRef,
      })
    );

    await result.current.execute({}, jest.fn());

    expect(draftCheckSucceededRef.current).toBe(true);
  });
});
