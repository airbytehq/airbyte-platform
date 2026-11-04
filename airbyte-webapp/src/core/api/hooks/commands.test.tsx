import { act, renderHook } from "@testing-library/react";

import { mockDestination, mockSource, TestWrapper } from "test-utils";

import { useConnectCommand, useTestConnectorCommand } from "./commands";
import { HttpError } from "../errors";
import {
  getCheckCommandOutput,
  getCommandStatus,
  getConnectCommandOutput,
  runCheckCommand,
  runConnectCommand,
} from "../generated/AirbyteClient";
import { GetConnectCommandOutput200 } from "../types/AirbyteClient";

jest.mock("uuid", () => ({ v4: () => "command-id" }));
jest.mock("core/utils/uuid", () => ({ shortUuid: () => "request-id" }));

jest.mock("../generated/AirbyteClient", () => ({
  cancelCommand: jest.fn(),
  getCheckCommandOutput: jest.fn(),
  getCommandStatus: jest.fn(),
  getConnectCommandOutput: jest.fn(),
  runCheckCommand: jest.fn(),
  runConnectCommand: jest.fn(),
}));

jest.mock("../useRequestOptions", () => ({ useRequestOptions: () => ({}) }));
jest.mock("area/workspace/utils", () => ({ useCurrentWorkspaceId: () => "workspace-id" }));

const mockRunCheckCommand = runCheckCommand as jest.MockedFunction<typeof runCheckCommand>;
const mockGetCommandStatus = getCommandStatus as jest.MockedFunction<typeof getCommandStatus>;
const mockGetCheckCommandOutput = getCheckCommandOutput as jest.MockedFunction<typeof getCheckCommandOutput>;
const mockRunConnectCommand = runConnectCommand as jest.MockedFunction<typeof runConnectCommand>;
const mockGetConnectCommandOutput = getConnectCommandOutput as jest.MockedFunction<typeof getConnectCommandOutput>;

describe("useTestConnectorCommand", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it.each([
    ["source", mockSource, mockSource.sourceId],
    ["destination", mockDestination, mockDestination.destinationId],
  ] as const)("checks a newly persisted %s draft by actor ID", async (formType, actor, actorId) => {
    mockRunCheckCommand.mockResolvedValue({ id: "command-id" });
    mockGetCommandStatus.mockResolvedValue({ id: "command-id", status: "completed" });
    mockGetCheckCommandOutput.mockResolvedValue({ id: "command-id", status: "succeeded" });
    const { result } = renderHook(() => useTestConnectorCommand({ formType }), {
      wrapper: TestWrapper,
    });

    await result.current.testConnector(
      { name: "Draft actor", serviceType: "definition-id", connectionConfiguration: {}, resourceAllocation: {} },
      actor
    );

    expect(mockRunCheckCommand).toHaveBeenCalledWith(
      expect.objectContaining({ actor_id: actorId, config: undefined }),
      expect.anything()
    );
    expect(mockRunCheckCommand).not.toHaveBeenCalledWith(
      expect.objectContaining({ actor_definition_id: expect.anything() }),
      expect.anything()
    );
  });

  it("does not expose a cancelled check as successful UI state", async () => {
    mockRunCheckCommand.mockResolvedValue({ id: "command-id" });
    mockGetCommandStatus.mockResolvedValue({ id: "command-id", status: "cancelled" });
    const { result } = renderHook(() => useTestConnectorCommand({ formType: "source" }), {
      wrapper: TestWrapper,
    });

    await act(async () => {
      await result.current.testConnector({
        name: "Draft actor",
        serviceType: "definition-id",
        connectionConfiguration: {},
        resourceAllocation: {},
      });
    });

    expect(result.current.isTestConnectionInProgress).toBe(false);
    expect(result.current.isSuccess).toBe(false);
  });
});

describe("useConnectCommand", () => {
  const target = { host: "db.example.com", port: 5432 };
  const connectOutput: GetConnectCommandOutput200 = {
    id: "command-id",
    reachable: true,
    error_code: "none",
    message: "Connected to 10.0.0.1:5432",
    resolved_addresses: ["10.0.0.1"],
    connect_latency_ms: 12,
  };
  const httpError = (status: number, url: string) => new HttpError({ url, method: "POST" }, status, {});

  const renderConnectCommand = () => renderHook(() => useConnectCommand(), { wrapper: TestWrapper });

  beforeEach(() => {
    jest.resetAllMocks();
    mockRunConnectCommand.mockResolvedValue({ id: "command-id" });
    mockGetCommandStatus.mockResolvedValue({ id: "command-id", status: "completed" });
    mockGetConnectCommandOutput.mockResolvedValue(connectOutput);
  });

  it("runs the command, polls until it resolves, and resolves with its output", async () => {
    mockGetCommandStatus
      .mockResolvedValueOnce({ id: "command-id", status: "running" })
      .mockResolvedValueOnce({ id: "command-id", status: "completed" });
    const { result } = renderConnectCommand();

    await expect(result.current(target)).resolves.toEqual(connectOutput);

    expect(mockRunConnectCommand).toHaveBeenCalledWith(
      { id: "command-id", workspace_id: "workspace-id", host: "db.example.com", port: 5432 },
      expect.anything()
    );
    expect(mockGetCommandStatus).toHaveBeenCalledTimes(2);
    expect(mockGetConnectCommandOutput).toHaveBeenCalledWith({ id: "command-id" }, expect.anything());
  });

  it("rejects when the command was cancelled, without reading its output", async () => {
    mockGetCommandStatus.mockResolvedValue({ id: "command-id", status: "cancelled" });
    const { result } = renderConnectCommand();

    await expect(result.current(target)).rejects.toThrow("Connection test were not successful.");

    expect(mockGetConnectCommandOutput).not.toHaveBeenCalled();
  });

  it("resolves with undefined and does not poll when the run request returns a 409", async () => {
    mockRunConnectCommand.mockRejectedValue(httpError(409, "/commands/run/connect"));
    const { result } = renderConnectCommand();

    await expect(result.current(target)).resolves.toBeUndefined();

    expect(mockGetCommandStatus).not.toHaveBeenCalled();
    expect(mockGetConnectCommandOutput).not.toHaveBeenCalled();
  });

  it("rejects with any other error from the run request", async () => {
    const blockedAddressError = httpError(400, "/commands/run/connect");
    mockRunConnectCommand.mockRejectedValue(blockedAddressError);
    const { result } = renderConnectCommand();

    await expect(result.current(target)).rejects.toBe(blockedAddressError);

    expect(mockGetCommandStatus).not.toHaveBeenCalled();
  });

  it("rejects with a 409 from the output request, which does not mean that the server refused the command", async () => {
    const stillRunningError = httpError(409, "/commands/output/connect");
    mockGetConnectCommandOutput.mockRejectedValue(stillRunningError);
    const { result } = renderConnectCommand();

    await expect(result.current(target)).rejects.toBe(stillRunningError);
  });
});
