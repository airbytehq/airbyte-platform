import { act, renderHook } from "@testing-library/react";

import { mockDestination, mockSource, TestWrapper } from "test-utils";

import { useTestConnectorCommand } from "./commands";
import { getCheckCommandOutput, getCommandStatus, runCheckCommand } from "../generated/AirbyteClient";

jest.mock("uuid", () => ({ v4: () => "command-id" }));

jest.mock("../generated/AirbyteClient", () => ({
  cancelCommand: jest.fn(),
  getCheckCommandOutput: jest.fn(),
  getCommandStatus: jest.fn(),
  runCheckCommand: jest.fn(),
}));

jest.mock("../useRequestOptions", () => ({ useRequestOptions: () => ({}) }));
jest.mock("area/workspace/utils", () => ({ useCurrentWorkspaceId: () => "workspace-id" }));

const mockRunCheckCommand = runCheckCommand as jest.MockedFunction<typeof runCheckCommand>;
const mockGetCommandStatus = getCommandStatus as jest.MockedFunction<typeof getCommandStatus>;
const mockGetCheckCommandOutput = getCheckCommandOutput as jest.MockedFunction<typeof getCheckCommandOutput>;

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
