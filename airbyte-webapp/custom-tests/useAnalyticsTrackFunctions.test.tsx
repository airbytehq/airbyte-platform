import { renderHook, act } from "@testing-library/react";
import { useAnalyticsTrackFunctions } from "../src/views/Connector/ConnectorCard/useAnalyticsTrackFunctions";

// Mocks de dependencias
const mockTrack = jest.fn();

jest.mock("core/services/analytics", () => ({
  useAnalyticsService: () => ({ track: mockTrack }),
  Action: { TEST: "TEST", SUCCESS: "SUCCESS", FAILURE: "FAILURE" },
  Namespace: { SOURCE: "SOURCE", DESTINATION: "DESTINATION" },
}));

jest.mock("core/domain/connector", () => ({
  Connector: { id: (connector: any) => `id-${connector.name}` },
}));

describe("useAnalyticsTrackFunctions", () => {
  const mockConnector = {
    name: "MockConnector",
    documentationUrl: "https://docs.airbyte.io",
  } as any;

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("Compronar 'Test started' para source connector", () => {
    const { result } = renderHook(() => useAnalyticsTrackFunctions("source"));

    act(() => {
      result.current.trackTestConnectorStarted(mockConnector);
    });

    expect(mockTrack).toHaveBeenCalledWith("SOURCE", "TEST", {
      actionDescription: "Test a connector",
      connector: "MockConnector",
      connector_definition_id: "id-MockConnector",
      connector_documentation_url: "https://docs.airbyte.io",
      external_message: undefined,
      failure_reason: undefined,
    });
  });

  it("tracks 'Success' action for destination connector", () => {
    const { result } = renderHook(() => useAnalyticsTrackFunctions("destination"));

    act(() => {
      result.current.trackTestConnectorSuccess(mockConnector);
    });

    expect(mockTrack).toHaveBeenCalledWith("DESTINATION", "SUCCESS", expect.objectContaining({
      actionDescription: "Tested connector - success",
      connector: "MockConnector",
    }));
  });

  it("Comprobar 'Failure' incluyendo jobInfo y mensaje", () => {
    const { result } = renderHook(() => useAnalyticsTrackFunctions("source"));

    const jobInfo = { failureReason: { externalMessage: "API Error" } };
    const message = "Test failed due to timeout";

    act(() => {
      result.current.trackTestConnectorFailure(mockConnector, jobInfo as any, message);
    });

    expect(mockTrack).toHaveBeenCalledWith("SOURCE", "FAILURE", expect.objectContaining({
      actionDescription: "Tested connector - failure",
      external_message: "API Error",
      failure_reason: jobInfo.failureReason,
    }));
  });

  it("no hacer nada si connector es undefined", () => {
    const { result } = renderHook(() => useAnalyticsTrackFunctions("source"));

    act(() => {
      result.current.trackTestConnectorStarted(undefined);
    });

    expect(mockTrack).not.toHaveBeenCalled();
  });
});
