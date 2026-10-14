import { renderHook } from "@testing-library/react";

import { useExperiment } from "core/services/Experiment";
import { useIsCloudApp } from "core/utils/app";

import { useConnectionSemanticSearchAvailability } from "./useConnectionSemanticSearchAvailability";
import { useContextLayerEligibility } from "./useContextLayerEligibility";

jest.mock("core/services/Experiment", () => ({ useExperiment: jest.fn() }));
jest.mock("core/utils/app", () => ({ useIsCloudApp: jest.fn() }));
jest.mock("./useContextLayerEligibility", () => ({ useContextLayerEligibility: jest.fn() }));

const mockUseExperiment = useExperiment as jest.MockedFunction<typeof useExperiment>;
const mockUseIsCloudApp = useIsCloudApp as jest.MockedFunction<typeof useIsCloudApp>;
const mockUseContextLayerEligibility = useContextLayerEligibility as jest.MockedFunction<
  typeof useContextLayerEligibility
>;

const eligibility = (isEligible: boolean, isInitialLoading = false) =>
  ({ isEligible, statusQuery: { isInitialLoading } }) as never;

describe("useConnectionSemanticSearchAvailability", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockUseIsCloudApp.mockReturnValue(true);
    mockUseExperiment.mockReturnValue(true);
    mockUseContextLayerEligibility.mockReturnValue(eligibility(true));
  });

  it("is available in Cloud with the flag on and the Context Layer available", () => {
    expect(renderHook(() => useConnectionSemanticSearchAvailability()).result.current).toBe("available");
    expect(mockUseExperiment).toHaveBeenCalledWith("platform.fusion-semantic-search-ui");
  });

  it("is loading while the Context Layer status loads", () => {
    mockUseContextLayerEligibility.mockReturnValue(eligibility(false, true));

    expect(renderHook(() => useConnectionSemanticSearchAvailability()).result.current).toBe("loading");
  });

  it("is unavailable when the Context Layer is not available", () => {
    mockUseContextLayerEligibility.mockReturnValue(eligibility(false));

    expect(renderHook(() => useConnectionSemanticSearchAvailability()).result.current).toBe("unavailable");
  });

  it.each([
    ["the app is not Cloud", () => mockUseIsCloudApp.mockReturnValue(false)],
    ["the semantic search UI flag is off", () => mockUseExperiment.mockReturnValue(false)],
  ])("is unavailable, not loading, when %s", (_, arrange) => {
    arrange();
    mockUseContextLayerEligibility.mockReturnValue(eligibility(false, true));

    expect(renderHook(() => useConnectionSemanticSearchAvailability()).result.current).toBe("unavailable");
  });
});
