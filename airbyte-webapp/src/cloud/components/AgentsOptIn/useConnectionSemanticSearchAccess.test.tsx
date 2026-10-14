import { renderHook } from "@testing-library/react";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import {
  useAgentsSupportedDestinationDefinitionIds,
  useAgentsSupportedSourceDefinitionIds,
  useOrganizationSemanticSearchEnablement,
} from "core/api";
import { useExperiment } from "core/services/Experiment";
import { useIsCloudApp } from "core/utils/app";
import { useIntent } from "core/utils/rbac";

import { useConnectionSemanticSearchAccess } from "./useConnectionSemanticSearchAccess";
import { useContextLayerEligibility } from "./useContextLayerEligibility";

jest.mock("area/workspace/utils", () => ({ useCurrentWorkspaceId: jest.fn() }));
jest.mock("core/api", () => ({
  useAgentsSupportedDestinationDefinitionIds: jest.fn(),
  useAgentsSupportedSourceDefinitionIds: jest.fn(),
  useOrganizationSemanticSearchEnablement: jest.fn(),
}));
jest.mock("core/services/Experiment", () => ({ useExperiment: jest.fn() }));
jest.mock("core/utils/app", () => ({ useIsCloudApp: jest.fn() }));
jest.mock("core/utils/rbac", () => ({ useIntent: jest.fn() }));
jest.mock("./useContextLayerEligibility", () => ({ useContextLayerEligibility: jest.fn() }));

const mockUseCurrentWorkspaceId = useCurrentWorkspaceId as jest.MockedFunction<typeof useCurrentWorkspaceId>;
const mockUseSupportedSourceIds = useAgentsSupportedSourceDefinitionIds as jest.MockedFunction<
  typeof useAgentsSupportedSourceDefinitionIds
>;
const mockUseSupportedDestinationIds = useAgentsSupportedDestinationDefinitionIds as jest.MockedFunction<
  typeof useAgentsSupportedDestinationDefinitionIds
>;
const mockUseOrganizationSetting = useOrganizationSemanticSearchEnablement as jest.MockedFunction<
  typeof useOrganizationSemanticSearchEnablement
>;
const mockUseExperiment = useExperiment as jest.MockedFunction<typeof useExperiment>;
const mockUseIsCloudApp = useIsCloudApp as jest.MockedFunction<typeof useIsCloudApp>;
const mockUseIntent = useIntent as jest.MockedFunction<typeof useIntent>;
const mockUseContextLayerEligibility = useContextLayerEligibility as jest.MockedFunction<
  typeof useContextLayerEligibility
>;

const SUPPORTED_SOURCE = "supported-source";
const SUPPORTED_DESTINATION = "supported-destination";

const organizationSetting = (semanticSearchEnabled?: boolean, overrides: Record<string, unknown> = {}) =>
  ({
    data: semanticSearchEnabled === undefined ? undefined : { organizationId: "org-1", semanticSearchEnabled },
    isInitialLoading: false,
    isError: false,
    ...overrides,
  }) as never;

const eligibility = (isEligible: boolean, isInitialLoading = false) =>
  ({ isEligible, statusQuery: { isInitialLoading } }) as never;

const renderAccess = (
  definitionIds: { sourceDefinitionId?: string; destinationDefinitionId?: string } = {
    sourceDefinitionId: SUPPORTED_SOURCE,
    destinationDefinitionId: SUPPORTED_DESTINATION,
  }
) => renderHook(() => useConnectionSemanticSearchAccess(definitionIds)).result.current;

describe("useConnectionSemanticSearchAccess", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockUseIsCloudApp.mockReturnValue(true);
    mockUseExperiment.mockReturnValue(true);
    mockUseContextLayerEligibility.mockReturnValue(eligibility(true));
    mockUseSupportedSourceIds.mockReturnValue(new Set([SUPPORTED_SOURCE]));
    mockUseSupportedDestinationIds.mockReturnValue(new Set([SUPPORTED_DESTINATION]));
    mockUseOrganizationSetting.mockReturnValue(organizationSetting(true));
    mockUseCurrentWorkspaceId.mockReturnValue("workspace-1");
    mockUseIntent.mockReturnValue(true);
  });

  it("is editable when every check passes", () => {
    expect(renderAccess()).toBe("editable");
    expect(mockUseContextLayerEligibility).toHaveBeenCalledWith();
    expect(mockUseOrganizationSetting).toHaveBeenCalledWith({ enabled: true });
    expect(mockUseExperiment).toHaveBeenCalledWith("platform.fusion-semantic-search-ui");
  });

  it.each([
    ["the app is not Cloud", () => mockUseIsCloudApp.mockReturnValue(false)],
    ["the semantic search UI flag is off", () => mockUseExperiment.mockReturnValue(false)],
  ])("is hidden and does not read the organization setting when %s", (_, arrange) => {
    arrange();

    expect(renderAccess()).toBe("hidden");
    expect(mockUseOrganizationSetting).toHaveBeenCalledWith({ enabled: false });
  });

  it("is hidden and does not read the organization setting when the Context Layer is not available", () => {
    mockUseContextLayerEligibility.mockReturnValue(eligibility(false));

    expect(renderAccess()).toBe("hidden");
    expect(mockUseOrganizationSetting).toHaveBeenCalledWith({ enabled: false });
  });

  it.each([
    [
      "the source is not an agent connector",
      { sourceDefinitionId: "other", destinationDefinitionId: SUPPORTED_DESTINATION },
    ],
    [
      "the destination is not an agent connector",
      { sourceDefinitionId: SUPPORTED_SOURCE, destinationDefinitionId: "other" },
    ],
    ["the source is unknown", { destinationDefinitionId: SUPPORTED_DESTINATION }],
    ["the destination is unknown", { sourceDefinitionId: SUPPORTED_SOURCE }],
  ])("is hidden and does not read the organization setting when %s", (_, definitionIds) => {
    expect(renderAccess(definitionIds)).toBe("hidden");
    expect(mockUseOrganizationSetting).toHaveBeenCalledWith({ enabled: false });
  });

  it("is loading while the Context Layer status loads", () => {
    mockUseContextLayerEligibility.mockReturnValue(eligibility(false, true));

    expect(renderAccess()).toBe("loading");
  });

  it("is loading while the organization setting loads", () => {
    mockUseOrganizationSetting.mockReturnValue(organizationSetting(undefined, { isInitialLoading: true }));

    expect(renderAccess()).toBe("loading");
  });

  it("is hidden when the organization setting fails to load", () => {
    mockUseOrganizationSetting.mockReturnValue(organizationSetting(undefined, { isError: true }));

    expect(renderAccess()).toBe("hidden");
  });

  it("is orgOff when semantic search is off for the organization, before the permission check", () => {
    mockUseOrganizationSetting.mockReturnValue(organizationSetting(false));
    mockUseIntent.mockReturnValue(false);

    expect(renderAccess()).toBe("orgOff");
  });

  it("is noPermission when the viewer cannot edit semantic search in the current workspace", () => {
    mockUseIntent.mockReturnValue(false);

    expect(renderAccess()).toBe("noPermission");
    expect(mockUseIntent).toHaveBeenCalledWith("EditConnectionSemanticSearch", { workspaceId: "workspace-1" });
  });
});
