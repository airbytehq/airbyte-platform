import { UseQueryResult } from "@tanstack/react-query";
import { renderHook } from "@testing-library/react";

import { useGetScimConfig } from "core/api";
import { ScimConfigResponse, ScimConfigStatus } from "core/api/types/AirbyteClient";
import { useExperiment } from "core/services/Experiment";
import { Intent, INTENTS } from "core/utils/rbac/generated-intents";
import { useGeneratedIntent } from "core/utils/rbac/useGeneratedIntent";

import { useCurrentOrganizationId } from "./useCurrentOrganizationId";
import { useScimSettingsAccess } from "./useScimSettingsAccess";

jest.mock("core/api", () => ({ useGetScimConfig: jest.fn() }));
jest.mock("core/services/Experiment", () => ({ useExperiment: jest.fn() }));
jest.mock("core/utils/rbac/useGeneratedIntent", () => ({ useGeneratedIntent: jest.fn() }));
jest.mock("./useCurrentOrganizationId", () => ({ useCurrentOrganizationId: jest.fn() }));

const mockUseGetScimConfig = useGetScimConfig as unknown as jest.Mock<
  Partial<UseQueryResult<ScimConfigResponse, unknown>>
>;
const mockUseExperiment = useExperiment as jest.MockedFunction<typeof useExperiment>;
const mockUseGeneratedIntent = useGeneratedIntent as jest.MockedFunction<typeof useGeneratedIntent>;
const mockUseCurrentOrganizationId = useCurrentOrganizationId as jest.MockedFunction<typeof useCurrentOrganizationId>;

// A fully-valid ScimConfigResponse so generated-type drift (e.g. a new required field) breaks typecheck here.
const scimConfigResponse: ScimConfigResponse = {
  status: ScimConfigStatus.enabled,
  scimBaseUrl: "https://cloud.airbyte.com/scim/v2/organizations/org-123",
  available: true,
};

describe("useScimSettingsAccess", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockUseCurrentOrganizationId.mockReturnValue("org-123");
    mockUseExperiment.mockReturnValue(true);
    mockUseGeneratedIntent.mockReturnValue(true);
    mockUseGetScimConfig.mockReturnValue({ data: undefined, isInitialLoading: false, isError: false });
  });

  it("gates off when the flag is off, even for an admin", () => {
    mockUseExperiment.mockReturnValue(false);

    const { result } = renderHook(() => useScimSettingsAccess());

    expect(result.current.canManageScim).toBe(false);
    expect(mockUseGetScimConfig).toHaveBeenCalledWith({ enabled: false });
    expect(result.current.scimConfig).toBeUndefined();
  });

  it("gates off for a non-admin, even when the flag is on", () => {
    mockUseGeneratedIntent.mockReturnValue(false);

    const { result } = renderHook(() => useScimSettingsAccess());

    expect(result.current.canManageScim).toBe(false);
    expect(mockUseGetScimConfig).toHaveBeenCalledWith({ enabled: false });
  });

  it("gates off when there is no organization id", () => {
    mockUseCurrentOrganizationId.mockReturnValue("");

    const { result } = renderHook(() => useScimSettingsAccess());

    expect(result.current.canManageScim).toBe(false);
    expect(mockUseGetScimConfig).toHaveBeenCalledWith({ enabled: false });
  });

  it("gates on and exposes an available config when flag, admin, and org id all line up", () => {
    mockUseGetScimConfig.mockReturnValue({ data: scimConfigResponse, isInitialLoading: false, isError: false });

    const { result } = renderHook(() => useScimSettingsAccess());

    expect(result.current.canManageScim).toBe(true);
    expect(mockUseGetScimConfig).toHaveBeenCalledWith({ enabled: true });
    expect(result.current.scimConfig).toEqual(scimConfigResponse);
    expect(result.current.isScimAvailable).toBe(true);
  });

  it("reports unavailable while still exposing the config when available is false", () => {
    mockUseGetScimConfig.mockReturnValue({
      data: { ...scimConfigResponse, available: false },
      isInitialLoading: false,
      isError: false,
    });

    const { result } = renderHook(() => useScimSettingsAccess());

    expect(result.current.isScimAvailable).toBe(false);
    expect(result.current.scimConfig).toEqual({ ...scimConfigResponse, available: false });
  });

  it("discards stale cached data once the gate is closed", () => {
    mockUseExperiment.mockReturnValue(false);
    mockUseGetScimConfig.mockReturnValue({ data: scimConfigResponse, isInitialLoading: false, isError: false });

    const { result } = renderHook(() => useScimSettingsAccess());

    expect(result.current.scimConfig).toBeUndefined();
    expect(result.current.isScimAvailable).toBe(false);
  });

  it("suppresses isLoading when gated out, even if the underlying query reports loading", () => {
    mockUseExperiment.mockReturnValue(false);
    mockUseGetScimConfig.mockReturnValue({ data: undefined, isInitialLoading: true, isError: false });

    const { result } = renderHook(() => useScimSettingsAccess());

    expect(result.current.isLoading).toBe(false);
  });

  it("reports isLoading when gated in and the underlying query is loading", () => {
    mockUseGetScimConfig.mockReturnValue({ data: undefined, isInitialLoading: true, isError: false });

    const { result } = renderHook(() => useScimSettingsAccess());

    expect(result.current.isLoading).toBe(true);
  });

  it("reports isError when gated in and the underlying query fails", () => {
    mockUseGetScimConfig.mockReturnValue({ data: undefined, isInitialLoading: false, isError: true });

    const { result } = renderHook(() => useScimSettingsAccess());

    expect(result.current.isError).toBe(true);
    expect(result.current.isScimAvailable).toBe(false);
  });

  it("suppresses isError when gated out, even if the underlying query reports an error", () => {
    mockUseExperiment.mockReturnValue(false);
    mockUseGetScimConfig.mockReturnValue({ data: undefined, isInitialLoading: false, isError: true });

    const { result } = renderHook(() => useScimSettingsAccess());

    expect(result.current.isError).toBe(false);
  });

  it("wires the flag key and intent as pinned", () => {
    renderHook(() => useScimSettingsAccess());

    expect(mockUseExperiment).toHaveBeenCalledWith("settings.scimProvisioning");
    expect(mockUseGeneratedIntent).toHaveBeenCalledWith(Intent.UpdateOrganizationPermissions, {
      organizationId: "org-123",
    });
  });
});

describe("Intent.UpdateOrganizationPermissions drift guard", () => {
  it("resolves to exactly the organization-admin/instance-admin allow-list backing the scim_config endpoints' @Secured(ORGANIZATION_ADMIN) check", () => {
    expect(INTENTS[Intent.UpdateOrganizationPermissions].roles).toEqual(["organization_admin", "instance_admin"]);
  });
});
