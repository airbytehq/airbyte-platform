import { renderHook } from "@testing-library/react";
import { useLinkToBillingPage } from "../src/packages/cloud/area/billing/utils/useLinkToBillingPage";

// Mocks de dependencias
jest.mock("area/organization/utils", () => ({
  useCurrentOrganizationId: jest.fn(),
}));

jest.mock("area/workspace/utils", () => ({
  useCurrentWorkspaceId: jest.fn(),
}));

jest.mock("hooks/services/Experiment", () => ({
  useExperiment: jest.fn(),
}));

jest.mock("packages/cloud/views/settings/routePaths", () => ({
  CloudSettingsRoutePaths: { Billing: "billing" },
}));

jest.mock("pages/routePaths", () => ({
  RoutePaths: {
    Organization: "organizations",
    Workspaces: "workspaces",
    Settings: "settings",
  },
}));

const { useCurrentOrganizationId } = jest.requireMock("area/organization/utils");
const { useCurrentWorkspaceId } = jest.requireMock("area/workspace/utils");
const { useExperiment } = jest.requireMock("hooks/services/Experiment");

describe("useLinkToBillingPage", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    (useCurrentOrganizationId as jest.Mock).mockReturnValue("org-123");
    (useCurrentWorkspaceId as jest.Mock).mockReturnValue("ws-456");
  });

  it("retorna la ruta del workspace cuando el experimento está desactivado", () => {
    (useExperiment as jest.Mock).mockReturnValue(false);

    const { result } = renderHook(() => useLinkToBillingPage());

    expect(result.current).toBe("/workspaces/ws-456/settings/billing");
    expect(useCurrentWorkspaceId).toHaveBeenCalled();
    expect(useCurrentOrganizationId).toHaveBeenCalled();
  });

  it("retorna la ruta de la organización cuando el experimento está activado", () => {
    (useExperiment as jest.Mock).mockReturnValue(true);

    const { result } = renderHook(() => useLinkToBillingPage());

    expect(result.current).toBe("/organizations/org-123/settings/billing");
    expect(useCurrentOrganizationId).toHaveBeenCalled();
    expect(useCurrentWorkspaceId).toHaveBeenCalled();
  });
});
