import { screen } from "@testing-library/react";
import { Route, Routes } from "react-router-dom";

import { render, TestWrapper } from "test-utils";

import { useCurrentOrganizationId } from "area/organization/utils";
import { useAgentsProvisioningStatusQuery } from "core/api";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

import { ContextLayerPage } from "./ContextLayerPage";

jest.mock("area/organization/utils", () => ({
  useCurrentOrganizationId: jest.fn(() => "test-organization-id"),
}));

jest.mock("core/api", () => ({ useAgentsProvisioningStatusQuery: jest.fn() }));
jest.mock("core/utils/app", () => ({ useIsCloudApp: () => true }));
jest.mock("core/utils/rbac", () => ({
  Intent: { UpdateOrganizationPermissions: "UpdateOrganizationPermissions" },
  useGeneratedIntent: jest.fn(),
}));

jest.mock("components/ui/HeadTitle", () => ({
  HeadTitle: ({ titles }: { titles: Array<{ id: string }> }) => (
    <div data-testid="head-title">{titles.map(({ id }) => id).join("|")}</div>
  ),
}));

const mockUseCurrentOrganizationId = useCurrentOrganizationId as jest.MockedFunction<typeof useCurrentOrganizationId>;
const mockUseAgentsProvisioningStatusQuery = useAgentsProvisioningStatusQuery as jest.MockedFunction<
  typeof useAgentsProvisioningStatusQuery
>;
const mockUseGeneratedIntent = useGeneratedIntent as jest.MockedFunction<typeof useGeneratedIntent>;

const renderAt = async (path: string) =>
  render(<ContextLayerPage />, {
    wrapper: ({ children }) => (
      <TestWrapper route={path}>
        <Routes>
          <Route path="/organization/:organizationId/context-layer" element={children}>
            <Route index element={<div>Context Layer settings content</div>} />
            <Route path="agent-access" element={<div>Agent access content</div>} />
          </Route>
          <Route path="/organization/:organizationId/workspaces" element={<div>Workspaces content</div>} />
        </Routes>
      </TestWrapper>
    ),
  });

describe("ContextLayerPage", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockUseCurrentOrganizationId.mockReturnValue("test-organization-id");
    mockUseAgentsProvisioningStatusQuery.mockReturnValue({ data: { is_enrolled: true } } as never);
    mockUseGeneratedIntent.mockReturnValue(false);
  });

  it("owns the Context Layer secondary navigation and renders its content", async () => {
    await renderAt("/organization/test-organization-id/context-layer");

    expect(screen.getByText("Context Layer")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Settings" })).toHaveAttribute(
      "href",
      "/organization/test-organization-id/context-layer"
    );
    expect(screen.getByRole("link", { name: "Settings" })).toHaveAttribute("aria-current", "page");
    expect(screen.getByRole("link", { name: "Agent access" })).toHaveAttribute(
      "href",
      "/organization/test-organization-id/context-layer/agent-access"
    );
    expect(screen.getByRole("link", { name: "Agent access" }).querySelector('[data-icon="robot"]')).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "Sources" })).not.toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "Destinations" })).not.toBeInTheDocument();
    expect(screen.getByText("Context Layer settings content")).toBeInTheDocument();
    expect(screen.getByTestId("head-title")).toHaveTextContent("cloud.contextLayer.navigation.title");
  });

  it("marks Agent access as the active child page", async () => {
    await renderAt("/organization/test-organization-id/context-layer/agent-access");

    expect(screen.getByRole("link", { name: "Agent access" })).toHaveAttribute("aria-current", "page");
    expect(screen.getByRole("link", { name: "Settings" })).not.toHaveAttribute("aria-current");
  });

  it("waits for provisioning status before showing navigation", async () => {
    mockUseAgentsProvisioningStatusQuery.mockReturnValue({ isInitialLoading: true } as never);
    await renderAt("/organization/test-organization-id/context-layer/agent-access");
    expect(screen.queryByRole("link", { name: "Agent access" })).not.toBeInTheDocument();
  });

  it("keeps the navigation visible to an eligible admin after disabling the Context Layer", async () => {
    mockUseAgentsProvisioningStatusQuery.mockReturnValue({
      data: {
        is_enrolled: false,
        external_cloud_eligible: true,
        eligible_external_organization_id: "test-organization-id",
      },
    } as never);
    mockUseGeneratedIntent.mockImplementation((intent) => intent === Intent.UpdateOrganizationPermissions);
    await renderAt("/organization/test-organization-id/context-layer/agent-access");
    expect(screen.getByText("Agent access content")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Agent access" })).toHaveAttribute("aria-current", "page");
  });

  it("redirects disabled non-admin direct URLs", async () => {
    mockUseAgentsProvisioningStatusQuery.mockReturnValue({
      data: { is_enrolled: false, external_cloud_eligible: true },
    } as never);
    await renderAt("/organization/test-organization-id/context-layer/agent-access");
    expect(screen.getByText("Workspaces content")).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "Agent access" })).not.toBeInTheDocument();
  });

  it("hides secondary navigation for an ineligible admin", async () => {
    mockUseAgentsProvisioningStatusQuery.mockReturnValue({
      data: { is_enrolled: false, external_cloud_eligible: false },
    } as never);
    mockUseGeneratedIntent.mockReturnValue(true);
    await renderAt("/organization/test-organization-id/context-layer/agent-access");
    expect(screen.getByText("Agent access content")).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "Agent access" })).not.toBeInTheDocument();
  });

  it("fails closed for a non-admin when provisioning status fails", async () => {
    mockUseAgentsProvisioningStatusQuery.mockReturnValue({ isError: true } as never);
    await renderAt("/organization/test-organization-id/context-layer/agent-access");
    expect(screen.getByText("Workspaces content")).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "Agent access" })).not.toBeInTheDocument();
  });

  it("retains the admin page for a provisioning error and retry", async () => {
    mockUseAgentsProvisioningStatusQuery.mockReturnValue({ isError: true } as never);
    mockUseGeneratedIntent.mockReturnValue(true);
    await renderAt("/organization/test-organization-id/context-layer");
    expect(screen.getByText("Context Layer settings content")).toBeInTheDocument();
  });
});
