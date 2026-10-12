import { fireEvent, render, screen } from "@testing-library/react";
import { Route, Routes } from "react-router-dom";

import { TestWrapper } from "test-utils";

import { useCurrentOrganizationId } from "area/organization/utils";
import { useAgentsProvisioningStatusQuery, useOrganizationSemanticSearchEnablement } from "core/api";

import { SemanticSearchPage } from "./SemanticSearchPage";
import { useShowAgentsOptIn } from "./useShowAgentsOptIn";

jest.mock("area/organization/utils", () => ({
  useCurrentOrganizationId: jest.fn(),
}));

jest.mock("core/api", () => ({
  useAgentsProvisioningStatusQuery: jest.fn(),
  useOrganizationSemanticSearchEnablement: jest.fn(),
}));

jest.mock("core/utils/app", () => ({
  useIsCloudApp: () => true,
}));

jest.mock("./useShowAgentsOptIn", () => ({ useShowAgentsOptIn: jest.fn() }));

const mockUseCurrentOrganizationId = useCurrentOrganizationId as jest.MockedFunction<typeof useCurrentOrganizationId>;
const mockUseAgentsProvisioningStatusQuery = useAgentsProvisioningStatusQuery as jest.MockedFunction<
  typeof useAgentsProvisioningStatusQuery
>;
const mockUseOrganizationSemanticSearchEnablement = useOrganizationSemanticSearchEnablement as jest.MockedFunction<
  typeof useOrganizationSemanticSearchEnablement
>;
const mockUseShowAgentsOptIn = useShowAgentsOptIn as jest.MockedFunction<typeof useShowAgentsOptIn>;

const renderPage = () =>
  render(
    <TestWrapper route="/organization/test-org-123/context-layer/semantic-search">
      <Routes>
        <Route path="/organization/:organizationId/context-layer/semantic-search" element={<SemanticSearchPage />} />
        <Route path="/organization/:organizationId/context-layer" element={<div>Context layer settings page</div>} />
      </Routes>
    </TestWrapper>
  );

describe("SemanticSearchPage", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockUseCurrentOrganizationId.mockReturnValue("test-org-123");
    mockUseShowAgentsOptIn.mockReturnValue(true);
    mockUseAgentsProvisioningStatusQuery.mockReturnValue({
      data: { is_enrolled: true, external_cloud_eligible: true },
      isInitialLoading: false,
      isError: false,
      refetch: jest.fn(),
    } as never);
    mockUseOrganizationSemanticSearchEnablement.mockReturnValue({
      data: { organizationId: "test-org-123", semanticSearchEnabled: true },
      isInitialLoading: false,
      isError: false,
      refetch: jest.fn(),
    } as never);
  });

  describe("gating", () => {
    it.each([
      {
        name: "the organization is not eligible",
        status: { is_enrolled: false, external_cloud_eligible: false },
        showAgentsOptIn: true,
      },
      {
        name: "the opt-in is hidden for an unenrolled organization",
        status: { is_enrolled: false, external_cloud_eligible: true },
        showAgentsOptIn: false,
      },
    ])("shows the unavailable state when $name", ({ status, showAgentsOptIn }) => {
      mockUseShowAgentsOptIn.mockReturnValue(showAgentsOptIn);
      mockUseAgentsProvisioningStatusQuery.mockReturnValue({ data: status, isError: false } as never);

      renderPage();

      expect(screen.getByTestId("context-layer-unavailable")).toBeInTheDocument();
      expect(screen.queryByRole("heading", { name: "Semantic search" })).not.toBeInTheDocument();
    });

    it("does not read the semantic search setting when the page is unavailable", () => {
      mockUseAgentsProvisioningStatusQuery.mockReturnValue({
        data: { is_enrolled: false, external_cloud_eligible: false },
        isError: false,
      } as never);

      renderPage();

      expect(screen.getByTestId("context-layer-unavailable")).toBeInTheDocument();
      expect(mockUseOrganizationSemanticSearchEnablement).not.toHaveBeenCalled();
    });

    it("shows the unavailable state when the provisioning status is empty", () => {
      mockUseAgentsProvisioningStatusQuery.mockReturnValue({ data: null, isError: false } as never);

      renderPage();

      expect(screen.getByTestId("context-layer-unavailable")).toBeInTheDocument();
    });

    it("waits for the provisioning status", () => {
      mockUseAgentsProvisioningStatusQuery.mockReturnValue({ isInitialLoading: true } as never);

      renderPage();

      expect(screen.queryByRole("heading", { name: "Semantic search" })).not.toBeInTheDocument();
      expect(screen.queryByTestId("context-layer-unavailable")).not.toBeInTheDocument();
    });

    it("shows a load error with retry when the provisioning status fails", () => {
      const refetch = jest.fn();
      mockUseAgentsProvisioningStatusQuery.mockReturnValue({ isError: true, refetch } as never);

      renderPage();

      expect(screen.getByTestId("context-layer-load-error")).toBeInTheDocument();
      fireEvent.click(screen.getByRole("button", { name: "Try again" }));
      expect(refetch).toHaveBeenCalledTimes(1);
    });
  });

  describe("organization semantic search setting", () => {
    it.each([["off", { organizationId: "test-org-123", semanticSearchEnabled: false }]])(
      "asks to enable semantic search when the setting is %s",
      (_name, data) => {
        mockUseOrganizationSemanticSearchEnablement.mockReturnValue({
          data,
          isInitialLoading: false,
          isError: false,
        } as never);

        renderPage();

        expect(screen.getByRole("heading", { name: "Semantic search" })).toBeInTheDocument();
        expect(
          screen.getByText("Choose which connections populate your context layer for semantic search.")
        ).toBeInTheDocument();
        expect(screen.getByText("Enable semantic search")).toBeInTheDocument();
        expect(screen.getByText("Before you can control semantic search, you need to enable it.")).toBeInTheDocument();
      }
    );

    it("sends Go to settings to the Context layer settings page", () => {
      mockUseOrganizationSemanticSearchEnablement.mockReturnValue({
        data: { organizationId: "test-org-123", semanticSearchEnabled: false },
        isInitialLoading: false,
        isError: false,
      } as never);

      renderPage();

      fireEvent.click(screen.getByRole("button", { name: "Go to settings" }));
      expect(screen.getByText("Context layer settings page")).toBeInTheDocument();
    });

    it("waits for the setting", () => {
      mockUseOrganizationSemanticSearchEnablement.mockReturnValue({ isInitialLoading: true, isError: false } as never);

      renderPage();

      expect(screen.queryByRole("heading", { name: "Semantic search" })).not.toBeInTheDocument();
      expect(screen.queryByText("Enable semantic search")).not.toBeInTheDocument();
    });

    it("shows a load error with retry when the setting cannot be read", () => {
      const refetch = jest.fn();
      mockUseOrganizationSemanticSearchEnablement.mockReturnValue({
        isInitialLoading: false,
        isError: true,
        refetch,
      } as never);

      renderPage();

      expect(screen.getByTestId("context-layer-load-error")).toBeInTheDocument();
      expect(screen.queryByText("Enable semantic search")).not.toBeInTheDocument();
      fireEvent.click(screen.getByRole("button", { name: "Try again" }));
      expect(refetch).toHaveBeenCalledTimes(1);
    });
  });
});
