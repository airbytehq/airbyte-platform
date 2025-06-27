import { render, waitFor } from "@testing-library/react";
import React from "react";
import { MemoryRouter } from "react-router-dom";

import { getEntitlements, useRequestOptions } from "core/api";
import { FeatureItem, useFeatureService } from "core/services/features";

import { AirbyteEntitlementProvider } from "./EntitlementService";
import { useCurrentOrganizationId } from "../../../area/organization/utils";

// Top-level mocks
jest.mock("core/api", () => ({
  getEntitlements: jest.fn(),
  useRequestOptions: jest.fn(),
  useCurrentWorkspaceOrUndefined: jest.fn(),
}));

jest.mock("core/services/features", () => ({
  ...jest.requireActual("core/services/features"),
  useFeatureService: jest.fn(),
}));

jest.mock("../../../area/organization/utils", () => ({
  useCurrentOrganizationId: jest.fn(),
}));

describe("EntitlementService", () => {
  const mockOrganizationId = "test-org-id";
  const mockRequestOptions = { getAccessToken: jest.fn() };
  const mockSetEntitlementOverwrites = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
    (useCurrentOrganizationId as jest.Mock).mockReturnValue(mockOrganizationId);
    (useRequestOptions as jest.Mock).mockReturnValue(mockRequestOptions);
    (useFeatureService as jest.Mock).mockReturnValue({
      setEntitlementOverwrites: mockSetEntitlementOverwrites,
    });
  });

  const wrapper: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => (
    <MemoryRouter initialEntries={["/organization/test-org-id/settings"]}>
      <AirbyteEntitlementProvider>{children}</AirbyteEntitlementProvider>
    </MemoryRouter>
  );

  it("should fetch and map entitlements correctly", async () => {
    const mockEntitlements = [
      { feature_id: "feature-fe-allow-all-rbac-roles", is_entitled: true },
      { feature_id: "feature-connection-hashing-ui-v0", is_entitled: false },
      { feature_id: "feature-fe-cloud-for-teams-branding", is_entitled: true },
    ];

    (getEntitlements as jest.Mock).mockResolvedValue({ entitlements: mockEntitlements });

    render(<div>Test</div>, { wrapper });

    await waitFor(() => {
      expect(getEntitlements).toHaveBeenCalledWith({ organization_id: mockOrganizationId }, mockRequestOptions);
      expect(mockSetEntitlementOverwrites).toHaveBeenCalledWith({
        [FeatureItem.AllowAllRBACRoles]: true,
        [FeatureItem.FieldHashing]: false,
        [FeatureItem.CloudForTeamsBranding]: true,
      });
    });
  });

  it("should handle unknown feature IDs gracefully", async () => {
    const mockEntitlements = [
      { feature_id: "unknown-feature", is_entitled: true },
      { feature_id: "feature-fe-allow-all-rbac-roles", is_entitled: true },
    ];

    (getEntitlements as jest.Mock).mockResolvedValue({ entitlements: mockEntitlements });

    render(<div>Test</div>, { wrapper });

    await waitFor(() => {
      expect(mockSetEntitlementOverwrites).toHaveBeenCalledWith({
        [FeatureItem.AllowAllRBACRoles]: true,
      });
    });
  });

  it("should not fetch entitlements when organization ID is not available", async () => {
    (useCurrentOrganizationId as jest.Mock).mockReturnValue(undefined);

    render(<div>Test</div>, { wrapper });

    await waitFor(() => {
      expect(getEntitlements).not.toHaveBeenCalled();
      expect(mockSetEntitlementOverwrites).not.toHaveBeenCalled();
    });
  });

  it("should handle API errors gracefully", async () => {
    const consoleErrorSpy = jest.spyOn(console, "error").mockImplementation();
    const mockError = new Error("API Error");

    (getEntitlements as jest.Mock).mockRejectedValue(mockError);

    render(<div>Test</div>, { wrapper });

    await waitFor(() => {
      expect(consoleErrorSpy).toHaveBeenCalledWith("Failed to fetch entitlements from Airbyte server", mockError);
      expect(mockSetEntitlementOverwrites).not.toHaveBeenCalled();
    });

    consoleErrorSpy.mockRestore();
  });

  it("should refresh entitlements when organization ID changes", async () => {
    const { rerender } = render(<div>Test</div>, { wrapper });

    const newOrganizationId = "new-org-id";
    (useCurrentOrganizationId as jest.Mock).mockReturnValue(newOrganizationId);

    rerender(<div>Test</div>);

    await waitFor(() => {
      expect(getEntitlements).toHaveBeenCalledWith({ organization_id: newOrganizationId }, mockRequestOptions);
    });
  });
});
