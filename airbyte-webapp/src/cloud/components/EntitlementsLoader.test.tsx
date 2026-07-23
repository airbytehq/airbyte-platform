import { render } from "@testing-library/react";
import React from "react";
import { MemoryRouter } from "react-router-dom";

import { useCurrentOrganizationId } from "area/organization/utils";
import { useSetEntitlements } from "core/api";
import { FeatureService } from "core/services/features/FeatureService";
import { FeatureItem } from "core/services/features/types";

import { EntitlementsLoader } from "./EntitlementsLoader";

jest.mock("core/api", () => ({
  useSetEntitlements: jest.fn(),
}));

jest.mock("area/organization/utils", () => ({
  useCurrentOrganizationId: jest.fn(),
}));

describe("EntitlementsLoader", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    (useCurrentOrganizationId as jest.Mock).mockReturnValue("test-org-id");
    (useSetEntitlements as jest.Mock).mockImplementation(() => {});
  });

  const TestComponent: React.FC = () => <div data-testid="test-content">Test Content</div>;

  const wrapper: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => (
    <MemoryRouter>
      <FeatureService features={[FeatureItem.AllowDBTCloudIntegration]}>
        <EntitlementsLoader>{children}</EntitlementsLoader>
      </FeatureService>
    </MemoryRouter>
  );

  it("should render children correctly", () => {
    const { getByTestId } = render(<TestComponent />, { wrapper });
    expect(getByTestId("test-content")).toBeTruthy();
  });

  it("should call useSetEntitlements hook", () => {
    render(<TestComponent />, { wrapper });
    expect(useSetEntitlements).toHaveBeenCalled();
  });
});
