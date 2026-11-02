import { render, screen } from "@testing-library/react";
import React from "react";
import { IntlProvider } from "react-intl";

import { ORG_PLAN_IDS, useCurrentOrganizationId } from "area/organization/utils";
import { useCurrentOrganizationInfo, useOrganizationTrialStatus } from "core/api";
import { useFeature } from "core/services/features";
import { useIsCloudApp } from "core/utils/app";
import { useGeneratedIntent } from "core/utils/rbac";

import { BrandingBadge, useGetProductBranding } from "./BrandingBadge";

jest.mock("area/organization/utils", () => ({
  ...jest.requireActual("area/organization/utils"),
  useCurrentOrganizationId: jest.fn(),
}));
jest.mock("core/api", () => ({
  useCurrentOrganizationInfo: jest.fn(),
  useOrganizationTrialStatus: jest.fn(),
}));
jest.mock("core/services/features", () => ({
  ...jest.requireActual("core/services/features"),
  useFeature: jest.fn(),
}));
jest.mock("core/utils/app", () => ({
  ...jest.requireActual("core/utils/app"),
  useIsCloudApp: jest.fn(),
}));
jest.mock("core/utils/rbac", () => ({
  ...jest.requireActual("core/utils/rbac"),
  useGeneratedIntent: jest.fn(),
}));

const mockUseCurrentOrganizationId = jest.mocked(useCurrentOrganizationId);
const mockUseCurrentOrganizationInfo = jest.mocked(useCurrentOrganizationInfo);
const mockUseOrganizationTrialStatus = jest.mocked(useOrganizationTrialStatus);
const mockUseFeature = jest.mocked(useFeature);
const mockUseIsCloudApp = jest.mocked(useIsCloudApp);
const mockUseGeneratedIntent = jest.mocked(useGeneratedIntent);

const renderWithIntl = (component: React.ReactElement) => {
  return render(
    <IntlProvider locale="en" messages={{}}>
      {component}
    </IntlProvider>
  );
};

const ProductBrandingBadge: React.FC = () => <BrandingBadge product={useGetProductBranding()} />;

describe("BrandingBadge", () => {
  beforeEach(() => {
    mockUseCurrentOrganizationId.mockReturnValue("organization-id");
    mockUseCurrentOrganizationInfo.mockReturnValue(undefined);
    mockUseOrganizationTrialStatus.mockReturnValue(undefined);
    mockUseFeature.mockReturnValue(false);
    mockUseIsCloudApp.mockReturnValue(true);
    mockUseGeneratedIntent.mockReturnValue(true);
  });

  it("renders enterprise badge", () => {
    renderWithIntl(<BrandingBadge product="enterprise" testId="test-enterprise" />);
    expect(screen.getByTestId("test-enterprise")).toBeInTheDocument();
  });

  it("renders cloudForTeams badge", () => {
    renderWithIntl(<BrandingBadge product="cloudForTeams" testId="test-teams" />);
    expect(screen.getByTestId("test-teams")).toBeInTheDocument();
  });

  it("renders cloudInTrial badge", () => {
    renderWithIntl(<BrandingBadge product="cloudInTrial" testId="test-trial" />);
    expect(screen.getByTestId("test-trial")).toBeInTheDocument();
  });

  it("renders nothing when product is null", () => {
    const { container } = renderWithIntl(<BrandingBadge product={null} />);
    expect(container.firstChild).toBeNull();
  });

  it("does not fetch trial status when organization info has a plan id", () => {
    mockUseCurrentOrganizationInfo.mockReturnValue({
      organizationPlanId: ORG_PLAN_IDS.PRO,
    } as ReturnType<typeof useCurrentOrganizationInfo>);

    renderWithIntl(<ProductBrandingBadge />);

    expect(mockUseOrganizationTrialStatus).toHaveBeenCalledWith("organization-id", { enabled: false });
  });

  it("fetches trial status when the plan id is undefined and the user can view it in Cloud", () => {
    renderWithIntl(<ProductBrandingBadge />);

    expect(mockUseOrganizationTrialStatus).toHaveBeenCalledWith("organization-id", { enabled: true });
  });
});
