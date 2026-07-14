import { render, screen } from "@testing-library/react";
import { IntlProvider } from "react-intl";
import { MemoryRouter } from "react-router-dom";

import { useCurrentOrganizationId, useIsAdpOrganization, useIsInstanceAdmin } from "area/organization/utils";
import { useExperiment, useExperimentContext } from "core/services/Experiment";

import { AdpOrganizationAccessGuard } from "./AdpOrganizationAccessGuard";

jest.mock("area/organization/utils", () => ({
  useCurrentOrganizationId: jest.fn(),
  useIsAdpOrganization: jest.fn(),
  useIsInstanceAdmin: jest.fn(),
}));

jest.mock("core/services/Experiment", () => ({
  useExperiment: jest.fn(),
  useExperimentContext: jest.fn(),
}));

const mockUseCurrentOrganizationId = useCurrentOrganizationId as jest.MockedFunction<typeof useCurrentOrganizationId>;
const mockUseIsAdpOrganization = useIsAdpOrganization as jest.MockedFunction<typeof useIsAdpOrganization>;
const mockUseIsInstanceAdmin = useIsInstanceAdmin as jest.MockedFunction<typeof useIsInstanceAdmin>;
const mockUseExperiment = useExperiment as jest.MockedFunction<typeof useExperiment>;
const mockUseExperimentContext = useExperimentContext as jest.MockedFunction<typeof useExperimentContext>;

const messages = {
  "errors.forbidden.message": "Sorry, you don't have permission to access this page.",
  "errors.forbidden.goBack": "Back to workspaces",
};

const renderGuard = (children: React.ReactNode) =>
  render(
    <MemoryRouter>
      <IntlProvider locale="en" messages={messages}>
        <AdpOrganizationAccessGuard>{children}</AdpOrganizationAccessGuard>
      </IntlProvider>
    </MemoryRouter>
  );

beforeEach(() => {
  jest.clearAllMocks();
  mockUseCurrentOrganizationId.mockReturnValue("organization-id");
  mockUseExperiment.mockReturnValue(false);
});

describe("AdpOrganizationAccessGuard", () => {
  it("renders children for non-ADP organization", () => {
    mockUseIsAdpOrganization.mockReturnValue(false);
    mockUseIsInstanceAdmin.mockReturnValue(false);

    renderGuard(<div data-testid="protected-content">protected</div>);

    expect(screen.getByTestId("protected-content")).toBeInTheDocument();
    expect(screen.queryByText(/Sorry, you don't have permission/)).not.toBeInTheDocument();
  });

  it("renders children for ADP organization when user is instance admin", () => {
    mockUseIsAdpOrganization.mockReturnValue(true);
    mockUseIsInstanceAdmin.mockReturnValue(true);

    renderGuard(<div data-testid="protected-content">protected</div>);

    expect(screen.getByTestId("protected-content")).toBeInTheDocument();
    expect(mockUseExperimentContext).toHaveBeenCalledWith("organization", "organization-id");
  });

  it("renders children for ADP organization when data replication access is enabled", () => {
    mockUseIsAdpOrganization.mockReturnValue(true);
    mockUseIsInstanceAdmin.mockReturnValue(false);
    mockUseExperiment.mockReturnValue(true);

    renderGuard(<div data-testid="protected-content">protected</div>);

    expect(screen.getByTestId("protected-content")).toBeInTheDocument();
    expect(mockUseExperiment).toHaveBeenCalledWith("allowAgentsDataReplicationAccess");
  });

  it("renders forbidden state for ADP organization when user is not instance admin", () => {
    mockUseIsAdpOrganization.mockReturnValue(true);
    mockUseIsInstanceAdmin.mockReturnValue(false);

    renderGuard(<div data-testid="protected-content">protected</div>);

    expect(screen.queryByTestId("protected-content")).not.toBeInTheDocument();
    expect(screen.getByText(/Sorry, you don't have permission/)).toBeInTheDocument();
  });
});
