import { render, screen } from "@testing-library/react";
import { IntlProvider } from "react-intl";
import { MemoryRouter } from "react-router-dom";

import { useIsAdpOrganization, useIsInstanceAdmin } from "area/organization/utils";

import { AdpOrganizationAccessGuard } from "./AdpOrganizationAccessGuard";

jest.mock("area/organization/utils", () => ({
  useIsAdpOrganization: jest.fn(),
  useIsInstanceAdmin: jest.fn(),
}));

const mockUseIsAdpOrganization = useIsAdpOrganization as jest.MockedFunction<typeof useIsAdpOrganization>;
const mockUseIsInstanceAdmin = useIsInstanceAdmin as jest.MockedFunction<typeof useIsInstanceAdmin>;

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
  });

  it("renders forbidden state for ADP organization when user is not instance admin", () => {
    mockUseIsAdpOrganization.mockReturnValue(true);
    mockUseIsInstanceAdmin.mockReturnValue(false);

    renderGuard(<div data-testid="protected-content">protected</div>);

    expect(screen.queryByTestId("protected-content")).not.toBeInTheDocument();
    expect(screen.getByText(/Sorry, you don't have permission/)).toBeInTheDocument();
  });
});
