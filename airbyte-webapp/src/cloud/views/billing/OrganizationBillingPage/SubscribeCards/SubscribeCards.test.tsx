import { mocked, render } from "test-utils";
import { mockExperiments } from "test-utils/mockExperiments";

import { useOrgInfo } from "core/api";
import { useGeneratedIntent } from "core/utils/rbac";

import { SubscribeCards } from "./SubscribeCards";

jest.mock("area/organization/utils/useCurrentOrganizationId", () => ({
  useCurrentOrganizationId: jest.fn().mockReturnValue("test-organization-id"),
}));

jest.mock("cloud/area/billing/utils/useRedirectToCustomerPortal", () => ({
  useRedirectToCustomerPortal: jest.fn().mockReturnValue({
    goToCustomerPortal: jest.fn(),
    redirecting: false,
  }),
}));

jest.mock("core/api", () => ({
  useOrgInfo: jest.fn(),
}));

jest.mock("core/utils/rbac", () => ({
  Intent: {
    ManageOrganizationBilling: "ManageOrganizationBilling",
  },
  useGeneratedIntent: jest.fn(),
}));

describe("SubscribeCards", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockExperiments({ "billing.selfServePlusPlan": false });
    mocked(useGeneratedIntent).mockReturnValue(true);
    mocked(useOrgInfo).mockReturnValue({
      billing: {
        paymentStatus: "okay",
      },
    } as ReturnType<typeof useOrgInfo>);
  });

  it("keeps the existing Standard and Pro cards when self-serve Plus is disabled", async () => {
    const wrapper = await render(<SubscribeCards />);

    expect(wrapper.getByText("Standard")).toBeInTheDocument();
    expect(wrapper.queryByText("Plus")).not.toBeInTheDocument();
    expect(wrapper.getByText("Pro")).toBeInTheDocument();
  });

  it("shows Plus in Billing subscribe state when self-serve Plus is enabled", async () => {
    mockExperiments({ "billing.selfServePlusPlan": true });

    const wrapper = await render(<SubscribeCards />);

    expect(wrapper.getByText("Standard")).toBeInTheDocument();
    expect(wrapper.getByText("Plus")).toBeInTheDocument();
    expect(wrapper.getByText("Pro")).toBeInTheDocument();
  });
});
