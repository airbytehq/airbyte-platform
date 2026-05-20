import { mocked, render } from "test-utils";
import { mockExperiments } from "test-utils/mockExperiments";

import { useGetOrganizationSubscriptionInfo } from "core/api";

import { Subscription } from "./Subscription";

jest.mock("area/organization/utils/useCurrentOrganizationId", () => ({
  useCurrentOrganizationId: jest.fn().mockReturnValue("test-organization-id"),
}));

jest.mock("cloud/area/billing/utils/useLinkToPlanPage", () => ({
  useLinkToPlanPage: jest.fn().mockReturnValue("/organizations/test-organization-id/settings/plan"),
}));

jest.mock("core/api", () => ({
  useGetOrganizationSubscriptionInfo: jest.fn(),
  useCancelSubscription: jest.fn().mockReturnValue({ isLoading: false, mutateAsync: jest.fn() }),
  useUnscheduleCancelSubscription: jest.fn().mockReturnValue({ isLoading: false, mutateAsync: jest.fn() }),
}));

jest.mock("./CancelSubscription", () => ({
  CancelSubscription: ({ disabled }: { disabled?: boolean }) => (
    <button disabled={disabled} type="button">
      Cancel
    </button>
  ),
}));

beforeEach(() => {
  jest.clearAllMocks();
  mockExperiments({ "billing.selfServePlusPlan": true });
});

describe("Subscription", () => {
  it("renders the no-active-plan empty state and a Change plan link when there is no subscription", async () => {
    mocked(useGetOrganizationSubscriptionInfo).mockReturnValue({
      data: undefined,
      isLoading: false,
      isError: false,
    } as unknown as ReturnType<typeof useGetOrganizationSubscriptionInfo>);

    const wrapper = await render(<Subscription />);

    expect(wrapper.container.textContent).toContain("No active plan");
    expect(wrapper.queryByRole("button", { name: /^Cancel$/i })).not.toBeInTheDocument();
  });

  it("links Change plan to the plan page", async () => {
    mocked(useGetOrganizationSubscriptionInfo).mockReturnValue({
      data: undefined,
      isLoading: false,
      isError: false,
    } as unknown as ReturnType<typeof useGetOrganizationSubscriptionInfo>);

    const wrapper = await render(<Subscription />);
    const changePlan = wrapper.getByRole("link", { name: /Change plan/i });
    expect(changePlan).toHaveAttribute("href", "/organizations/test-organization-id/settings/plan");
  });

  it("renders the current plan name and the cancel control when subscribed to a self-serve plan", async () => {
    mocked(useGetOrganizationSubscriptionInfo).mockReturnValue({
      data: {
        name: "Standard",
        entitlementPlan: { planName: "Airbyte Standard" },
        selfServeSubscription: true,
      },
      isLoading: false,
      isError: false,
    } as unknown as ReturnType<typeof useGetOrganizationSubscriptionInfo>);

    const wrapper = await render(<Subscription />);

    expect(wrapper.container.textContent).toContain("Airbyte Standard");
    expect(wrapper.getByRole("link", { name: /Change plan/i })).toBeInTheDocument();
    expect(wrapper.getByRole("button", { name: /^Cancel$/i })).toBeInTheDocument();
  });

  it("hides the Change plan link when the selfServePlusPlan experiment is off", async () => {
    mockExperiments({ "billing.selfServePlusPlan": false });
    mocked(useGetOrganizationSubscriptionInfo).mockReturnValue({
      data: {
        name: "Standard",
        entitlementPlan: { planName: "Airbyte Standard" },
        selfServeSubscription: true,
      },
      isLoading: false,
      isError: false,
    } as unknown as ReturnType<typeof useGetOrganizationSubscriptionInfo>);

    const wrapper = await render(<Subscription />);

    expect(wrapper.queryByRole("link", { name: /Change plan/i })).not.toBeInTheDocument();
    expect(wrapper.getByRole("button", { name: /^Cancel$/i })).toBeInTheDocument();
  });
});
