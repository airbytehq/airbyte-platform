import { OrganizationInfoReadBilling } from "core/api/types/AirbyteClient";

import { hasOrganizationBillingHistory, isOrganizationSubscribed } from "./isOrganizationSubscribed";

const billing = (overrides: Partial<OrganizationInfoReadBilling>): OrganizationInfoReadBilling => ({
  paymentStatus: "okay",
  ...overrides,
});

describe("isOrganizationSubscribed", () => {
  it("is false for undefined billing", () => {
    expect(isOrganizationSubscribed(undefined)).toBe(false);
  });

  it("is true only when subscribed with an initialized payment status", () => {
    expect(isOrganizationSubscribed(billing({ subscriptionStatus: "subscribed" }))).toBe(true);
    expect(
      isOrganizationSubscribed(billing({ subscriptionStatus: "subscribed", paymentStatus: "uninitialized" }))
    ).toBe(false);
    expect(isOrganizationSubscribed(billing({ subscriptionStatus: "unsubscribed" }))).toBe(false);
    expect(isOrganizationSubscribed(billing({ subscriptionStatus: "pre_subscription" }))).toBe(false);
  });
});

describe("hasOrganizationBillingHistory", () => {
  it("is false for undefined billing", () => {
    expect(hasOrganizationBillingHistory(undefined)).toBe(false);
  });

  it("is false for organizations that never subscribed or are still in trial", () => {
    expect(hasOrganizationBillingHistory(billing({ subscriptionStatus: "pre_subscription" }))).toBe(false);
    expect(
      hasOrganizationBillingHistory(billing({ subscriptionStatus: "subscribed", paymentStatus: "uninitialized" }))
    ).toBe(false);
  });

  it("is true for currently subscribed organizations and unsubscribed organizations", () => {
    expect(hasOrganizationBillingHistory(billing({ subscriptionStatus: "subscribed" }))).toBe(true);
    expect(
      hasOrganizationBillingHistory(billing({ subscriptionStatus: "subscribed", paymentStatus: "grace_period" }))
    ).toBe(true);
    expect(hasOrganizationBillingHistory(billing({ subscriptionStatus: "unsubscribed" }))).toBe(true);
    expect(
      hasOrganizationBillingHistory(billing({ subscriptionStatus: "unsubscribed", paymentStatus: "disabled" }))
    ).toBe(true);
  });

  it("is false for unsubscribed organizations that never initialized payment, e.g. cancelled trials", () => {
    expect(
      hasOrganizationBillingHistory(billing({ subscriptionStatus: "unsubscribed", paymentStatus: "uninitialized" }))
    ).toBe(false);
  });
});
