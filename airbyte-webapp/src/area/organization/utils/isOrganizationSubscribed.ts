import { OrganizationInfoReadBilling } from "core/api/types/AirbyteClient";

export const isOrganizationSubscribed = (billing: OrganizationInfoReadBilling | undefined): boolean =>
  billing?.subscriptionStatus === "subscribed" && billing.paymentStatus !== "uninitialized";

/**
 * True when the organization has ever been a paying customer: it is currently subscribed with a
 * payment method, or it previously subscribed and has since unsubscribed. Trial organizations
 * (subscribed, but payment never initialized) and organizations that never subscribed are excluded,
 * since they have no billing history.
 */
export const hasOrganizationBillingHistory = (billing: OrganizationInfoReadBilling | undefined): boolean =>
  !!billing && billing.subscriptionStatus !== "pre_subscription" && billing.paymentStatus !== "uninitialized";
