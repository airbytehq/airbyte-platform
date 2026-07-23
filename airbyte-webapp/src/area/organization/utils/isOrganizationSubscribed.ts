import { OrganizationInfoReadBilling } from "core/api/types/AirbyteClient";

export const isOrganizationSubscribed = (billing: OrganizationInfoReadBilling | undefined): boolean =>
  billing?.subscriptionStatus === "subscribed" && billing.paymentStatus !== "uninitialized";
