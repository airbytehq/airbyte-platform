import React from "react";

import { Message } from "components/ui/Message";

import { useBillingStatusBanner } from "cloud/area/billing/utils/useBillingStatusBanner";

export const BillingBanners: React.FC = () => {
  const billingBanner = useBillingStatusBanner("billing_page");

  return <>{billingBanner && <Message type={billingBanner.level} text={billingBanner.content} />}</>;
};
