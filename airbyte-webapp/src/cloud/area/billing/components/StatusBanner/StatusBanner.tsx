import React from "react";

import { AlertBanner } from "components/ui/Banner/AlertBanner";

import { HideableBanner } from "cloud/components/CloudBannerGroup/components/HideableBanner";

import { useBillingStatusBanner } from "../../utils/useBillingStatusBanner";

const MainViewStatusBanner: React.FC = () => {
  const statusBanner = useBillingStatusBanner("top_level");

  if (!statusBanner) {
    return null;
  }

  if (statusBanner.level === "info") {
    return <HideableBanner id="billing-status" data-testid="billing-status-banner" message={statusBanner.content} />;
  }

  return <AlertBanner data-testid="billing-status-banner" message={statusBanner.content} color={statusBanner.level} />;
};

export const StatusBanner: React.FC = () => {
  return (
    <React.Suspense>
      <MainViewStatusBanner />
    </React.Suspense>
  );
};
