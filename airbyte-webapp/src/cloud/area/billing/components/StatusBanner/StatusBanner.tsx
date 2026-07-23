import React from "react";

import { AlertBanner } from "components/ui/Banner/AlertBanner";

import { useBillingStatusBanner } from "../../utils/useBillingStatusBanner";

const MainViewStatusBanner: React.FC = () => {
  const statusBanner = useBillingStatusBanner("top_level");
  return statusBanner ? (
    <AlertBanner data-testid="billing-status-banner" message={statusBanner.content} color={statusBanner.level} />
  ) : null;
};

export const StatusBanner: React.FC = () => {
  return (
    <React.Suspense>
      <MainViewStatusBanner />
    </React.Suspense>
  );
};
