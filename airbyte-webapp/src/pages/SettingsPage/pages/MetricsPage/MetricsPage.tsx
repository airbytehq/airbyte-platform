import React from "react";
import { useIntl } from "react-intl";

import { HeadTitle } from "components/common/HeadTitle";
import { Card } from "components/ui/Card";

import { useTrackPage, PageTrackingCodes } from "core/services/analytics";

import { MetricsForm } from "./components/MetricsForm";

export const MetricsPage: React.FC = () => {
  const { formatMessage } = useIntl();
  useTrackPage(PageTrackingCodes.SETTINGS_METRICS);

  return (
    <>
      <HeadTitle titles={[{ id: "sidebar.settings" }, { id: "settings.metrics" }]} />
      <Card title={formatMessage({ id: "settings.metricsSettings" })} titleWithBottomBorder>
        <MetricsForm />
      </Card>
    </>
  );
};
