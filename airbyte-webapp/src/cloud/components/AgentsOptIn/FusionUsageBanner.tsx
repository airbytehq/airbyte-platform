import { FormattedMessage, useIntl } from "react-intl";

import { AlertBanner } from "components/ui/Banner/AlertBanner";

import { useFusionUsage } from "core/api/cloud";
import { FusionUsageWindow } from "core/api/types/AirbyteClient";

export const FusionUsageBanner: React.FC = () => {
  const { formatDate } = useIntl();
  const { data, isEligible, isError } = useFusionUsage();
  if (!isEligible || isError || !data?.freeCapsApplicable) {
    return null;
  }

  const month = data.month;
  if (!isMonthlyAllowanceExhausted(month)) {
    return null;
  }

  return (
    <AlertBanner
      data-testid="fusion-usage-banner"
      color="warning"
      message={
        <FormattedMessage
          id="fusion.usage.exhausted"
          values={{
            resetsAt: formatDate(month.resetsAt, { month: "long", day: "numeric", timeZone: "UTC" }),
          }}
        />
      }
    />
  );
};

const isMonthlyAllowanceExhausted = (month: FusionUsageWindow): boolean =>
  month.used !== null &&
  month.limit !== null &&
  month.limit > 0 &&
  month.used >= month.limit &&
  Date.parse(month.resetsAt) > Date.now();
