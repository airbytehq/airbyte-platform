import React from "react";
import { useIntl } from "react-intl";

import { Link } from "components/ui/Link";
import { Message } from "components/ui/Message";

import { useExperiment } from "hooks/services/Experiment";
import { useBillingStatusBanner } from "packages/cloud/area/billing/utils/useBillingStatusBanner";

export const BillingBanners: React.FC = () => {
  const { formatMessage } = useIntl();
  const billingBanner = useBillingStatusBanner("billing_page");
  const isAutoRechargeEnabled = useExperiment("billing.autoRecharge");

  return (
    <>
      {billingBanner && <Message type={billingBanner.level} text={billingBanner.content} />}
      {isAutoRechargeEnabled && (
        <Message
          data-testid="autoRechargeEnabledBanner"
          text={formatMessage(
            {
              id: "credits.autoRechargeEnabled",
            },
            {
              contact: (node: React.ReactNode) => (
                <Link opensInNewTab to="mailto:billing@airbyte.io" variant="primary">
                  {node}
                </Link>
              ),
            }
          )}
        />
      )}
    </>
  );
};
