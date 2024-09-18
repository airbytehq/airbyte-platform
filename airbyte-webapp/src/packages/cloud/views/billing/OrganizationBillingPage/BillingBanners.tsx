import React from "react";
import { useIntl } from "react-intl";

import { Link } from "components/ui/Link";
import { Message } from "components/ui/Message";

import { useCurrentOrganizationInfo } from "core/api";
import { links } from "core/utils/links";
import { useExperiment } from "hooks/services/Experiment";

export const BillingBanners: React.FC = () => {
  const { formatMessage } = useIntl();
  const { billing } = useCurrentOrganizationInfo();
  const isAutoRechargeEnabled = useExperiment("billing.autoRecharge", false);

  return (
    <>
      {billing?.paymentStatus === "manual" && (
        <Message
          type="info"
          text={formatMessage(
            { id: "settings.organization.billing.manualPaymentStatus" },
            {
              lnk: (node: React.ReactNode) => (
                <Link opensInNewTab to={links.contactSales} variant="primary">
                  {node}
                </Link>
              ),
            }
          )}
        />
      )}
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
