import dayjs from "dayjs";
import { useIntl } from "react-intl";

import { ExternalLink } from "components/ui/Link";

import { useCurrentOrganizationInfo } from "core/api";
import { links } from "core/utils/links";

interface BillingStatusBanner {
  content: React.ReactNode;
  level: "warning" | "info";
}

export const useBillingStatusBanner = (): BillingStatusBanner | undefined => {
  const { formatMessage } = useIntl();
  const { billing } = useCurrentOrganizationInfo();

  if (!billing) {
    return undefined;
  }

  if (billing.paymentStatus === "manual") {
    return {
      level: "info",
      content: formatMessage(
        { id: "settings.organization.billing.manualPaymentStatus" },
        {
          lnk: (node: React.ReactNode) => (
            <ExternalLink opensInNewTab href={links.contactSales} variant="primary">
              {node}
            </ExternalLink>
          ),
        }
      ),
    };
  }

  if (billing.paymentStatus === "locked") {
    return {
      level: "warning",
      content: formatMessage(
        { id: "settings.organization.billing.lockedPaymentStatus" },
        {
          mail: (
            <ExternalLink href="mailto:billing@airbyte.io" variant="primary">
              billing@airbyte.io
            </ExternalLink>
          ),
        }
      ),
    };
  }

  if (billing.paymentStatus === "disabled") {
    return {
      level: "warning",
      content: formatMessage({ id: "settings.organization.billing.disabledPaymentStatus" }),
    };
  }

  if (billing.paymentStatus === "grace_period") {
    return {
      level: "warning",
      content: formatMessage(
        { id: "settings.organization.billing.gracePeriodPaymentStatus" },
        {
          days: billing?.gracePeriodEndsAt
            ? Math.max(dayjs(billing.gracePeriodEndsAt * 1000).diff(dayjs(), "days"), 0)
            : 0,
        }
      ),
    };
  }

  return undefined;
};
