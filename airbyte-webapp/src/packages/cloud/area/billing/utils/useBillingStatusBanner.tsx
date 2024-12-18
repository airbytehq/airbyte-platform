import dayjs from "dayjs";
import { useIntl } from "react-intl";

import { ExternalLink, Link } from "components/ui/Link";

import { useCurrentWorkspaceLink } from "area/workspace/utils";
import { useCurrentOrganizationInfo, useOrganizationTrialStatus } from "core/api";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
import { CloudSettingsRoutePaths } from "packages/cloud/views/settings/routePaths";
import { RoutePaths } from "pages/routePaths";

interface BillingStatusBanner {
  content: React.ReactNode;
  level: "warning" | "info";
}

export const useBillingStatusBanner = (context: "top_level" | "billing_page"): BillingStatusBanner | undefined => {
  const { formatMessage } = useIntl();
  const createLink = useCurrentWorkspaceLink();
  const { organizationId, billing } = useCurrentOrganizationInfo();
  const canViewTrialStatus = useGeneratedIntent(Intent.ViewOrganizationTrialStatus);
  const canManageOrganizationBilling = useGeneratedIntent(Intent.ManageOrganizationBilling);
  const trialStatus = useOrganizationTrialStatus(
    organizationId,
    (billing?.paymentStatus === "uninitialized" || billing?.paymentStatus === "okay") && canViewTrialStatus
  );

  if (!billing) {
    return undefined;
  }

  if (billing.paymentStatus === "manual") {
    if (context === "top_level") {
      // Do not show this information banner as a top-level banner.
      return undefined;
    }
    if (billing.accountType === "free") {
      return {
        level: "info",
        content: formatMessage({ id: "billing.banners.manualPaymentStatusFree" }),
      };
    } else if (billing.accountType === "internal") {
      return {
        level: "info",
        content: formatMessage({ id: "billing.banners.manualPaymentStatusInternal" }),
      };
    }
  }

  if (billing.paymentStatus === "locked") {
    return {
      level: "warning",
      content: formatMessage(
        { id: "billing.banners.lockedPaymentStatus" },
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
      content: formatMessage(
        {
          id:
            context === "top_level" && canManageOrganizationBilling
              ? "billing.banners.disabledPaymentStatusWithLink"
              : "billing.banners.disabledPaymentStatus",
        },
        {
          lnk: (node: React.ReactNode) => (
            <Link to={createLink(`/${RoutePaths.Settings}/${CloudSettingsRoutePaths.Billing}`)}>{node}</Link>
          ),
        }
      ),
    };
  }

  if (billing.paymentStatus === "grace_period") {
    return {
      level: "warning",
      content: formatMessage(
        {
          id:
            context === "top_level" && canManageOrganizationBilling
              ? "billing.banners.gracePeriodPaymentStatusWithLink"
              : "billing.banners.gracePeriodPaymentStatus",
        },
        {
          days: billing?.gracePeriodEndsAt
            ? Math.max(dayjs(billing.gracePeriodEndsAt * 1000).diff(dayjs(), "days"), 0)
            : 0,
          lnk: (node: React.ReactNode) => (
            <Link to={createLink(`/${RoutePaths.Settings}/${CloudSettingsRoutePaths.Billing}`)}>{node}</Link>
          ),
        }
      ),
    };
  }

  if (trialStatus?.trialStatus === "pre_trial") {
    return {
      level: "info",
      content: formatMessage({ id: "billing.banners.preTrial" }),
    };
  }

  if (trialStatus?.trialStatus === "in_trial") {
    if (billing.paymentStatus === "okay") {
      return {
        level: "info",
        content: formatMessage(
          { id: "billing.banners.inTrialWithPaymentMethod" },
          { days: Math.max(dayjs(trialStatus.trialEndsAt).diff(dayjs(), "days"), 0) }
        ),
      };
    }
    if (billing.paymentStatus === "uninitialized") {
      return {
        level: "info",
        content: formatMessage(
          {
            id:
              context === "top_level" && canManageOrganizationBilling
                ? "billing.banners.inTrialWithLink"
                : "billing.banners.inTrial",
          },
          {
            days: Math.max(dayjs(trialStatus.trialEndsAt).diff(dayjs(), "days"), 0),
            lnk: (node: React.ReactNode) => (
              <Link to={createLink(`/${RoutePaths.Settings}/${CloudSettingsRoutePaths.Billing}`)}>{node}</Link>
            ),
          }
        ),
      };
    }
  }

  if (
    trialStatus?.trialStatus === "post_trial" &&
    (billing.paymentStatus === "uninitialized" || billing.subscriptionStatus !== "subscribed")
  ) {
    return {
      level: "info",
      content: formatMessage(
        {
          id:
            context === "top_level" && canManageOrganizationBilling
              ? "billing.banners.postTrialWithLink"
              : "billing.banners.postTrial",
        },
        {
          lnk: (node: React.ReactNode) => (
            <Link to={createLink(`/${RoutePaths.Settings}/${CloudSettingsRoutePaths.Billing}`)}>{node}</Link>
          ),
        }
      ),
    };
  }

  return undefined;
};
