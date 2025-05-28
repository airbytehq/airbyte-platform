import dayjs from "dayjs";
import React from "react";
import { useIntl } from "react-intl";

import { ExternalLink, Link } from "components/ui/Link";

import { useCurrentWorkspaceLink } from "area/workspace/utils";
import {
  useOrganizationTrialStatus,
  useGetOrganizationPaymentConfig,
  useMaybeWorkspaceCurrentOrganizationId,
} from "core/api";
import { links } from "core/utils/links";
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
  const organizationId = useMaybeWorkspaceCurrentOrganizationId();
  const { data: paymentConfig } = useGetOrganizationPaymentConfig(organizationId);
  const canViewTrialStatus = useGeneratedIntent(Intent.ViewOrganizationTrialStatus, { organizationId });
  const canManageOrganizationBilling = useGeneratedIntent(Intent.ManageOrganizationBilling, { organizationId });
  const trialStatus = useOrganizationTrialStatus(
    organizationId,
    (paymentConfig?.paymentStatus === "uninitialized" || paymentConfig?.paymentStatus === "okay") && canViewTrialStatus
  );

  if (!paymentConfig) {
    return undefined;
  }

  if (paymentConfig.paymentStatus === "manual") {
    if (context === "top_level") {
      // Do not show this information banner as a top-level banner.
      return undefined;
    }
    if (paymentConfig.usageCategoryOverwrite === "free") {
      return {
        level: "info",
        content: formatMessage({ id: "billing.banners.manualPaymentStatusFree" }),
      };
    } else if (paymentConfig.usageCategoryOverwrite === "internal") {
      return {
        level: "info",
        content: formatMessage({ id: "billing.banners.manualPaymentStatusInternal" }),
      };
    }
  }

  if (paymentConfig.paymentStatus === "locked") {
    return {
      level: "warning",
      content: formatMessage(
        { id: "billing.banners.lockedPaymentStatus" },
        {
          lnk: (node: React.ReactNode) => (
            <ExternalLink href={links.supportPortal} opensInNewTab>
              {node}
            </ExternalLink>
          ),
        }
      ),
    };
  }

  if (paymentConfig.paymentStatus === "disabled") {
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

  if (paymentConfig.paymentStatus === "grace_period") {
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
          days: paymentConfig?.gracePeriodEndAt
            ? Math.max(dayjs(paymentConfig.gracePeriodEndAt).diff(dayjs(), "days"), 0)
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
    if (paymentConfig.paymentStatus === "okay") {
      return {
        level: "info",
        content: formatMessage(
          { id: "billing.banners.inTrialWithPaymentMethod" },
          { days: Math.max(dayjs(trialStatus.trialEndsAt).diff(dayjs(), "days"), 0) }
        ),
      };
    }
    if (paymentConfig.paymentStatus === "uninitialized") {
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
    (paymentConfig.paymentStatus === "uninitialized" || paymentConfig.subscriptionStatus !== "subscribed")
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
