import dayjs from "dayjs";
import React from "react";
import { useIntl } from "react-intl";

import { ExternalLink, Link } from "components/ui/Link";

import { links } from "core/utils/links";
import { useOrganizationSubscriptionStatus } from "core/utils/useOrganizationSubscriptionStatus";

import { useLinkToBillingPage } from "./useLinkToBillingPage";

interface BillingStatusBanner {
  content: React.ReactNode;
  level: "warning" | "info";
}

export const useBillingStatusBanner = (context: "top_level" | "billing_page"): BillingStatusBanner | undefined => {
  const { formatMessage } = useIntl();
  const {
    trialStatus,
    trialDaysLeft,
    canManageOrganizationBilling,
    paymentStatus,
    subscriptionStatus,
    accountType,
    gracePeriodEndsAt,
  } = useOrganizationSubscriptionStatus();
  const linkToBilling = useLinkToBillingPage();

  if (!paymentStatus || !subscriptionStatus) {
    return undefined;
  }

  if (paymentStatus === "manual") {
    if (context === "top_level") {
      // Do not show this information banner as a top-level banner.
      return undefined;
    }
    if (accountType === "free") {
      return {
        level: "info",
        content: formatMessage({ id: "billing.banners.manualPaymentStatusFree" }),
      };
    } else if (accountType === "internal") {
      return {
        level: "info",
        content: formatMessage({ id: "billing.banners.manualPaymentStatusInternal" }),
      };
    }
  }

  if (paymentStatus === "locked") {
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

  if (paymentStatus === "disabled") {
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
          lnk: (node: React.ReactNode) => <Link to={linkToBilling}>{node}</Link>,
        }
      ),
    };
  }

  if (paymentStatus === "grace_period") {
    const gracePeriodDaysLeft = gracePeriodEndsAt ? Math.max(dayjs(gracePeriodEndsAt).diff(dayjs(), "days"), 0) : 0;
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
          days: gracePeriodDaysLeft,
          lnk: (node: React.ReactNode) => <Link to={linkToBilling}>{node}</Link>,
        }
      ),
    };
  }

  if (trialStatus === "pre_trial") {
    return {
      level: "info",
      content: formatMessage({ id: "billing.banners.preTrial" }),
    };
  }

  if (trialStatus === "in_trial") {
    if (paymentStatus === "okay") {
      return {
        level: "info",
        content: formatMessage({ id: "billing.banners.inTrialWithPaymentMethod" }, { days: trialDaysLeft }),
      };
    }
    if (paymentStatus === "uninitialized") {
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
            days: trialDaysLeft,
            lnk: (node: React.ReactNode) => <Link to={linkToBilling}>{node}</Link>,
          }
        ),
      };
    }
  }

  if (trialStatus === "post_trial" && (paymentStatus === "uninitialized" || subscriptionStatus !== "subscribed")) {
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
          lnk: (node: React.ReactNode) => <Link to={linkToBilling}>{node}</Link>,
        }
      ),
    };
  }

  return undefined;
};
