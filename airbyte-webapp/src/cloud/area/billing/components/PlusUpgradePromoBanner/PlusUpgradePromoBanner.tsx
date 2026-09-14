import dayjs from "dayjs";
import React from "react";
import { FormattedMessage } from "react-intl";

import { AlertBanner } from "components/ui/Banner/AlertBanner";
import { Link } from "components/ui/Link";

import { useOrganizationPlan } from "area/organization/utils";
import { useExperiment } from "core/services/Experiment";

import styles from "./PlusUpgradePromoBanner.module.scss";
import { useLinkToPlanPage } from "../../utils/useLinkToPlanPage";

/**
 * Midnight Pacific time on September 29, 2026 (PDT, UTC-7). The banner is hidden from this instant onward.
 */
export const PLUS_UPGRADE_PROMO_BANNER_END = "2026-09-29T00:00:00-07:00";

const PlusUpgradePromoBannerContent: React.FC = () => {
  const isPlanPageRedesignEnabled = useExperiment("plan-page-redesign-ui");
  const { isStandardPlan, isStandardTrialPlan } = useOrganizationPlan();
  const linkToPlanPage = useLinkToPlanPage();
  const isStandardOrTrial = isStandardPlan || isStandardTrialPlan;

  if (!isPlanPageRedesignEnabled || !isStandardOrTrial || !dayjs().isBefore(PLUS_UPGRADE_PROMO_BANNER_END)) {
    return null;
  }

  return (
    <AlertBanner
      data-testid="plus-upgrade-promo-banner"
      color="info"
      message={
        <FormattedMessage
          id="billing.banners.plusUpgradePromo"
          values={{
            lnk: (node: React.ReactNode) => (
              <Link to={linkToPlanPage} className={styles.link}>
                {node}
              </Link>
            ),
          }}
        />
      }
    />
  );
};

export const PlusUpgradePromoBanner: React.FC = () => (
  <React.Suspense>
    <PlusUpgradePromoBannerContent />
  </React.Suspense>
);
