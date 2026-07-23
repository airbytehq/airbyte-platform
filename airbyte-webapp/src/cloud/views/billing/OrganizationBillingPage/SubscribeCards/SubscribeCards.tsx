import React from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";
import {
  PlusPlanCard,
  PricingComparisonLink,
  ProPlanCard,
  StandardPlanCard,
} from "cloud/area/billing/components/PlanCards";
import { useOrgInfo } from "core/api";
import { useExperiment } from "core/services/Experiment";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

import styles from "./SubscribeCards.module.scss";

export const SubscribeCards: React.FC = () => {
  const organizationId = useCurrentOrganizationId();
  const canManageOrganizationBilling = useGeneratedIntent(Intent.ManageOrganizationBilling, { organizationId });
  const { billing } = useOrgInfo(organizationId, canManageOrganizationBilling) || {};
  const isSelfServePlusPlanEnabled = useExperiment("billing.selfServePlusPlan");
  const isLockedSubscription = billing?.paymentStatus === "locked";

  return (
    <Box className={styles.subscribe} p="xl">
      <FlexContainer direction="column" gap="xl">
        <Heading as="h2" size="md">
          <FormattedMessage id="settings.organization.billing.subscribeTitle" />
        </Heading>
        <FlexContainer wrap="wrap" className={styles.subscribe__cards}>
          <StandardPlanCard disabled={isLockedSubscription} />
          {isSelfServePlusPlanEnabled && <PlusPlanCard disabled={isLockedSubscription} />}
          <ProPlanCard />
        </FlexContainer>
        <PricingComparisonLink />
      </FlexContainer>
    </Box>
  );
};
