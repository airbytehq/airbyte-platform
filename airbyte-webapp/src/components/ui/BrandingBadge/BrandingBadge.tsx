import React from "react";
import { FormattedMessage } from "react-intl";

import { Badge } from "components/ui/Badge";
import { FlexContainer } from "components/ui/Flex";

import { useCurrentOrganizationId } from "area/organization/utils";
import { useOrganizationTrialStatus, useCurrentOrganizationInfo } from "core/api";
import { FeatureItem, useFeature } from "core/services/features";
import { useIsCloudApp } from "core/utils/app";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

type ProductBranding = "enterprise" | "cloudForTeams" | "cloudInTrial" | null;

export const ORG_PLAN_IDS = {
  CORE: "plan-airbyte-core",
  STANDARD: "plan-airbyte-standard",
  SME: "plan-airbyte-sme",
  FLEX: "plan-airbyte-flex",
  STANDARD_TRIAL: "plan-airbyte-standard-trial",
  UNIFIED_TRIAL: "plan-airbyte-unified-trial",
  PRO: "plan-airbyte-pro",
  EMBEDDED_PAYG: "plan-airbyte-embedded-payg",
  EMBEDDED_ANNUAL_COMMITMENT: "plan-airbyte-embedded-annual-commitment",
} as const;

type PlanId = (typeof ORG_PLAN_IDS)[keyof typeof ORG_PLAN_IDS];

const planIdToBrandingMap: Record<PlanId, ProductBranding> = {
  [ORG_PLAN_IDS.CORE]: null,
  [ORG_PLAN_IDS.STANDARD]: null,
  [ORG_PLAN_IDS.SME]: "enterprise",
  [ORG_PLAN_IDS.FLEX]: "enterprise",
  [ORG_PLAN_IDS.STANDARD_TRIAL]: "cloudInTrial",
  [ORG_PLAN_IDS.UNIFIED_TRIAL]: "cloudInTrial",
  [ORG_PLAN_IDS.PRO]: "cloudForTeams",
  [ORG_PLAN_IDS.EMBEDDED_PAYG]: null,
  [ORG_PLAN_IDS.EMBEDDED_ANNUAL_COMMITMENT]: null,
};

export interface BrandingBadgeProps {
  product: ProductBranding;
  testId?: string;
}

/**
 * Another name for this component is "Pill Plan".
 * It is used to display the current plan of the organization on the side bar.
 * and as advertisement for the Pro features.
 */
export const BrandingBadge: React.FC<BrandingBadgeProps> = ({ product, testId }) =>
  product === null ? null : (
    <Badge variant="blue" data-testid={testId}>
      <FlexContainer alignItems="center">
        <FormattedMessage
          id={
            {
              enterprise: "enterprise.enterprise",
              cloudForTeams: "cloud.cloudForTeams",
              cloudInTrial: "cloud.inTrial",
            }[product]
          }
        />
      </FlexContainer>
    </Badge>
  );

/**
 * Determines the product branding type for the current organization.
 * Returns the appropriate branding type based on feature flags and trial status.
 * Used to display the correct badge (Enterprise, Teams, or Trial) in the UI.
 */
export const useGetProductBranding = (): ProductBranding => {
  // try to get the Stigg org plan id, otherwise, the default calculation will be used
  const organizationInfo = useCurrentOrganizationInfo();
  const planId = organizationInfo?.organizationPlanId;

  // default product type calculation
  // TODO: can be removed once we complete the migration to Stigg
  const currentOrganizationId = useCurrentOrganizationId();
  const isEnterprise = useFeature(FeatureItem.EnterpriseBranding);
  const isCloudApp = useIsCloudApp();
  const isCloudForTeams = useFeature(FeatureItem.CloudForTeamsBranding);
  const canViewTrialStatus = useGeneratedIntent(Intent.ViewOrganizationTrialStatus);
  const isCloudInTrial =
    useOrganizationTrialStatus(currentOrganizationId, {
      enabled: !!currentOrganizationId && isCloudApp && canViewTrialStatus,
    })?.trialStatus === "in_trial";

  // If the planId is provided, it means we still need to use Stigg — even if the plan is out of scope.
  if (planId !== undefined) {
    return planIdToBrandingMap[planId as PlanId] || null;
  }

  // fallback to feature flag logic
  return isEnterprise ? "enterprise" : isCloudForTeams ? "cloudForTeams" : isCloudInTrial ? "cloudInTrial" : null;
};
