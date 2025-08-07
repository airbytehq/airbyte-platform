import React from "react";
import { FormattedMessage } from "react-intl";

import { Badge } from "components/ui/Badge";
import { FlexContainer } from "components/ui/Flex";

import { useCurrentOrganizationId } from "area/organization/utils";
import { useOrganizationTrialStatus } from "core/api";
import { FeatureItem, useFeature } from "core/services/features";
import { useIsCloudApp } from "core/utils/app";

export interface BrandingBadgeProps {
  product: "enterprise" | "cloudForTeams" | "cloudInTrial" | null;
  testId?: string;
}

export const BrandingBadge: React.FC<BrandingBadgeProps> = ({ product, testId }) =>
  product === null ? null : (
    <Badge variant={product === "enterprise" ? "darkBlue" : "blue"} data-testid={testId}>
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
export const useGetProductBranding = (): "enterprise" | "cloudForTeams" | "cloudInTrial" | null => {
  const currentOrganizationId = useCurrentOrganizationId();
  const isEnterprise = useFeature(FeatureItem.EnterpriseBranding);
  const isCloudApp = useIsCloudApp();
  const isCloudForTeams = useFeature(FeatureItem.CloudForTeamsBranding);
  const isCloudInTrial =
    useOrganizationTrialStatus(currentOrganizationId, {
      enabled: !!currentOrganizationId && isCloudApp,
    })?.trialStatus === "in_trial";
  return isEnterprise ? "enterprise" : isCloudForTeams ? "cloudForTeams" : isCloudInTrial ? "cloudInTrial" : null;
};
