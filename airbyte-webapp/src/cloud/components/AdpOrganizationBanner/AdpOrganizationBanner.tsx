import React from "react";
import { FormattedMessage } from "react-intl";

import { Link } from "components/ui/Link";

import { useCurrentOrganizationId, useIsAdpOrganization } from "area/organization/utils";
import { HideableBanner } from "cloud/components/CloudBannerGroup/components/HideableBanner";
import { RoutePaths } from "pages/routePaths";

const AdpOrganizationBannerContent: React.FC = () => {
  const isAdpOrganization = useIsAdpOrganization();
  const organizationId = useCurrentOrganizationId();

  if (!isAdpOrganization) {
    return null;
  }

  const contextLayerUrl = `/${RoutePaths.Organization}/${organizationId}/${RoutePaths.ContextLayer}`;

  return (
    <HideableBanner
      id="adp-organization"
      data-testid="adp-organization-banner"
      message={
        <FormattedMessage
          id="cloud.adpOrganization.banner"
          values={{
            lnk: (node: React.ReactNode) => <Link to={contextLayerUrl}>{node}</Link>,
          }}
        />
      }
    />
  );
};

export const AdpOrganizationBanner: React.FC = () => (
  <React.Suspense>
    <AdpOrganizationBannerContent />
  </React.Suspense>
);
