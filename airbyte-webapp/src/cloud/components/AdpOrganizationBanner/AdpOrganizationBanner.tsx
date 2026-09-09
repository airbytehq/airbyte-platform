import React from "react";
import { FormattedMessage } from "react-intl";

import { AlertBanner } from "components/ui/Banner/AlertBanner";
import { Link } from "components/ui/Link";

import { useCurrentOrganizationId, useIsAdpOrganization } from "area/organization/utils";
import { CloudSettingsRoutePaths } from "cloud/views/settings/routePaths";
import { RoutePaths } from "pages/routePaths";

const AdpOrganizationBannerContent: React.FC = () => {
  const isAdpOrganization = useIsAdpOrganization();
  const organizationId = useCurrentOrganizationId();

  if (!isAdpOrganization) {
    return null;
  }

  const contextLayerUrl = `/${RoutePaths.Organization}/${organizationId}/${RoutePaths.Settings}/${CloudSettingsRoutePaths.ContextLayer}`;

  return (
    <AlertBanner
      data-testid="adp-organization-banner"
      color="info"
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
