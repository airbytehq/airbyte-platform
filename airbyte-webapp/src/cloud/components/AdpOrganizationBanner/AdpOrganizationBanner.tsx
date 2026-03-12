import React from "react";
import { FormattedMessage } from "react-intl";

import { AlertBanner } from "components/ui/Banner/AlertBanner";
import { ExternalLink } from "components/ui/Link";

import { useCurrentOrganizationId, useIsAdpOrganization } from "area/organization/utils";
import { links } from "core/utils/links";

const AdpOrganizationBannerContent: React.FC = () => {
  const isAdpOrganization = useIsAdpOrganization();
  const organizationId = useCurrentOrganizationId();

  if (!isAdpOrganization) {
    return null;
  }

  const adpUrl = `${links.agentEngineApp}/organizations/${organizationId}`;

  return (
    <AlertBanner
      data-testid="adp-organization-banner"
      color="info"
      message={
        <FormattedMessage
          id="cloud.adpOrganization.banner"
          values={{
            lnk: (node: React.ReactNode) => <ExternalLink href={adpUrl}>{node}</ExternalLink>,
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
