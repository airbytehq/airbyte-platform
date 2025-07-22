import React from "react";
import { useIntl } from "react-intl";

import AirbyteLogo from "components/illustrations/airbyte-logo.svg?react";
import { BrandingBadge } from "components/ui/BrandingBadge";
import { Link } from "components/ui/Link";

import { FeatureItem, IfFeatureEnabled } from "core/services/features";
import { RoutePaths } from "pages/routePaths";

import styles from "./AirbyteHomeLink.module.scss";

export const AirbyteHomeLink: React.FC = () => {
  const { formatMessage } = useIntl();

  return (
    <div className={styles.homeLink}>
      <Link
        to={RoutePaths.Connections}
        aria-label={formatMessage({ id: "sidebar.homepage" })}
        className={styles.homeLink__link}
      >
        <AirbyteLogo height={20} className={styles.homeLink__logo} />
      </Link>
      <IfFeatureEnabled feature={FeatureItem.EnterpriseBranding}>
        <BrandingBadge product="enterprise" testId="enterprise-badge" />
      </IfFeatureEnabled>
      <IfFeatureEnabled feature={FeatureItem.CloudForTeamsBranding}>
        <BrandingBadge product="cloudForTeams" testId="cloud-for-teams-badge" />
      </IfFeatureEnabled>
    </div>
  );
};
