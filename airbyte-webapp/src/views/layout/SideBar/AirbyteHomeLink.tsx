import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Badge } from "components/ui/Badge";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Link } from "components/ui/Link";

import { FeatureItem, IfFeatureEnabled } from "core/services/features";
import { RoutePaths } from "pages/routePaths";

import styles from "./AirbyteHomeLink.module.scss";
import AirbyteLogo from "./airbyteLogo.svg?react";

export const AirbyteHomeLink: React.FC = () => {
  const { formatMessage } = useIntl();

  return (
    <div className={styles.homeLink}>
      <Link
        to={RoutePaths.Connections}
        aria-label={formatMessage({ id: "sidebar.homepage" })}
        className={styles.homeLink__link}
      >
        <AirbyteLogo height={24} className={styles.homeLink__logo} />
      </Link>
      <IfFeatureEnabled feature={FeatureItem.EnterpriseBranding}>
        <EnterpriseBadge />
      </IfFeatureEnabled>
    </div>
  );
};

const EnterpriseBadge = () => (
  <Badge variant="green">
    <FlexContainer gap="xs" alignItems="center">
      <Icon type="star" size="xs" />
      <FormattedMessage id="enterprise.enterprise" />
    </FlexContainer>
  </Badge>
);
