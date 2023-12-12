import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Badge } from "components/ui/Badge";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Link } from "components/ui/Link";

import { FeatureItem, useFeature } from "core/services/features";
import { RoutePaths } from "pages/routePaths";

import styles from "./AirbyteHomeLink.module.scss";
import AirbyteLogo from "./airbyteLogo.svg?react";

export const AirbyteHomeLink: React.FC = () => {
  const { formatMessage } = useIntl();
  const showEnterpriseBranding = useFeature(FeatureItem.EnterpriseBranding);

  return (
    <Link
      to={RoutePaths.Connections}
      aria-label={formatMessage({ id: "sidebar.homepage" })}
      className={styles.homeLink}
    >
      <FlexContainer direction="column" alignItems="center">
        <AirbyteLogo height={33} width={33} className={styles.homeLink__logo} />
        {showEnterpriseBranding && <EnterpriseBadge />}
      </FlexContainer>
    </Link>
  );
};

const EnterpriseBadge = () => (
  <Badge variant="green">
    <FlexContainer gap="xs" alignItems="center">
      <Icon type="star" size="sm" />
      <FormattedMessage id="enterprise.enterprise" />
    </FlexContainer>
  </Badge>
);
