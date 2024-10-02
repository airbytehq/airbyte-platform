import classNames from "classnames";
import { FormattedMessage } from "react-intl";

import { ConnectorIcon } from "components/ConnectorIcon";
import { FlexContainer } from "components/ui/Flex";
import { ExternalLink } from "components/ui/Link";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { BaseActorDefinitionVersionInfo } from "core/api/types/AirbyteClient";

import styles from "./BaseConnectorInfo.module.scss";

interface BaseConnectorInfoProps extends BaseActorDefinitionVersionInfo {
  className?: string;
  disableTooltip?: boolean;
  showDocsLink?: boolean;
}

export const BaseConnectorInfo: React.FC<BaseConnectorInfoProps> = ({
  className,
  disableTooltip,
  showDocsLink,
  dockerImageTag,
  icon,
  name,
  documentationUrl,
}) => {
  const nameAndVersion = (
    <FormattedMessage
      id="connectorBuilder.listPage.baseConnector.nameAndVersion"
      values={{
        name,
        version: dockerImageTag,
        a: (node: React.ReactNode) =>
          showDocsLink && documentationUrl ? <ExternalLink href={documentationUrl}>{node}</ExternalLink> : node,
      }}
    />
  );
  return (
    <Tooltip
      control={
        <FlexContainer direction="row" alignItems="center" gap="sm" className={classNames(styles.container, className)}>
          <Text color="grey" size="sm">
            <FormattedMessage id="connectorBuilder.listPage.baseConnector.forkedFrom" />
          </Text>
          <ConnectorIcon icon={icon} className={styles.connectorIcon} />
          <Text color="grey" size="sm" className={styles.nameAndVersion}>
            {nameAndVersion}
          </Text>
        </FlexContainer>
      }
      disabled={disableTooltip}
    >
      <FormattedMessage id="connectorBuilder.listPage.baseConnector.forkedFrom" /> {nameAndVersion}
    </Tooltip>
  );
};
