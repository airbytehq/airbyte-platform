import classNames from "classnames";

import { ConnectorIcon } from "components/common/ConnectorIcon";
import { ReleaseStageBadge } from "components/ReleaseStageBadge";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import styles from "./ConnectorOptionLabel.module.scss";
import { AvailableDestination, AvailableSource } from "./CreditsUsageContext";

interface ConnectorOptionLabelProps {
  connector: AvailableSource | AvailableDestination;
  disabled?: boolean;
}

export const ConnectorOptionLabel: React.FC<ConnectorOptionLabelProps> = ({ connector, disabled }) => {
  return (
    <FlexContainer
      title={connector.name}
      alignItems="center"
      justifyContent="flex-start"
      className={classNames(styles.labelContainer, { [styles.disabled]: disabled })}
    >
      <ConnectorIcon icon={connector.icon} />
      <Text color={disabled ? "grey300" : "darkBlue"} className={styles.connectorName}>
        {connector.name}
      </Text>
      <FlexItem>
        <ReleaseStageBadge stage={connector.releaseStage} />
      </FlexItem>
    </FlexContainer>
  );
};
