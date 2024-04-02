import { ConnectorIcon } from "components/common/ConnectorIcon";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { SupportLevelBadge } from "components/ui/SupportLevelBadge";
import { Text } from "components/ui/Text";

import styles from "./ConnectorOptionLabel.module.scss";
import { AvailableDestination, AvailableSource } from "./CreditsUsageContext";

interface ConnectorOptionLabelProps {
  connector: AvailableSource | AvailableDestination;
}

export const ConnectorOptionLabel: React.FC<ConnectorOptionLabelProps> = ({ connector }) => (
  <FlexContainer title={connector.name} alignItems="center" justifyContent="flex-start">
    <ConnectorIcon icon={connector.icon} />
    <Text color="darkBlue" className={styles.connectorName}>
      {connector.name}
    </Text>
    <FlexItem>
      <SupportLevelBadge supportLevel={connector.supportLevel} custom={connector.custom} />
    </FlexItem>
  </FlexContainer>
);
