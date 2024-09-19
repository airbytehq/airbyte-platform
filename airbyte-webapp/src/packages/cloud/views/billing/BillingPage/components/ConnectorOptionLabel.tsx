import { ConnectorIcon } from "components/ConnectorIcon";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import styles from "./ConnectorOptionLabel.module.scss";
import { AvailableDestination, AvailableSource } from "./CreditsUsageContext";

interface ConnectorOptionLabelProps {
  connector: AvailableSource | AvailableDestination | { name: string; icon: string };
}

export const ConnectorOptionLabel: React.FC<ConnectorOptionLabelProps> = ({ connector }) => (
  <FlexContainer title={connector.name} alignItems="center" justifyContent="flex-start">
    <ConnectorIcon icon={connector.icon} />
    <Text color="darkBlue" className={styles.connectorName}>
      {connector.name}
    </Text>
  </FlexContainer>
);
