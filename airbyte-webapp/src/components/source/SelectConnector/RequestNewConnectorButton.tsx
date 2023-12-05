import { FormattedMessage } from "react-intl";

import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import styles from "./RequestNewConnectorButton.module.scss";

interface RequestNewConnectorButtonProps {
  onClick: () => void;
}

export const RequestNewConnectorButton: React.FC<RequestNewConnectorButtonProps> = ({ onClick }) => {
  return (
    <button onClick={onClick} className={styles.button}>
      <Icon type="plus" />
      <Text size="lg" className={styles.button__text}>
        <FormattedMessage id="connector.requestConnector" />
      </Text>
    </button>
  );
};
