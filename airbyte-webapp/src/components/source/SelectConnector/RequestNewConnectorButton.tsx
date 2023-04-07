import { FormattedMessage } from "react-intl";

import { PlusIcon } from "components/icons/PlusIcon";
import { Text } from "components/ui/Text";

import styles from "./RequestNewConnectorButton.module.scss";

interface RequestNewConnectorButtonProps {
  onClick: () => void;
}

export const RequestNewConnectorButton: React.FC<RequestNewConnectorButtonProps> = ({ onClick }) => {
  return (
    <button onClick={onClick} className={styles.button}>
      <PlusIcon />
      <Text size="lg" className={styles.button__text}>
        <FormattedMessage id="connector.requestConnector" />
      </Text>
    </button>
  );
};
