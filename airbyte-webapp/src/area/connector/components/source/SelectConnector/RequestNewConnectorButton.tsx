import classNames from "classnames";
import { FormattedMessage } from "react-intl";

import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import styles from "./RequestNewConnectorButton.module.scss";

interface RequestNewConnectorButtonProps {
  className?: string;
  onClick: () => void;
}

export const RequestNewConnectorButton: React.FC<RequestNewConnectorButtonProps> = ({ className, onClick }) => {
  return (
    <button onClick={onClick} className={classNames(styles.button, className)}>
      <Icon type="plus" size="sm" className={styles.icon} />
      <Text size="lg" className={styles.button__text}>
        <FormattedMessage id="connector.requestConnector" />
      </Text>
    </button>
  );
};
