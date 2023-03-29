import { ConnectorIcon } from "components/common/ConnectorIcon";
import { ReleaseStageBadge } from "components/ReleaseStageBadge";
import { Text } from "components/ui/Text";

import { ConnectorDefinition } from "core/domain/connector";

import styles from "./ConnectorButton.module.scss";

interface ConnectorButtonProps {
  onClick: (definition: ConnectorDefinition) => void;
  definition: ConnectorDefinition;
}

export const ConnectorButton: React.FC<ConnectorButtonProps> = ({ definition, onClick }) => {
  return (
    <button className={styles.button} onClick={() => onClick(definition)}>
      <ConnectorIcon icon={definition.icon} className={styles.icon} />

      <Text size="lg" as="span" className={styles.text}>
        {definition.name}
      </Text>

      <span className={styles.releaseStage}>
        <ReleaseStageBadge stage={definition.releaseStage} />
      </span>
    </button>
  );
};
