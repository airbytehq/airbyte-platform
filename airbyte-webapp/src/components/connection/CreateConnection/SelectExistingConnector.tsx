import { Card } from "components/ui/Card";

import { DestinationRead, SourceRead } from "core/api/types/AirbyteClient";
import { isSource } from "core/domain/connector/source";

import { ExistingConnectorButton } from "./ExistingConnectorButton";
import styles from "./SelectExistingConnector.module.scss";

interface SelectExistingConnectorProps<T extends SourceRead | DestinationRead> {
  connectors: T[];
  selectConnector: (connectorId: string) => void;
}

export const SelectExistingConnector = <T extends SourceRead | DestinationRead>({
  connectors,
  selectConnector,
}: SelectExistingConnectorProps<T>) => {
  return (
    <Card>
      <ul className={styles.existingConnectors}>
        {connectors.map((connector) => {
          const key = isSource(connector) ? connector.sourceId : connector.destinationId;

          return (
            <li key={key} className={styles.existingConnectors__item}>
              <ExistingConnectorButton connector={connector} onClick={() => selectConnector(key)} />
            </li>
          );
        })}
      </ul>
    </Card>
  );
};
