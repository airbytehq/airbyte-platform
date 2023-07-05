import { useMemo } from "react";

import { Card } from "components/ui/Card";

import { isSource } from "core/domain/connector/source";
import { DestinationRead, SourceRead } from "core/request/AirbyteClient";

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
  const sortedConnectors = useMemo(() => [...connectors].sort((a, b) => a.name.localeCompare(b.name)), [connectors]);

  return (
    <Card>
      <ul className={styles.existingConnectors}>
        {sortedConnectors.map((connector) => {
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
