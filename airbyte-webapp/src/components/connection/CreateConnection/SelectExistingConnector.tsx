import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";

import { DestinationRead, SourceRead } from "core/api/types/AirbyteClient";
import { isSource } from "core/domain/connector/source";

import { ExistingConnectorButton } from "./ExistingConnectorButton";
import styles from "./SelectExistingConnector.module.scss";

interface SelectExistingConnectorProps<T extends SourceRead | DestinationRead> {
  connectors: T[];
  selectConnector: (connectorId: string) => void;
  hasNextPage: boolean;
  isFetchingNextPage: boolean;
  fetchNextPage: () => void;
}

export const SelectExistingConnector = <T extends SourceRead | DestinationRead>({
  connectors,
  selectConnector,
  hasNextPage,
  fetchNextPage,
  isFetchingNextPage,
}: SelectExistingConnectorProps<T>) => {
  return (
    <Card noPadding>
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
      {hasNextPage && (
        <Box pt="sm" pb="lg">
          <FlexContainer justifyContent="center">
            <Button
              variant="clear"
              onClick={fetchNextPage}
              isLoading={isFetchingNextPage}
              disabled={isFetchingNextPage}
              data-testid="load-more-existing-connectors"
            >
              <FormattedMessage id="connection.loadMoreJobs" />
            </Button>
          </FlexContainer>
        </Box>
      )}
    </Card>
  );
};
