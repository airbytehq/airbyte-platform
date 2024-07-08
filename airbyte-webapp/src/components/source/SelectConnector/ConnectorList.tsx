import isString from "lodash/isString";
import { useMemo } from "react";

import { FlexContainer } from "components/ui/Flex";

import { ConnectorDefinition } from "core/domain/connector";
import { isSourceDefinition } from "core/domain/connector/source";

import { BuilderConnectorButton, ConnectorButton } from "./ConnectorButton";
import styles from "./ConnectorList.module.scss";
import { RequestNewConnectorButton } from "./RequestNewConnectorButton";
import { ConnectorSorting } from "./SelectConnector";
import { SuggestedConnectors } from "./SuggestedConnectors";

interface ConnectorListProps<T extends ConnectorDefinition> {
  sorting: ConnectorSorting;
  displayType?: "grid" | "list";
  connectorDefinitions: T[];
  noSearchResultsContent: React.ReactNode;
  suggestedConnectorDefinitionIds?: string[];
  onConnectorButtonClick: (definition: ConnectorDefinition) => void;
  onOpenRequestConnectorModal: () => void;
  showConnectorBuilderButton?: boolean;
}

export const ConnectorList = <T extends ConnectorDefinition>({
  sorting,
  displayType,
  connectorDefinitions,
  noSearchResultsContent,
  suggestedConnectorDefinitionIds,
  onConnectorButtonClick,
  onOpenRequestConnectorModal,
  showConnectorBuilderButton = false,
}: ConnectorListProps<T>) => {
  const sortedConnectorDefinitions = useMemo(
    () =>
      connectorDefinitions.sort((a, b) => {
        switch (sorting.column) {
          case "name":
            const localeCompare = a.name.localeCompare(b.name);
            return sorting.isAscending ? localeCompare : -localeCompare;
          default:
            return sortNumericMetric(
              getNumericMetric(a, sorting.column),
              getNumericMetric(b, sorting.column),
              sorting.isAscending
            );
        }
      }),
    [connectorDefinitions, sorting.column, sorting.isAscending]
  );

  return (
    <FlexContainer direction="column" gap="xl">
      {suggestedConnectorDefinitionIds && suggestedConnectorDefinitionIds.length > 0 && (
        <div className={styles.connectorGrid__suggestedConnectors}>
          <SuggestedConnectors
            definitionIds={suggestedConnectorDefinitionIds}
            onConnectorButtonClick={onConnectorButtonClick}
          />
        </div>
      )}

      {connectorDefinitions.length === 0 && noSearchResultsContent}

      {displayType === "grid" ? (
        <div className={styles.connectorGrid}>
          {sortedConnectorDefinitions.map((definition) => {
            const key = isSourceDefinition(definition)
              ? definition.sourceDefinitionId
              : definition.destinationDefinitionId;
            return <ConnectorButton definition={definition} onClick={onConnectorButtonClick} key={key} maxLines={3} />;
          })}

          {showConnectorBuilderButton && <BuilderConnectorButton layout="vertical" />}
          <RequestNewConnectorButton onClick={onOpenRequestConnectorModal} />
        </div>
      ) : (
        <FlexContainer className={styles.connectorList} direction="column" gap="sm">
          {sortedConnectorDefinitions.map((definition) => {
            const key = isSourceDefinition(definition)
              ? definition.sourceDefinitionId
              : definition.destinationDefinitionId;
            return (
              <ConnectorButton
                className={styles.connectorListButton}
                definition={definition}
                onClick={onConnectorButtonClick}
                key={key}
                showMetrics
                maxLines={2}
              />
            );
          })}

          {showConnectorBuilderButton && (
            <BuilderConnectorButton className={styles.connectorListButton} layout="horizontal" />
          )}
          <RequestNewConnectorButton className={styles.connectorListButton} onClick={onOpenRequestConnectorModal} />
        </FlexContainer>
      )}
    </FlexContainer>
  );
};

type NumericMetric = 1 | 2 | 3 | undefined;

const getNumericMetric = (connectorDefinition: ConnectorDefinition, metric: "successRate" | "usage"): NumericMetric => {
  const rawMetricValue =
    metric === "successRate"
      ? connectorDefinition.metrics?.all?.sync_success_rate
      : connectorDefinition.metrics?.all?.usage;
  if (!isString(rawMetricValue)) {
    return undefined;
  }

  const lowercaseMetricValue = rawMetricValue.toLowerCase();
  if (lowercaseMetricValue !== "low" && lowercaseMetricValue !== "medium" && lowercaseMetricValue !== "high") {
    return undefined;
  }

  switch (lowercaseMetricValue) {
    case "low":
      return 1;
    case "medium":
      return 2;
    case "high":
      return 3;
  }
};

const sortNumericMetric = (a: NumericMetric, b: NumericMetric, isAscending: boolean) => {
  if (a && b) {
    if (isAscending) {
      return a - b;
    }
    return b - a;
  }
  if (a && !b) {
    return -1;
  }
  if (!a && b) {
    return 1;
  }
  return 0;
};
