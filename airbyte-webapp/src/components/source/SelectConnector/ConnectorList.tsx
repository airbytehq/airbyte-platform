import isString from "lodash/isString";
import { useMemo } from "react";

import { convertToConnectorDefinitionWithMetrics } from "components/connector/ConnectorQualityMetrics";
import { FlexContainer } from "components/ui/Flex";

import { ConnectorDefinitionOrEnterpriseStub } from "core/domain/connector";
import { isSourceDefinition } from "core/domain/connector/source";

import { BuilderConnectorButton, ConnectorButton } from "./ConnectorButton";
import styles from "./ConnectorList.module.scss";
import { RequestNewConnectorButton } from "./RequestNewConnectorButton";
import { ConnectorSorting } from "./SelectConnector";
import { SuggestedConnectors } from "./SuggestedConnectors";

interface ConnectorListProps {
  sorting: ConnectorSorting;
  displayType?: "grid" | "list";
  connectorDefinitions: ConnectorDefinitionOrEnterpriseStub[];
  noSearchResultsContent: React.ReactNode;
  suggestedConnectorDefinitionIds?: string[];
  onConnectorButtonClick: (definition: ConnectorDefinitionOrEnterpriseStub) => void;
  onOpenRequestConnectorModal?: () => void;
  showConnectorBuilderButton?: boolean;
}

export const ConnectorList: React.FC<ConnectorListProps> = ({
  sorting,
  displayType,
  connectorDefinitions,
  noSearchResultsContent,
  suggestedConnectorDefinitionIds,
  onConnectorButtonClick,
  onOpenRequestConnectorModal,
  showConnectorBuilderButton = false,
}) => {
  const sortedConnectorDefinitions = useMemo(
    () =>
      [...connectorDefinitions].sort((a, b) => {
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
            const key =
              "isEnterprise" in definition
                ? definition.id
                : isSourceDefinition(definition)
                ? definition.sourceDefinitionId
                : definition.destinationDefinitionId;
            return <ConnectorButton definition={definition} onClick={onConnectorButtonClick} key={key} maxLines={3} />;
          })}

          {showConnectorBuilderButton && <BuilderConnectorButton layout="vertical" />}
          {!!onOpenRequestConnectorModal && <RequestNewConnectorButton onClick={onOpenRequestConnectorModal} />}
        </div>
      ) : (
        <FlexContainer className={styles.connectorList} direction="column" gap="sm">
          {sortedConnectorDefinitions.map((definition) => {
            const key =
              "isEnterprise" in definition
                ? definition.id
                : isSourceDefinition(definition)
                ? definition.sourceDefinitionId
                : definition.destinationDefinitionId;
            return (
              <ConnectorButton
                className={styles.connectorListButton}
                definition={definition}
                onClick={onConnectorButtonClick}
                key={key}
                showMetrics={"metrics" in definition}
                maxLines={2}
              />
            );
          })}

          {showConnectorBuilderButton && (
            <BuilderConnectorButton className={styles.connectorListButton} layout="horizontal" />
          )}
          {!!onOpenRequestConnectorModal && (
            <RequestNewConnectorButton className={styles.connectorListButton} onClick={onOpenRequestConnectorModal} />
          )}
        </FlexContainer>
      )}
    </FlexContainer>
  );
};

type NumericMetric = number | undefined;

const getNumericMetric = (
  connectorDefinition: ConnectorDefinitionOrEnterpriseStub,
  metric: "successRate" | "usage"
): NumericMetric => {
  if ("isEnterprise" in connectorDefinition) {
    return 1;
  }

  const connectorDefinitionWithMetrics = convertToConnectorDefinitionWithMetrics(connectorDefinition);

  const rawMetricValue =
    metric === "successRate"
      ? connectorDefinitionWithMetrics.metrics?.all?.sync_success_rate
      : connectorDefinitionWithMetrics.metrics?.all?.usage;

  if (!isString(rawMetricValue)) {
    return 1;
  }

  const lowercaseMetricValue = rawMetricValue.toLowerCase();
  switch (lowercaseMetricValue) {
    case "low":
      return 2;
    case "medium":
      return 3;
    case "high":
      return 4;
    default:
      return 1;
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
