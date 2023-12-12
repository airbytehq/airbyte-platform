import classNames from "classnames";
import { useMemo, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { Link } from "components/ui/Link";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { ConnectorIds, SvgIcon } from "area/connector/utils";
import { useCurrentWorkspace, useSourceDefinitionList, useDestinationDefinitionList } from "core/api";
import { DestinationDefinitionRead, SourceDefinitionRead } from "core/api/types/AirbyteClient";
import { links } from "core/utils/links";
import { useExperiment } from "hooks/services/Experiment";
import { ConnectionRoutePaths, DestinationPaths, RoutePaths } from "pages/routePaths";

import { AirbyteIllustration, HighlightIndex } from "./AirbyteIllustration";
import styles from "./ConnectionOnboarding.module.scss";
import { SOURCE_DEFINITION_PARAM } from "../CreateConnection/CreateNewSource";
import { NEW_SOURCE_TYPE, SOURCE_TYPE_PARAM } from "../CreateConnection/SelectSource";

interface ConnectionOnboardingProps {
  onCreate: (sourceConnectorTypeId?: string) => void;
}

const DEFAULT_SOURCES = [
  ConnectorIds.Sources.FacebookMarketing,
  ConnectorIds.Sources.Postgres,
  ConnectorIds.Sources.GoogleSheets,
];

const DEFAULT_DESTINATIONS = [
  ConnectorIds.Destinations.BigQuery,
  ConnectorIds.Destinations.Snowflake,
  ConnectorIds.Destinations.Postgres,
];

interface ConnectorSpecificationMap {
  sourceDefinitions: Record<string, SourceDefinitionRead>;
  destinationDefinitions: Record<string, DestinationDefinitionRead>;
}

const roundConnectorCount = (connectors: Record<string, SourceDefinitionRead | DestinationDefinitionRead>): number => {
  return Math.floor(Object.keys(connectors).length / 10) * 10;
};

/**
 * Gets all available connectors and convert them to a map by id, to access them faster.
 */
export const useConnectorSpecificationMap = (): ConnectorSpecificationMap => {
  const { sourceDefinitions: sourceDefinitionsList } = useSourceDefinitionList();
  const { destinationDefinitions: destinationDefinitionsList } = useDestinationDefinitionList();

  const sourceDefinitions = useMemo(
    () =>
      sourceDefinitionsList.reduce<Record<string, SourceDefinitionRead>>((map, def) => {
        map[def.sourceDefinitionId] = def;
        return map;
      }, {}),
    [sourceDefinitionsList]
  );

  const destinationDefinitions = useMemo(
    () =>
      destinationDefinitionsList.reduce<Record<string, DestinationDefinitionRead>>((map, def) => {
        map[def.destinationDefinitionId] = def;
        return map;
      }, {}),
    [destinationDefinitionsList]
  );

  return { sourceDefinitions, destinationDefinitions };
};

export const ConnectionOnboarding: React.FC<ConnectionOnboardingProps> = () => {
  const { formatMessage } = useIntl();
  const { workspaceId } = useCurrentWorkspace();
  const { sourceDefinitions, destinationDefinitions } = useConnectorSpecificationMap();

  const [highlightedSource, setHighlightedSource] = useState<HighlightIndex>(1);
  const [highlightedDestination, setHighlightedDestination] = useState<HighlightIndex>(0);

  const sourceIds = useExperiment("connection.onboarding.sources", "").split(",");
  const destinationIds = useExperiment("connection.onboarding.destinations", "").split(",");

  const createConnectionPath = `/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Connections}/${ConnectionRoutePaths.ConnectionNew}`;
  const createDestinationBasePath = `/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Destination}/${DestinationPaths.SelectDestinationNew}`;

  const createSourcePath = (sourceDefinitionId?: string) => {
    const sourceDefinitionPath = sourceDefinitionId ? `&${SOURCE_DEFINITION_PARAM}=${sourceDefinitionId}` : "";

    return `${createConnectionPath}?${SOURCE_TYPE_PARAM}=${NEW_SOURCE_TYPE}${sourceDefinitionPath}`;
  };

  const sources = useMemo(
    () =>
      DEFAULT_SOURCES.map(
        (defaultId, index) => sourceDefinitions[sourceIds[index] || defaultId] ?? sourceDefinitions[defaultId]
      ),
    [sourceDefinitions, sourceIds]
  );

  const destinations = useMemo(
    () =>
      DEFAULT_DESTINATIONS.map(
        (defaultId, index) =>
          destinationDefinitions[destinationIds[index] || defaultId] ?? destinationDefinitions[defaultId]
      ),
    [destinationDefinitions, destinationIds]
  );

  const moreSourcesTooltip = formatMessage(
    { id: "connection.onboarding.moreSources" },
    { count: roundConnectorCount(sourceDefinitions) }
  );

  const moreDestinationsTooltip = formatMessage(
    { id: "connection.onboarding.moreDestinations" },
    { count: roundConnectorCount(destinationDefinitions) }
  );

  return (
    <div className={styles.container}>
      <Heading as="h2" size="lg" centered className={styles.heading}>
        <FormattedMessage id="connection.onboarding.title" />
      </Heading>
      <div className={styles.connectors}>
        <div className={styles.sources}>
          <Text bold as="div" className={styles.sourcesTitle}>
            <Tooltip
              control={
                <span>
                  <FormattedMessage id="connection.onboarding.sources" /> <Icon type="question" size="sm" />
                </span>
              }
            >
              <FormattedMessage id="connection.onboarding.sourcesDescription" />
            </Tooltip>
          </Text>
          {sources.map((source, index) => {
            const tooltipText = formatMessage({ id: "connection.onboarding.addSource" }, { source: source?.name });
            return (
              <Tooltip
                key={source?.sourceDefinitionId}
                placement="right"
                control={
                  <Link
                    data-testid={`onboardingSource-${index}`}
                    data-source-definition-id={source?.sourceDefinitionId}
                    aria-label={tooltipText}
                    to={createSourcePath(source?.sourceDefinitionId)}
                  >
                    <FlexContainer
                      className={styles.connectorButton}
                      onMouseEnter={() => setHighlightedSource(index as HighlightIndex)}
                      alignItems="center"
                      justifyContent="center"
                    >
                      <div className={styles.connectorIcon}>
                        <SvgIcon svg={source?.icon} />
                      </div>
                    </FlexContainer>
                  </Link>
                }
              >
                {tooltipText}
              </Tooltip>
            );
          })}

          <Tooltip
            placement="right"
            control={
              <Link data-testid="onboardingSource-more" to={createSourcePath()} aria-label={moreSourcesTooltip}>
                <FlexContainer
                  onMouseEnter={() => setHighlightedSource(3)}
                  className={styles.connectorButton}
                  alignItems="center"
                  justifyContent="center"
                >
                  <Icon type="plus" className={styles.moreIcon} />
                </FlexContainer>
              </Link>
            }
          >
            {moreSourcesTooltip}
          </Tooltip>
        </div>
        <div className={styles.airbyte} aria-hidden="true">
          <AirbyteIllustration
            sourceHighlighted={highlightedSource}
            destinationHighlighted={highlightedDestination}
            className={styles.illustration}
          />
        </div>
        <div className={styles.destinations}>
          <Text bold as="div" className={styles.destinationsTitle}>
            <Tooltip
              control={
                <span>
                  <FormattedMessage id="connection.onboarding.destinations" /> <Icon type="question" size="sm" />
                </span>
              }
            >
              <FormattedMessage id="connection.onboarding.destinationsDescription" />
            </Tooltip>
          </Text>
          {destinations.map((destination, index) => {
            const tooltipText = formatMessage(
              { id: "connection.onboarding.addDestination" },
              { destination: destination?.name }
            );
            return (
              <Tooltip
                key={destination?.destinationDefinitionId}
                placement="right"
                control={
                  <Link
                    data-testid={`onboardingDestination-${index}`}
                    data-destination-definition-id={destination?.destinationDefinitionId}
                    aria-label={tooltipText}
                    to={`${createDestinationBasePath}/${destination.destinationDefinitionId}`}
                  >
                    <FlexContainer
                      className={styles.connectorButton}
                      onMouseEnter={() => setHighlightedDestination(index as HighlightIndex)}
                      alignItems="center"
                      justifyContent="center"
                    >
                      <div className={styles.connectorIcon}>
                        <SvgIcon svg={destination?.icon} />
                      </div>
                    </FlexContainer>
                  </Link>
                }
              >
                {tooltipText}
              </Tooltip>
            );
          })}
          <Tooltip
            placement="right"
            control={
              <Link
                data-testid="onboardingDestination-more"
                to={createDestinationBasePath}
                aria-label={moreDestinationsTooltip}
              >
                <FlexContainer
                  onMouseEnter={() => setHighlightedDestination(3)}
                  className={styles.connectorButton}
                  alignItems="center"
                  justifyContent="center"
                >
                  <Icon type="plus" className={styles.moreIcon} />
                </FlexContainer>
              </Link>
            }
          >
            {moreDestinationsTooltip}
          </Tooltip>
        </div>
      </div>
      <div className={styles.footer}>
        <Link
          to={createConnectionPath}
          data-testid="new-connection-button"
          className={classNames(styles.button, styles.typePrimary, styles.sizeL, styles.linkText)}
        >
          <FormattedMessage id="connection.onboarding.createFirst" />
        </Link>
        <FormattedMessage
          tagName="span"
          id="connection.onboarding.demoInstance"
          values={{
            demoLnk: (children: React.ReactNode) => (
              <a href={links.demoLink} target="_blank" rel="noreferrer noopener" className={styles.demoLink}>
                {children}
              </a>
            ),
          }}
        />
      </div>
    </div>
  );
};
