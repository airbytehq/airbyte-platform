import classNames from "classnames";
import { useCallback, useMemo, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";
import { useDebounce } from "react-use";
import { match } from "ts-pattern";

import { ConnectorIcon } from "components/ConnectorIcon";
import { Button } from "components/ui/Button";
import { CheckBox } from "components/ui/CheckBox";
import { FlexContainer } from "components/ui/Flex";
import { SearchInput } from "components/ui/SearchInput";
import { SortableTableHeader } from "components/ui/Table/SortableTableHeader";
import { ButtonTab, Tabs } from "components/ui/Tabs";
import { Text } from "components/ui/Text";

import { useCurrentWorkspaceLink } from "area/workspace/utils";
import { useCurrentWorkspace, useFilters, useListEnterpriseStubsForWorkspace } from "core/api";
import { EnterpriseSourceStub } from "core/api/types/AirbyteClient";
import { ConnectorDefinition, ConnectorDefinitionOrEnterpriseStub } from "core/domain/connector";
import { isSourceDefinition } from "core/domain/connector/source";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { useModalService } from "hooks/services/Modal";
import { useAirbyteTheme } from "hooks/theme/useAirbyteTheme";
import { RoutePaths, SourcePaths } from "pages/routePaths";

import { ConnectorList } from "./ConnectorList";
import { RequestConnectorModal } from "./RequestConnectorModal";
import styles from "./SelectConnector.module.scss";
import { useTrackSelectConnector, useTrackSelectEnterpriseStub } from "./useTrackSelectConnector";

const AIRBYTE_CONNECTORS_CHECKBOX = "airbyteConnectorsCheckbox";
const ENTERPRISE_CONNECTORS_CHECKBOX = "enterpriseConnectorsCheckbox";

export type ConnectorTab = "certified" | "marketplace" | "custom";
export type ConnectorSortColumn = "name" | "successRate" | "usage";
export interface ConnectorSorting {
  column: ConnectorSortColumn;
  isAscending: boolean;
}

interface SelectConnectorProps {
  connectorType: "source" | "destination";
  connectorDefinitions: ConnectorDefinition[];
  onSelectConnectorDefinition: (id: string) => void;
  suggestedConnectorDefinitionIds: string[];
}

export const SelectConnector: React.FC<SelectConnectorProps> = ({
  connectorType,
  connectorDefinitions,
  onSelectConnectorDefinition,
  suggestedConnectorDefinitionIds,
}) => {
  const { formatMessage } = useIntl();
  const { email } = useCurrentWorkspace();
  const { openModal } = useModalService();
  const trackSelectConnector = useTrackSelectConnector(connectorType);
  const trackSelectEnterpriseStub = useTrackSelectEnterpriseStub();

  const [showAirbyteConnectors, setShowAirbyteConnectors] = useState(true);
  const [showEnterpriseConnectors, setShowEnterpriseConnectors] = useState(true);

  const handleAirbyteCheckboxChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (showEnterpriseConnectors || e.target.checked) {
      setShowAirbyteConnectors(e.target.checked);
    }
  };

  const handleEnterpriseCheckboxChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (showAirbyteConnectors || e.target.checked) {
      setShowEnterpriseConnectors(e.target.checked);
    }
  };

  // Fetch enterprise source stubs
  const { enterpriseSourceDefinitions } = useListEnterpriseStubsForWorkspace();

  const createLink = useCurrentWorkspaceLink();

  const onSelectEnterpriseSourceStub = (definition: EnterpriseSourceStub) => {
    // This is a temporary routing solution to navigate to the enterprise stub sales funnel.
    // If/when we implement enterprise connectors in the catalog, we should use onSelectConnectorDefinition.
    navigate(createLink(`/${RoutePaths.Source}/${SourcePaths.EnterpriseSource.replace(":id", definition.id)}`));
  };

  interface BaseFilters {
    search: string;
    tab: ConnectorTab;
    col: ConnectorSortColumn;
    asc: "true" | "false";
  }

  /*
  by splitting these into source_* and destination_* , we avoid a fun race condition:
    * the views in this flow are based on a mix of component states and URL params
    * filters are stored in and read from URL params

  If the filter names are kept the same between source and destination selection,
  when in an empty workspace (no sources or destinations), and selecting a source connector after applying filters, the following happens:
    * any filters are stored in the URL
    * user selects a connector, sourceDefinitionId is added to the URL triggering the source configuration view
    * onSubmit of source configuration, CreateNewSource::onCreateSource fires, adding sourceId to the URL
    * CreateConnectionPage sees `sourceId` and renders DefineDestination
      * empty workspaces default to create a destination, and we're back here in SelectConnector
      * SelectConnector defines its filters, grabbing the existing filters from the URL
    * CreateConnectionPage's useEffect watching URL's `sourceId` that deletes all URL params except `sourceId` (removing any set filters)
    * Another render pass is triggered, and the filter logic sees the filter values in memory don't match the URL, insertting the filters back into the URL
  */
  type DynamicFilters = {
    [Key in keyof BaseFilters as `${typeof connectorType}_${Key}`]: BaseFilters[Key];
  };

  const searchFilterName = `${connectorType}_search` as const;
  const tabFilterName = `${connectorType}_tab` as const;
  const colFilterName = `${connectorType}_col` as const;
  const ascFilterName = `${connectorType}_asc` as const;

  const [
    { [searchFilterName]: searchTerm, [tabFilterName]: selectedTab, [colFilterName]: sortColumn, [ascFilterName]: asc },
    setFilterValue,
  ] = useFilters<DynamicFilters>({
    [searchFilterName]: "",
    [tabFilterName]: "certified",
    [colFilterName]: "name",
    [ascFilterName]: "true",
  } as DynamicFilters);

  const isSortAscending = asc === "true";
  const navigate = useNavigate();

  const handleConnectorButtonClick = (definition: ConnectorDefinitionOrEnterpriseStub) => {
    if ("isEnterprise" in definition) {
      // Handle EnterpriseSourceStubs first
      trackSelectEnterpriseStub(definition);
      onSelectEnterpriseSourceStub(definition);
    } else if (isSourceDefinition(definition)) {
      trackSelectConnector(definition.sourceDefinitionId, definition.name);
      onSelectConnectorDefinition(definition.sourceDefinitionId);
    } else {
      trackSelectConnector(definition.destinationDefinitionId, definition.name);
      onSelectConnectorDefinition(definition.destinationDefinitionId);
    }
  };

  const onOpenRequestConnectorModal = () =>
    openModal<void>({
      title: formatMessage({ id: "connector.requestConnector" }),
      content: ({ onComplete, onCancel }) => (
        <RequestConnectorModal
          connectorType={connectorType}
          workspaceEmail={email}
          searchedConnectorName={searchTerm}
          onSubmit={onComplete}
          onCancel={onCancel}
        />
      ),
      size: "sm",
    });

  const handleSortClick = useCallback(
    (clickedColumn: ConnectorSortColumn) => {
      if (clickedColumn === sortColumn) {
        setFilterValue(ascFilterName, !isSortAscending ? "true" : "false");
      } else {
        setFilterValue(colFilterName, clickedColumn);
        setFilterValue(ascFilterName, clickedColumn === "successRate" || clickedColumn === "usage" ? "false" : "true");
      }
    },
    [isSortAscending, setFilterValue, sortColumn, ascFilterName, colFilterName]
  );

  const setSelectedTab = useCallback(
    (tab: ConnectorTab) => {
      setFilterValue(tabFilterName, tab);
      setFilterValue(colFilterName, "name");
      setFilterValue(ascFilterName, "true");
      // Reset filter checkboxes when switching tabs
      setShowAirbyteConnectors(true);
      setShowEnterpriseConnectors(true);
    },
    [setFilterValue, tabFilterName, colFilterName, ascFilterName]
  );

  const hasCustomConnectors = useMemo(
    () => connectorDefinitions.some((definition) => definition.supportLevel === "none"),
    [connectorDefinitions]
  );

  // Combine regular connectors with enterprise stubs
  const connectorListWithEnterpriseStubs = useMemo<ConnectorDefinitionOrEnterpriseStub[]>(() => {
    if (connectorType === "source") {
      return [...connectorDefinitions, ...enterpriseSourceDefinitions];
    }
    return connectorDefinitions;
  }, [connectorType, connectorDefinitions, enterpriseSourceDefinitions]);

  function keywordMatch(definition: ConnectorDefinitionOrEnterpriseStub, searchTerm: string) {
    const keywords = searchTerm.toLowerCase().split(" ").filter(Boolean);
    const name = definition.name.toLowerCase();
    return keywords.every((keyword) => name.includes(keyword));
  }

  // Filter all connectors based on search term
  const allSearchResults = useMemo(
    () => connectorListWithEnterpriseStubs.filter((definition) => keywordMatch(definition, searchTerm)),
    [connectorListWithEnterpriseStubs, searchTerm]
  );

  const searchResultsByTab: Record<ConnectorTab, ConnectorDefinitionOrEnterpriseStub[]> = useMemo(
    () =>
      allSearchResults.reduce(
        (acc, definition) => {
          const isEnterpriseConnector = "isEnterprise" in definition;
          const supportLevel = isEnterpriseConnector ? "certified" : definition.supportLevel;

          switch (supportLevel) {
            case "certified":
              if (
                (isEnterpriseConnector && showEnterpriseConnectors) ||
                (!isEnterpriseConnector && showAirbyteConnectors)
              ) {
                acc.certified.push(definition);
              }
              break;
            case "community":
              acc.marketplace.push(definition);
              break;
            case "none":
              acc.custom.push(definition);
              break;
            case undefined:
              break;
          }
          return acc;
        },
        {
          certified: [],
          marketplace: [],
          custom: [],
        } as Record<ConnectorTab, ConnectorDefinitionOrEnterpriseStub[]>
      ),
    [allSearchResults, showAirbyteConnectors, showEnterpriseConnectors]
  );

  const certifiedBadge = useMemo(
    () =>
      searchTerm && searchResultsByTab.certified.length > 0
        ? searchResultsByTab.certified.length.toString()
        : undefined,
    [searchTerm, searchResultsByTab.certified.length]
  );

  const marketplaceBadge = useMemo(
    () =>
      searchTerm && searchResultsByTab.marketplace.length > 0
        ? searchResultsByTab.marketplace.length.toString()
        : undefined,
    [searchTerm, searchResultsByTab.marketplace.length]
  );

  const customBadge = useMemo(
    () =>
      searchTerm && searchResultsByTab.custom.length > 0 ? searchResultsByTab.custom.length.toString() : undefined,
    [searchTerm, searchResultsByTab.custom.length]
  );

  const analyticsService = useAnalyticsService();
  useDebounce(
    () => {
      if (allSearchResults.length === 0) {
        analyticsService.track(
          connectorType === "source" ? Namespace.SOURCE : Namespace.DESTINATION,
          Action.NO_MATCHING_CONNECTOR,
          {
            actionDescription: "Connector query without results",
            query: searchTerm,
          }
        );
      }
    },
    350,
    [searchTerm]
  );

  const getTabDisplayName = useCallback(
    (tab: ConnectorTab) =>
      match(tab)
        .with("certified", () => formatMessage({ id: "connector.tab.certified" }))
        .with("marketplace", () => formatMessage({ id: "connector.tab.marketplace" }))
        .with("custom", () => formatMessage({ id: "connector.tab.custom" }))
        .exhaustive(),
    [formatMessage]
  );
  const { theme } = useAirbyteTheme();

  const seeMoreButtons = (
    <FlexContainer className={styles.selectConnector__seeMore}>
      {Object.keys(searchResultsByTab)
        .filter((tab) => selectedTab !== tab && searchResultsByTab[tab as ConnectorTab].length > 0)
        .map((tab) => {
          const tabName = tab as ConnectorTab;
          return (
            <Button
              key={tabName}
              data-testid={`see-more-${tabName}`}
              type="button"
              variant="secondary"
              onClick={() => setSelectedTab(tabName)}
            >
              <FlexContainer alignItems="center" gap="lg">
                {tabName !== "custom" && (
                  <FlexContainer gap="none">
                    {searchResultsByTab[tabName].slice(0, 3).map((definition) => (
                      <ConnectorIcon
                        icon={definition.icon}
                        key={definition.name}
                        className={classNames(styles.seeMoreIcon, {
                          [styles.seeMoreIconDarkTheme]: theme === "airbyteThemeDark",
                        })}
                      />
                    ))}
                  </FlexContainer>
                )}
                <div>
                  <FormattedMessage
                    id="connector.seeMore"
                    values={{
                      count: searchResultsByTab[tabName].length,
                      tabName: getTabDisplayName(tabName),
                    }}
                  />
                </div>
              </FlexContainer>
            </Button>
          );
        })}
    </FlexContainer>
  );

  return (
    <div className={styles.selectConnector}>
      <div className={classNames(styles.selectConnector__stickied, styles["selectConnector__gutter--left"])} />
      <FlexContainer
        className={classNames(styles.selectConnector__stickied, styles.selectConnector__header)}
        direction="column"
        gap="lg"
      >
        <SearchInput
          value={searchTerm}
          onChange={(e) => setFilterValue(searchFilterName, e.target.value)}
          placeholder={formatMessage(
            { id: "connector.searchPlaceholder" },
            { tabName: getTabDisplayName(selectedTab) }
          )}
        />
        <Tabs className={styles.selectConnector__tabs}>
          <ButtonTab
            id="certified"
            name={getTabDisplayName("certified")}
            badge={certifiedBadge}
            isActive={selectedTab === "certified"}
            onSelect={() => {
              setSelectedTab("certified");
            }}
          />
          <ButtonTab
            id="marketplace"
            name={getTabDisplayName("marketplace")}
            badge={marketplaceBadge}
            isActive={selectedTab === "marketplace"}
            onSelect={() => setSelectedTab("marketplace")}
          />
          {hasCustomConnectors && (
            <ButtonTab
              id="custom"
              name={getTabDisplayName("custom")}
              badge={customBadge}
              isActive={selectedTab === "custom"}
              onSelect={() => setSelectedTab("custom")}
            />
          )}
        </Tabs>
        {selectedTab === "certified" && (
          <FlexContainer direction="row" gap="lg" alignItems="center">
            <Text size="lg">
              <FormattedMessage id="connector.checkboxFilter.type" />
            </Text>
            <FlexContainer alignItems="center" justifyContent="space-between" gap="md">
              <CheckBox
                id={AIRBYTE_CONNECTORS_CHECKBOX}
                checked={showAirbyteConnectors}
                onChange={handleAirbyteCheckboxChange}
                disabled={showAirbyteConnectors && !showEnterpriseConnectors}
              />
              {/* eslint-disable-next-line jsx-a11y/label-has-associated-control */}
              <label
                htmlFor={AIRBYTE_CONNECTORS_CHECKBOX}
                className={classNames(styles.checkboxLabel, {
                  [styles.disabledCheckboxLabel]: showAirbyteConnectors && !showEnterpriseConnectors,
                })}
              >
                <Text size="sm">
                  <FormattedMessage id="connector.checkboxFilter.certified" />
                </Text>
              </label>
            </FlexContainer>
            <FlexContainer alignItems="center" justifyContent="space-between" gap="md">
              <CheckBox
                id={ENTERPRISE_CONNECTORS_CHECKBOX}
                checked={showEnterpriseConnectors}
                onChange={handleEnterpriseCheckboxChange}
                disabled={showEnterpriseConnectors && !showAirbyteConnectors}
              />
              {/* eslint-disable-next-line jsx-a11y/label-has-associated-control */}
              <label
                htmlFor={ENTERPRISE_CONNECTORS_CHECKBOX}
                className={classNames(styles.checkboxLabel, {
                  [styles.disabledCheckboxLabel]: showEnterpriseConnectors && !showAirbyteConnectors,
                })}
              >
                <Text size="sm">
                  <FormattedMessage id="connector.checkboxFilter.enterprise" />
                </Text>
              </label>
            </FlexContainer>
          </FlexContainer>
        )}
        <FlexContainer
          direction="row"
          justifyContent="space-between"
          alignItems="center"
          className={styles.countAndSort}
        >
          <Text size="sm">
            <FormattedMessage
              id="connector.connectorCount"
              values={{
                count: searchResultsByTab[selectedTab].length,
              }}
            />
          </Text>
          <FlexContainer direction="row" className={styles.sortHeader} gap="lg">
            <SortableTableHeader
              className={styles.sortButton}
              activeClassName={styles.activeSortColumn}
              onClick={() => handleSortClick("name")}
              isActive={sortColumn === "name"}
              isAscending={isSortAscending}
              iconSize="sm"
            >
              <FormattedMessage id="connector.sort.name" />
            </SortableTableHeader>
            {selectedTab === "marketplace" && (
              <>
                <SortableTableHeader
                  className={styles.sortButton}
                  activeClassName={styles.activeSortColumn}
                  onClick={() => handleSortClick("successRate")}
                  isActive={sortColumn === "successRate"}
                  isAscending={isSortAscending}
                  iconSize="sm"
                >
                  <FormattedMessage id="connector.sort.success" />
                </SortableTableHeader>
                <SortableTableHeader
                  className={styles.sortButton}
                  activeClassName={styles.activeSortColumn}
                  onClick={() => handleSortClick("usage")}
                  isActive={sortColumn === "usage"}
                  isAscending={isSortAscending}
                  iconSize="sm"
                >
                  <FormattedMessage id="connector.sort.usage" />
                </SortableTableHeader>
              </>
            )}
          </FlexContainer>
        </FlexContainer>
      </FlexContainer>
      <div className={classNames(styles.selectConnector__stickied, styles["selectConnector__gutter--right"])} />

      <div className={styles.selectConnector__grid}>
        <ConnectorList
          sorting={{
            column: sortColumn,
            isAscending: isSortAscending,
          }}
          displayType={selectedTab === "marketplace" ? "list" : "grid"}
          connectorDefinitions={searchResultsByTab[selectedTab]}
          suggestedConnectorDefinitionIds={
            selectedTab === "certified" ? (searchTerm ? [] : suggestedConnectorDefinitionIds) : undefined
          }
          onConnectorButtonClick={handleConnectorButtonClick}
          onOpenRequestConnectorModal={onOpenRequestConnectorModal}
          showConnectorBuilderButton={connectorType === "source"}
          noSearchResultsContent={
            <FlexContainer direction="column" gap="none">
              <Text size="sm" italicized color="grey400">
                <FormattedMessage
                  id="connector.noSearchResults"
                  values={{
                    tabName: getTabDisplayName(selectedTab),
                  }}
                />
              </Text>
              {seeMoreButtons}
            </FlexContainer>
          }
        />
      </div>

      {searchTerm.length > 0 && searchResultsByTab[selectedTab].length > 0 && seeMoreButtons}
    </div>
  );
};
