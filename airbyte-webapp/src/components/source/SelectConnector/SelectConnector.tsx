import classNames from "classnames";
import { useCallback, useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useDebounce } from "react-use";
import { match } from "ts-pattern";

import { ConnectorIcon } from "components/common/ConnectorIcon";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { SearchInput } from "components/ui/SearchInput";
import { SortableTableHeader } from "components/ui/Table/SortableTableHeader";
import { ButtonTab, Tabs } from "components/ui/Tabs";
import { Text } from "components/ui/Text";

import { useCurrentWorkspace, useFilters } from "core/api";
import { ConnectorDefinition } from "core/domain/connector";
import { isSourceDefinition } from "core/domain/connector/source";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { useModalService } from "hooks/services/Modal";
import { useAirbyteTheme } from "hooks/theme/useAirbyteTheme";

import { ConnectorList } from "./ConnectorList";
import { RequestConnectorModal } from "./RequestConnectorModal";
import styles from "./SelectConnector.module.scss";
import { useTrackSelectConnector } from "./useTrackSelectConnector";

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

  const [{ search: searchTerm, tab: selectedTab, col: sortColumn, asc }, setFilterValue] = useFilters<{
    search: string;
    tab: ConnectorTab;
    col: ConnectorSortColumn;
    asc: "true" | "false";
  }>({
    search: "",
    tab: "certified",
    col: "name",
    asc: "true",
  });
  const isSortAscending = asc === "true";

  const handleConnectorButtonClick = (definition: ConnectorDefinition) => {
    if (isSourceDefinition(definition)) {
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
        setFilterValue("asc", !isSortAscending ? "true" : "false");
      } else {
        setFilterValue("col", clickedColumn);
        setFilterValue("asc", clickedColumn === "successRate" || clickedColumn === "usage" ? "false" : "true");
      }
    },
    [isSortAscending, setFilterValue, sortColumn]
  );

  const setSelectedTab = useCallback(
    (tab: ConnectorTab) => {
      setFilterValue("tab", tab);
      setFilterValue("col", "name");
      setFilterValue("asc", "true");
    },
    [setFilterValue]
  );

  const hasCustomConnectors = useMemo(
    () => connectorDefinitions.some((definition) => definition.supportLevel === "none"),
    [connectorDefinitions]
  );

  const allSearchResults = useMemo(
    () =>
      connectorDefinitions.filter((definition) =>
        definition.name.toLowerCase().includes(searchTerm.toLocaleLowerCase())
      ),
    [searchTerm, connectorDefinitions]
  );

  const searchResultsByTab: Record<ConnectorTab, ConnectorDefinition[]> = useMemo(
    () =>
      allSearchResults.reduce(
        (acc, definition) => {
          if (definition.supportLevel) {
            switch (definition.supportLevel) {
              case "certified":
                acc.certified.push(definition);
                break;
              case "community":
                acc.marketplace.push(definition);
                break;
              case "none":
                acc.custom.push(definition);
                break;
            }
          }
          return acc;
        },
        {
          certified: [],
          marketplace: [],
          custom: [],
        } as Record<ConnectorTab, ConnectorDefinition[]>
      ),
    [allSearchResults]
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
              className={styles.selectConnector__seeMore}
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
          onChange={(e) => setFilterValue("search", e.target.value)}
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
