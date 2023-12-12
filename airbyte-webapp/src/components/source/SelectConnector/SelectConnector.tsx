import classNames from "classnames";
import { useMemo, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useDebounce } from "react-use";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { SearchInput } from "components/ui/SearchInput";
import { Text } from "components/ui/Text";

import { SuggestedConnectors } from "area/connector/components";
import { useCurrentWorkspace } from "core/api";
import { SupportLevel } from "core/api/types/AirbyteClient";
import { ConnectorDefinition } from "core/domain/connector";
import { isSourceDefinition } from "core/domain/connector/source";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { useLocalStorage } from "core/utils/useLocalStorage";
import { useModalService } from "hooks/services/Modal";
import RequestConnectorModal from "views/Connector/RequestConnectorModal";

import { ConnectorGrid } from "./ConnectorGrid";
import { FilterSupportLevel } from "./FilterSupportLevel";
import styles from "./SelectConnector.module.scss";
import { useTrackSelectConnector } from "./useTrackSelectConnector";

interface SelectConnectorProps {
  connectorType: "source" | "destination";
  connectorDefinitions: ConnectorDefinition[];
  onSelectConnectorDefinition: (id: string) => void;
  suggestedConnectorDefinitionIds: string[];
}

const SUPPORT_LEVELS: SupportLevel[] = ["certified", "community", "none"];
export const DEFAULT_SELECTED_SUPPORT_LEVELS: SupportLevel[] = ["certified", "community", "none"];

export const SelectConnector: React.FC<SelectConnectorProps> = (props) => {
  return <SelectConnectorSupportLevel {...props} />;
};

const SelectConnectorSupportLevel: React.FC<SelectConnectorProps> = ({
  connectorType,
  connectorDefinitions,
  onSelectConnectorDefinition,
  suggestedConnectorDefinitionIds,
}) => {
  const { formatMessage } = useIntl();
  const { email } = useCurrentWorkspace();
  const { openModal, closeModal } = useModalService();
  const trackSelectConnector = useTrackSelectConnector(connectorType);
  const [searchTerm, setSearchTerm] = useState("");
  const [supportLevelsInLocalStorage, setSelectedSupportLevels] = useLocalStorage(
    "airbyte_connector-grid-support-level-filter",
    []
  );
  const availableSupportLevels = SUPPORT_LEVELS.filter((stage) =>
    connectorDefinitions.some((d) => d.supportLevel === stage)
  );
  const selectedSupportLevels = supportLevelsInLocalStorage.filter((supportLevel) =>
    availableSupportLevels.includes(supportLevel)
  );
  if (selectedSupportLevels.length === 0) {
    selectedSupportLevels.push(...DEFAULT_SELECTED_SUPPORT_LEVELS);
  }

  const updateSelectedSupportLevels = (updatedSelectedSupportLevels: SupportLevel[]) => {
    // It's possible there was a release stage selected that is currently not being displayed.
    // We should add that back in before we persist to local storage.
    const hiddenSupportLevels = supportLevelsInLocalStorage.filter((stage) => !selectedSupportLevels.includes(stage));
    setSelectedSupportLevels([...updatedSelectedSupportLevels, ...hiddenSupportLevels]);
  };

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
    openModal({
      title: formatMessage({ id: "connector.requestConnector" }),
      content: () => (
        <RequestConnectorModal
          connectorType={connectorType}
          workspaceEmail={email}
          searchedConnectorName={searchTerm}
          onClose={closeModal}
        />
      ),
      size: "sm",
    });

  const filteredSearchResults = useMemo(
    () =>
      connectorDefinitions
        .filter((definition) => definition.supportLevel && selectedSupportLevels.includes(definition.supportLevel))
        .filter((definition) => definition.name.toLowerCase().includes(searchTerm.toLocaleLowerCase())),
    [searchTerm, connectorDefinitions, selectedSupportLevels]
  );

  const allSearchResults = useMemo(
    () =>
      connectorDefinitions.filter((definition) =>
        definition.name.toLowerCase().includes(searchTerm.toLocaleLowerCase())
      ),
    [searchTerm, connectorDefinitions]
  );

  const analyticsService = useAnalyticsService();
  useDebounce(
    () => {
      if (filteredSearchResults.length === 0) {
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

  return (
    <div className={styles.selectConnector}>
      <div className={classNames(styles.selectConnector__gutter, styles["selectConnector__gutter--left"])} />
      <div className={styles.selectConnector__header}>
        <SearchInput value={searchTerm} onChange={(e) => setSearchTerm(e.target.value)} />

        <Box mt="lg">
          <FlexContainer justifyContent="space-between">
            <FilterSupportLevel
              availableSupportLevels={availableSupportLevels}
              selectedSupportLevels={selectedSupportLevels}
              onUpdateSelectedSupportLevels={(supportLevels) => updateSelectedSupportLevels(supportLevels)}
            />
            <Text color="grey">
              <FormattedMessage id="connector.connectorCount" values={{ count: filteredSearchResults.length }} />
            </Text>
          </FlexContainer>
        </Box>
      </div>

      <div className={classNames(styles.selectConnector__gutter, styles["selectConnector__gutter--right"])} />

      {suggestedConnectorDefinitionIds.length > 0 && (
        <div className={styles.selectConnector__suggestedConnectors}>
          <SuggestedConnectors
            definitionIds={suggestedConnectorDefinitionIds}
            onConnectorButtonClick={handleConnectorButtonClick}
          />
        </div>
      )}

      <div className={styles.selectConnector__grid}>
        <ConnectorGrid
          searchResultsHiddenByFilters={
            searchTerm.length > 0 ? allSearchResults.length - filteredSearchResults.length : 0
          }
          onShowAllResultsClick={() => {
            updateSelectedSupportLevels(SUPPORT_LEVELS);
          }}
          connectorDefinitions={filteredSearchResults}
          onConnectorButtonClick={handleConnectorButtonClick}
          onOpenRequestConnectorModal={onOpenRequestConnectorModal}
          showConnectorBuilderButton={connectorType === "source"}
        />
      </div>
    </div>
  );
};
