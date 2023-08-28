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
import { ConnectorDefinition } from "core/domain/connector";
import { isSourceDefinition } from "core/domain/connector/source";
import { ReleaseStage } from "core/request/AirbyteClient";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { useLocalStorage } from "core/utils/useLocalStorage";
import { useModalService } from "hooks/services/Modal";
import RequestConnectorModal from "views/Connector/RequestConnectorModal";

import { ConnectorGrid } from "./ConnectorGrid";
import { FilterReleaseStage } from "./FilterReleaseStage";
import styles from "./SelectConnector.module.scss";
import { useTrackSelectConnector } from "./useTrackSelectConnector";

interface SelectConnectorProps {
  connectorType: "source" | "destination";
  connectorDefinitions: ConnectorDefinition[];
  onSelectConnectorDefinition: (id: string) => void;
  suggestedConnectorDefinitionIds: string[];
}

const RELEASE_STAGES: ReleaseStage[] = ["generally_available", "beta", "alpha", "custom"];
export const DEFAULT_SELECTED_RELEASE_STAGES: ReleaseStage[] = ["generally_available", "beta", "custom"];

export const SelectConnector: React.FC<SelectConnectorProps> = ({
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
  const defaultReleaseStages = DEFAULT_SELECTED_RELEASE_STAGES.filter((stage) =>
    connectorDefinitions.some((definition) => definition.releaseStage === stage)
  );
  const [releaseStagesInLocalStorage, setSelectedReleaseStages] = useLocalStorage(
    "airbyte_connector-grid-release-stage-filter",
    defaultReleaseStages
  );
  const availableReleaseStages = RELEASE_STAGES.filter((stage) =>
    connectorDefinitions.some((d) => d.releaseStage === stage)
  );
  const selectedReleaseStages = releaseStagesInLocalStorage.filter((releaseStage) =>
    availableReleaseStages.includes(releaseStage)
  );
  if (selectedReleaseStages.length === 0) {
    selectedReleaseStages.push(...defaultReleaseStages);
  }

  const updateSelectedReleaseStages = (updatedSelectedReleaseStages: ReleaseStage[]) => {
    // It's possible there was a release stage selected that is currently not being displayed.
    // We should add that back in before we persist to local storage.
    const hiddenReleaseStages = releaseStagesInLocalStorage.filter((stage) => !selectedReleaseStages.includes(stage));
    setSelectedReleaseStages([...updatedSelectedReleaseStages, ...hiddenReleaseStages]);
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
        .filter((definition) => definition.releaseStage && selectedReleaseStages.includes(definition.releaseStage))
        .filter((definition) => definition.name.toLowerCase().includes(searchTerm.toLocaleLowerCase())),
    [searchTerm, connectorDefinitions, selectedReleaseStages]
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
            <FilterReleaseStage
              availableReleaseStages={availableReleaseStages}
              selectedReleaseStages={selectedReleaseStages}
              onUpdateSelectedReleaseStages={(releaseStages) => updateSelectedReleaseStages(releaseStages)}
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
            updateSelectedReleaseStages(RELEASE_STAGES);
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
