import classNames from "classnames";
import { useMemo, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { SearchInput } from "components/ui/SearchInput";
import { Text } from "components/ui/Text";

import { useCurrentWorkspace } from "core/api";
import { ConnectorDefinition } from "core/domain/connector";
import { isSourceDefinition } from "core/domain/connector/source";
import { ReleaseStage } from "core/request/AirbyteClient";
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
}

const RELEASE_STAGES: ReleaseStage[] = ["generally_available", "beta", "alpha", "custom"];
const DEFAULT_SELECTED_RELEASE_STAGES: ReleaseStage[] = ["generally_available", "beta", "custom"];

export const SelectConnector: React.FC<SelectConnectorProps> = ({
  connectorType,
  connectorDefinitions,
  onSelectConnectorDefinition,
}) => {
  const { formatMessage } = useIntl();
  const { email } = useCurrentWorkspace();
  const { openModal, closeModal } = useModalService();
  const trackSelectConnector = useTrackSelectConnector(connectorType);
  const [searchTerm, setSearchTerm] = useState("");
  const [selectedReleaseStages, setSelectedReleaseStages] = useState<ReleaseStage[]>(
    DEFAULT_SELECTED_RELEASE_STAGES.filter((stage) =>
      connectorDefinitions.some((definition) => definition.releaseStage === stage)
    )
  );

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

  return (
    <div className={styles.selectConnector}>
      <div className={classNames(styles.selectConnector__gutter, styles["selectConnector__gutter--left"])} />
      <div className={styles.selectConnector__header}>
        <SearchInput value={searchTerm} onChange={(e) => setSearchTerm(e.target.value)} />

        <Box mt="lg">
          <FlexContainer justifyContent="space-between">
            <FilterReleaseStage
              availableReleaseStages={RELEASE_STAGES.filter((stage) =>
                connectorDefinitions.some((d) => d.releaseStage === stage)
              )}
              selectedReleaseStages={selectedReleaseStages}
              onUpdateSelectedReleaseStages={setSelectedReleaseStages}
            />
            <Text color="grey">
              <FormattedMessage id="connector.connectorCount" values={{ count: filteredSearchResults.length }} />
            </Text>
          </FlexContainer>
        </Box>
      </div>
      <div className={classNames(styles.selectConnector__gutter, styles["selectConnector__gutter--right"])} />

      <div className={styles.selectConnector__grid}>
        <ConnectorGrid
          searchResultsHiddenByFilters={allSearchResults.length > filteredSearchResults.length}
          connectorDefinitions={filteredSearchResults}
          onConnectorButtonClick={handleConnectorButtonClick}
          onOpenRequestConnectorModal={onOpenRequestConnectorModal}
          showConnectorBuilderButton={connectorType === "source"}
        />
      </div>
    </div>
  );
};
