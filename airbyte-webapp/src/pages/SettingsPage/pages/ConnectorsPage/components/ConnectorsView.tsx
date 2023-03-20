import { createColumnHelper } from "@tanstack/react-table";
import { useCallback, useMemo, useState } from "react";
import { FormattedMessage } from "react-intl";

import { HeadTitle } from "components/common/HeadTitle";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { NextTable } from "components/ui/NextTable";

import { Connector, ConnectorDefinition } from "core/domain/connector";
import { DestinationDefinitionRead, SourceDefinitionRead } from "core/request/AirbyteClient";
import { useAvailableConnectorDefinitions } from "hooks/domain/connector/useAvailableConnectorDefinitions";
import { FeatureItem, useFeature } from "hooks/services/Feature";
import { useCurrentWorkspace } from "hooks/services/useWorkspace";

import ConnectorCell from "./ConnectorCell";
import styles from "./ConnectorsView.module.scss";
import { ConnectorsViewContext } from "./ConnectorsViewContext";
import CreateConnector from "./CreateConnector";
import ImageCell from "./ImageCell";
import { UpdateDestinationConnectorVersionCell } from "./UpdateDestinationConnectorVersionCell";
import { UpdateSourceConnectorVersionCell } from "./UpdateSourceConnectorVersionCell";
import UpgradeAllButton from "./UpgradeAllButton";

export interface ConnectorsViewProps {
  type: "sources" | "destinations";
  usedConnectorsDefinitions: SourceDefinitionRead[] | DestinationDefinitionRead[];
  connectorsDefinitions: SourceDefinitionRead[] | DestinationDefinitionRead[];
  updatingDefinitionId?: string;
  onUpdateVersion: ({ id, version }: { id: string; version: string }) => void;
  feedbackList: Record<string, string>;
}

const ConnectorsView: React.FC<ConnectorsViewProps> = ({
  type,
  onUpdateVersion,
  feedbackList,
  usedConnectorsDefinitions,
  updatingDefinitionId,
  connectorsDefinitions,
}) => {
  const [updatingAllConnectors, setUpdatingAllConnectors] = useState(false);
  const allowUpdateConnectors = useFeature(FeatureItem.AllowUpdateConnectors);
  const allowUploadCustomImage = useFeature(FeatureItem.AllowUploadCustomImage);
  const workspace = useCurrentWorkspace();
  const availableConnectorDefinitions = useAvailableConnectorDefinitions<ConnectorDefinition>(
    connectorsDefinitions,
    workspace
  );
  const showVersionUpdateColumn = useCallback(
    (definitions: ConnectorDefinition[]) => {
      if (allowUpdateConnectors) {
        return true;
      }
      if (allowUploadCustomImage && definitions.some((definition) => definition.releaseStage === "custom")) {
        return true;
      }
      return false;
    },
    [allowUpdateConnectors, allowUploadCustomImage]
  );

  const columnHelper = createColumnHelper<ConnectorDefinition>();

  const renderColumns = useCallback(
    (showVersionUpdateColumn: boolean) => [
      columnHelper.accessor("name", {
        header: () => <FormattedMessage id="admin.connectors" />,
        meta: {
          thClassName: styles.thName,
        },
        cell: (props) => (
          <ConnectorCell
            connectorName={props.cell.getValue()}
            img={props.row.original.icon}
            currentVersion={props.row.original.dockerImageTag}
            releaseStage={props.row.original.releaseStage}
            id={Connector.id(props.row.original)}
            type={type}
          />
        ),
      }),
      columnHelper.accessor("dockerRepository", {
        header: () => <FormattedMessage id="admin.image" />,
        meta: {
          thClassName: styles.thDockerRepository,
        },
        cell: (props) => <ImageCell imageName={props.cell.getValue()} link={props.row.original.documentationUrl} />,
      }),
      columnHelper.accessor("dockerImageTag", {
        header: () => <FormattedMessage id="admin.currentVersion" />,
        meta: {
          thClassName: styles.thDockerImageTag,
        },
      }),
      ...(showVersionUpdateColumn
        ? [
            columnHelper.display({
              id: "versionUpdate",
              header: () => (
                <div className={styles.changeToHeader}>
                  <FormattedMessage id="admin.changeTo" />
                </div>
              ),
              cell: (props) =>
                allowUpdateConnectors || (allowUploadCustomImage && props.row.original.releaseStage === "custom") ? (
                  type === "sources" ? (
                    <UpdateSourceConnectorVersionCell
                      id={Connector.id(props.row.original)}
                      onChange={onUpdateVersion}
                      currentVersion={props.row.original.dockerImageTag}
                    />
                  ) : (
                    <UpdateDestinationConnectorVersionCell
                      id={Connector.id(props.row.original)}
                      onChange={onUpdateVersion}
                      currentVersion={props.row.original.dockerImageTag}
                    />
                  )
                ) : null,
            }),
          ]
        : []),
    ],
    [columnHelper, allowUpdateConnectors, allowUploadCustomImage, onUpdateVersion, type]
  );

  const renderHeaderControls = (section: "used" | "available") =>
    ((section === "used" && usedConnectorsDefinitions.length > 0) ||
      (section === "available" && usedConnectorsDefinitions.length === 0)) && (
      <FlexContainer>
        {allowUploadCustomImage && <CreateConnector type={type} />}
        {allowUpdateConnectors && <UpgradeAllButton connectorType={type} />}
      </FlexContainer>
    );

  const ctx = useMemo(
    () => ({
      setUpdatingAll: setUpdatingAllConnectors,
      updatingAll: updatingAllConnectors,
      updatingDefinitionId,
      feedbackList,
    }),
    [feedbackList, updatingDefinitionId, updatingAllConnectors]
  );

  const usedDefinitionColumns = useMemo(
    () => renderColumns(showVersionUpdateColumn(usedConnectorsDefinitions)),
    [renderColumns, showVersionUpdateColumn, usedConnectorsDefinitions]
  );
  const availableDefinitionColumns = useMemo(
    () => renderColumns(showVersionUpdateColumn(availableConnectorDefinitions)),
    [renderColumns, showVersionUpdateColumn, availableConnectorDefinitions]
  );

  return (
    <ConnectorsViewContext.Provider value={ctx}>
      <div className={styles.connectorsTable}>
        <HeadTitle
          titles={[{ id: "sidebar.settings" }, { id: type === "sources" ? "admin.sources" : "admin.destinations" }]}
        />
        <FlexContainer direction="column" gap="2xl">
          {usedConnectorsDefinitions.length > 0 && (
            <FlexContainer direction="column">
              <FlexContainer alignItems="center">
                <FlexItem grow>
                  <Heading as="h2">
                    <FormattedMessage id={type === "sources" ? "admin.manageSource" : "admin.manageDestination"} />
                  </Heading>
                </FlexItem>
                {renderHeaderControls("used")}
              </FlexContainer>
              <NextTable columns={usedDefinitionColumns} data={usedConnectorsDefinitions} />
            </FlexContainer>
          )}

          <FlexContainer direction="column">
            <FlexContainer alignItems="center">
              <FlexItem grow>
                <Heading as="h2">
                  <FormattedMessage id={type === "sources" ? "admin.availableSource" : "admin.availableDestinations"} />
                </Heading>
              </FlexItem>
              {renderHeaderControls("available")}
            </FlexContainer>
            <NextTable columns={availableDefinitionColumns} data={availableConnectorDefinitions} />
          </FlexContainer>
        </FlexContainer>
      </div>
    </ConnectorsViewContext.Provider>
  );
};

export default ConnectorsView;
