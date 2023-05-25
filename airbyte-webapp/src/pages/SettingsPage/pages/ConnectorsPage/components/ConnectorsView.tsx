import { createColumnHelper } from "@tanstack/react-table";
import { useCallback, useMemo, useState } from "react";
import { FormattedMessage } from "react-intl";

import { HeadTitle } from "components/common/HeadTitle";
import { ConnectorBuilderProjectTable } from "components/ConnectorBuilderProjectTable";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Table } from "components/ui/Table";

import { Connector, ConnectorDefinition } from "core/domain/connector";
import { DestinationDefinitionRead, SourceDefinitionRead } from "core/request/AirbyteClient";
import { FeatureItem, useFeature } from "hooks/services/Feature";
import { RoutePaths } from "pages/routePaths";
import { BuilderProject } from "services/connectorBuilder/ConnectorBuilderProjectsService";

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
  connectorBuilderProjects?: BuilderProject[];
}

function filterByBuilderConnectors(
  connectorsDefinitions: Array<SourceDefinitionRead | DestinationDefinitionRead>,
  connectorBuilderProjects?: BuilderProject[]
) {
  const builderDefinitionIds = new Set<string>();
  connectorBuilderProjects?.forEach((project) => {
    if (project.sourceDefinitionId) {
      builderDefinitionIds.add(project.sourceDefinitionId);
    }
  });
  return connectorsDefinitions.filter(
    (definition) =>
      !builderDefinitionIds.has(
        "sourceDefinitionId" in definition ? definition.sourceDefinitionId : definition.destinationDefinitionId
      )
  );
}

const columnHelper = createColumnHelper<ConnectorDefinition>();

const ConnectorsView: React.FC<ConnectorsViewProps> = ({
  type,
  onUpdateVersion,
  feedbackList,
  usedConnectorsDefinitions,
  updatingDefinitionId,
  connectorsDefinitions,
  connectorBuilderProjects,
}) => {
  const [updatingAllConnectors, setUpdatingAllConnectors] = useState(false);
  const allowUpdateConnectors = useFeature(FeatureItem.AllowUpdateConnectors);
  const allowUploadCustomImage = useFeature(FeatureItem.AllowUploadCustomImage);

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
    [allowUpdateConnectors, allowUploadCustomImage, onUpdateVersion, type]
  );

  const filteredUsedConnectorsDefinitions = useMemo(
    () => filterByBuilderConnectors(usedConnectorsDefinitions, connectorBuilderProjects),
    [connectorBuilderProjects, usedConnectorsDefinitions]
  );

  const filteredConnectorsDefinitions = useMemo(
    () => filterByBuilderConnectors(connectorsDefinitions, connectorBuilderProjects),
    [connectorBuilderProjects, connectorsDefinitions]
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
  const definitionColumns = useMemo(
    () => renderColumns(showVersionUpdateColumn(connectorsDefinitions)),
    [renderColumns, showVersionUpdateColumn, connectorsDefinitions]
  );

  const sections: Array<{ title: string; content: React.ReactNode }> = [];

  if (type === "sources" && connectorBuilderProjects && connectorBuilderProjects.length > 0) {
    sections.push({
      title: "admin.managerBuilderConnector",
      content: (
        <ConnectorBuilderProjectTable
          projects={connectorBuilderProjects}
          basePath={`../../${RoutePaths.ConnectorBuilder}/`}
        />
      ),
    });
  }

  if (usedConnectorsDefinitions.length > 0) {
    sections.push({
      title: type === "sources" ? "admin.manageSource" : "admin.manageDestination",
      content: <Table columns={usedDefinitionColumns} data={filteredUsedConnectorsDefinitions} />,
    });
  }

  sections.push({
    title: type === "sources" ? "admin.availableSource" : "admin.availableDestinations",
    content: <Table columns={definitionColumns} data={filteredConnectorsDefinitions} />,
  });

  return (
    <ConnectorsViewContext.Provider value={ctx}>
      <div className={styles.connectorsTable}>
        <HeadTitle
          titles={[{ id: "sidebar.settings" }, { id: type === "sources" ? "admin.sources" : "admin.destinations" }]}
        />
        <FlexContainer direction="column" gap="2xl">
          {sections.map((section, index) => (
            <FlexContainer key={index} direction="column">
              <FlexContainer alignItems="center">
                <FlexItem grow>
                  <Heading as="h2">
                    <FormattedMessage id={section.title} />
                  </Heading>
                </FlexItem>
                {index === 0 && (
                  <FlexContainer>
                    <CreateConnector type={type} />
                    {allowUpdateConnectors && <UpgradeAllButton connectorType={type} />}
                  </FlexContainer>
                )}
              </FlexContainer>
              {section.content}
            </FlexContainer>
          ))}
        </FlexContainer>
      </div>
    </ConnectorsViewContext.Provider>
  );
};

export default ConnectorsView;
