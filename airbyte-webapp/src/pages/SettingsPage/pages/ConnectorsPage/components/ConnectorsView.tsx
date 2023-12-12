import { createColumnHelper } from "@tanstack/react-table";
import React, { useCallback, useMemo, useState } from "react";
import { FormattedMessage } from "react-intl";

import { HeadTitle } from "components/common/HeadTitle";
import { ConnectorBuilderProjectTable } from "components/ConnectorBuilderProjectTable";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Table } from "components/ui/Table";
import { InfoTooltip } from "components/ui/Tooltip";

import { BuilderProject } from "core/api";
import { DestinationDefinitionRead, SourceDefinitionRead } from "core/api/types/AirbyteClient";
import { Connector, ConnectorDefinition } from "core/domain/connector";
import { FeatureItem, useFeature } from "core/services/features";
import { useIntent } from "core/utils/rbac";
import { RoutePaths } from "pages/routePaths";

import { ConnectorCell } from "./ConnectorCell";
import styles from "./ConnectorsView.module.scss";
import { ConnectorsViewContext } from "./ConnectorsViewContext";
import CreateConnector from "./CreateConnector";
import ImageCell from "./ImageCell";
import { UpdateDestinationConnectorVersionCell } from "./UpdateDestinationConnectorVersionCell";
import { UpdateSourceConnectorVersionCell } from "./UpdateSourceConnectorVersionCell";
import UpgradeAllButton from "./UpgradeAllButton";
import { ConnectorVersionFormValues } from "./VersionCell";

export interface ConnectorsViewProps {
  type: "sources" | "destinations";
  usedConnectorsDefinitions: SourceDefinitionRead[] | DestinationDefinitionRead[];
  connectorsDefinitions: SourceDefinitionRead[] | DestinationDefinitionRead[];
  updatingDefinitionId?: string;
  onUpdateVersion: (values: ConnectorVersionFormValues) => Promise<void>;
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

const MemoizedTable = React.memo(Table) as typeof Table;

const ConnectorsView: React.FC<ConnectorsViewProps> = ({
  type,
  onUpdateVersion,
  usedConnectorsDefinitions,
  updatingDefinitionId,
  connectorsDefinitions,
  connectorBuilderProjects,
}) => {
  const [updatingAllConnectors, setUpdatingAllConnectors] = useState(false);
  const hasUpdateConnectorsPermissions = useIntent("UpdateConnectorVersions");
  const allowUpdateConnectors = useFeature(FeatureItem.AllowUpdateConnectors) && hasUpdateConnectorsPermissions;
  const allowUploadCustomImage = useFeature(FeatureItem.AllowUploadCustomImage);

  const showVersionUpdateColumn = useCallback(
    (definitions: ConnectorDefinition[]) => {
      if (allowUpdateConnectors) {
        return true;
      }
      if (allowUploadCustomImage && definitions.some((definition) => definition.custom)) {
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
            supportLevel={props.row.original.supportLevel}
            custom={props.row.original.custom}
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
        header: () => (
          <FlexContainer alignItems="center" gap="none">
            <FormattedMessage id="admin.defaultVersion" />
            {allowUpdateConnectors && (
              <InfoTooltip>
                <FormattedMessage id="admin.defaultVersionDescription" values={{ type }} />
              </InfoTooltip>
            )}
          </FlexContainer>
        ),
        meta: {
          thClassName: styles.thCurrentVersion,
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
                allowUpdateConnectors || (allowUploadCustomImage && props.row.original.custom) ? (
                  type === "sources" ? (
                    <UpdateSourceConnectorVersionCell
                      connectorDefinitionId={Connector.id(props.row.original)}
                      onChange={onUpdateVersion}
                      currentVersion={props.row.original.dockerImageTag}
                      custom={props.row.original.custom}
                    />
                  ) : (
                    <UpdateDestinationConnectorVersionCell
                      connectorDefinitionId={Connector.id(props.row.original)}
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
    }),
    [updatingDefinitionId, updatingAllConnectors]
  );

  const showUpdateColumnForUsedDefinitions = showVersionUpdateColumn(usedConnectorsDefinitions);
  const usedDefinitionColumns = useMemo(
    () => renderColumns(showUpdateColumnForUsedDefinitions),
    [renderColumns, showUpdateColumnForUsedDefinitions]
  );
  const showUpdateColumnForDefinitions = showVersionUpdateColumn(connectorsDefinitions);
  const definitionColumns = useMemo(
    () => renderColumns(showUpdateColumnForDefinitions),
    [renderColumns, showUpdateColumnForDefinitions]
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

  if (filteredUsedConnectorsDefinitions.length > 0) {
    sections.push({
      title: type === "sources" ? "admin.manageSource" : "admin.manageDestination",
      content: (
        <MemoizedTable columns={usedDefinitionColumns} data={filteredUsedConnectorsDefinitions} sorting={false} />
      ),
    });
  }

  sections.push({
    title: type === "sources" ? "admin.availableSource" : "admin.availableDestinations",
    content: <MemoizedTable columns={definitionColumns} data={filteredConnectorsDefinitions} sorting={false} />,
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
