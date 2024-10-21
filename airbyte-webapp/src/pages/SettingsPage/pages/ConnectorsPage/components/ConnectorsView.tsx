import { createColumnHelper } from "@tanstack/react-table";
import React, { useCallback, useMemo, useState } from "react";
import { FormattedMessage } from "react-intl";

import { DeleteDestinationDefinitionButton } from "components/connector/DeleteDestinationDefinitionButton";
import { DeleteSourceDefinitionButton } from "components/connector/DeleteSourceDefinitionButton";
import { EditDestinationDefinitionButton } from "components/connector/EditDestinationDefinitionButton";
import { EditSourceDefinitionButton } from "components/connector/EditSourceDefinitionButton";
import { ConnectorBuilderProjectTable } from "components/ConnectorBuilderProjectTable";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Table } from "components/ui/Table";
import { InfoTooltip } from "components/ui/Tooltip";

import { BuilderProject, useCurrentWorkspace } from "core/api";
import { DestinationDefinitionRead, SourceDefinitionRead } from "core/api/types/AirbyteClient";
import { Connector, ConnectorDefinition } from "core/domain/connector";
import { isSourceDefinition } from "core/domain/connector/source";
import { FeatureItem, useFeature } from "core/services/features";
import { Intent, useIntent, useGeneratedIntent } from "core/utils/rbac";
import { RoutePaths } from "pages/routePaths";

import { AddNewConnectorButton } from "./AddNewConnectorButton";
import { ConnectorCell } from "./ConnectorCell";
import styles from "./ConnectorsView.module.scss";
import { ConnectorsViewContext } from "./ConnectorsViewContext";
import ImageCell from "./ImageCell";
import UpgradeAllButton from "./UpgradeAllButton";

export interface ConnectorsViewProps {
  type: "sources" | "destinations";
  usedConnectorsDefinitions: SourceDefinitionRead[] | DestinationDefinitionRead[];
  connectorsDefinitions: SourceDefinitionRead[] | DestinationDefinitionRead[];
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
  usedConnectorsDefinitions,
  connectorsDefinitions,
  connectorBuilderProjects,
}) => {
  const [updatingAllConnectors, setUpdatingAllConnectors] = useState(false);
  const workspace = useCurrentWorkspace();
  const canUpdateConnectors = useIntent("UpdateConnectorVersions", {
    organizationId: workspace.organizationId,
  });
  const canUpdateOrDeleteCustomConnectors = useGeneratedIntent(Intent.UpdateOrDeleteCustomConnector);
  const allowUpdateConnectors = useFeature(FeatureItem.AllowUpdateConnectors) && canUpdateConnectors;
  const allowUpdateDeleteCustomConnectors =
    useFeature(FeatureItem.AllowUploadCustomImage) && canUpdateOrDeleteCustomConnectors;

  const showVersionUpdateColumn = useCallback(
    (definitions: ConnectorDefinition[]) => {
      if (allowUpdateConnectors) {
        return true;
      }
      if (allowUpdateDeleteCustomConnectors && definitions.some((definition) => definition.custom)) {
        return true;
      }
      return false;
    },
    [allowUpdateConnectors, allowUpdateDeleteCustomConnectors]
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
              header: () => null,
              cell: (props) => {
                return allowUpdateConnectors || (canUpdateOrDeleteCustomConnectors && props.row.original.custom) ? (
                  <FlexContainer justifyContent="flex-end" gap="xs">
                    {isSourceDefinition(props.row.original) ? (
                      <EditSourceDefinitionButton
                        definitionId={Connector.id(props.row.original)}
                        isConnectorNameEditable={!!props.row.original.custom}
                      />
                    ) : (
                      <EditDestinationDefinitionButton
                        definitionId={Connector.id(props.row.original)}
                        isConnectorNameEditable={!!props.row.original.custom}
                      />
                    )}
                    {props.row.original.custom ? (
                      isSourceDefinition(props.row.original) ? (
                        <DeleteSourceDefinitionButton sourceDefinitionId={Connector.id(props.row.original)} />
                      ) : (
                        <DeleteDestinationDefinitionButton destinationDefinitionId={Connector.id(props.row.original)} />
                      )
                    ) : null}
                  </FlexContainer>
                ) : null;
              },
            }),
          ]
        : []),
    ],
    [allowUpdateConnectors, canUpdateOrDeleteCustomConnectors, type]
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
    }),
    [updatingAllConnectors]
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
        <MemoizedTable
          stickyHeaders={false}
          columns={usedDefinitionColumns}
          data={filteredUsedConnectorsDefinitions}
          sorting={false}
          className={styles.connectorsTable}
        />
      ),
    });
  }

  sections.push({
    title: type === "sources" ? "admin.availableSource" : "admin.availableDestinations",
    content: (
      <MemoizedTable
        stickyHeaders={false}
        columns={definitionColumns}
        data={filteredConnectorsDefinitions}
        sorting={false}
        className={styles.connectorsTable}
      />
    ),
  });

  return (
    <ConnectorsViewContext.Provider value={ctx}>
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
                  <AddNewConnectorButton type={type} />
                  {allowUpdateConnectors && <UpgradeAllButton connectorType={type} />}
                </FlexContainer>
              )}
            </FlexContainer>
            {section.content}
          </FlexContainer>
        ))}
      </FlexContainer>
    </ConnectorsViewContext.Provider>
  );
};

export default ConnectorsView;
