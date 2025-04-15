import { useMemo } from "react";
import { createSearchParams, useNavigate } from "react-router-dom";

import { ConnectorEmptyStateContent } from "components/connector/ConnectorEmptyStateContent";
import { TableItemTitle } from "components/ConnectorBlocks";
import { ConnectorIcon } from "components/ConnectorIcon";
import { DestinationConnectionTable } from "components/destination/DestinationConnectionTable";
import { DropdownMenuOptionType } from "components/ui/DropdownMenu";
import { FlexContainer } from "components/ui/Flex";

import { useGetDestinationFromParams } from "area/connector/utils";
import { useCurrentWorkspace, useConnectionList, useSourceList, useListConnectionsStatusesAsync } from "core/api";
import { WebBackendConnectionListItem } from "core/api/types/AirbyteClient";
import { useExperiment } from "hooks/services/Experiment";
import { ConnectionRoutePaths, RoutePaths } from "pages/routePaths";

import styles from "./DestinationConnectionsPage.module.scss";

const emptyArray: never[] = [];
export const DestinationConnectionsPage = () => {
  const navigate = useNavigate();
  const { workspaceId } = useCurrentWorkspace();

  const destination = useGetDestinationFromParams();

  // We load only connections attached to this destination to be shown in the connections grid
  const connectionList = useConnectionList({ destinationId: [destination.destinationId] });
  const connections = connectionList?.connections ?? (emptyArray as WebBackendConnectionListItem[]);

  const isAllConnectionsStatusEnabled = useExperiment("connections.connectionsStatusesEnabled");
  const connectionIds = useMemo(() => connections.map((connection) => connection.connectionId), [connections]);
  useListConnectionsStatusesAsync(connectionIds, isAllConnectionsStatusEnabled);

  // We load all sources so the add source button has a pre-filled list of options.
  const { sources } = useSourceList();
  const sourceDropdownOptions = useMemo<DropdownMenuOptionType[]>(
    () =>
      sources.map((source) => {
        return {
          as: "button",
          icon: <ConnectorIcon icon={source.icon} />,
          iconPosition: "right",
          displayName: source.name,
          value: source.sourceId,
        };
      }),
    [sources]
  );

  const onSelect = (data: DropdownMenuOptionType) => {
    const path = `/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Connections}/${ConnectionRoutePaths.ConnectionNew}`;

    const searchParams =
      data.value !== "create-new-item"
        ? createSearchParams({ sourceId: data.value as string, destinationId: destination.destinationId })
        : createSearchParams({ destinationId: destination.destinationId, sourceType: "new" });

    navigate({ pathname: path, search: `?${searchParams}` });
  };

  return (
    <>
      {connections.length ? (
        <FlexContainer direction="column" gap="xl" className={styles.fullHeight}>
          <TableItemTitle
            type="source"
            dropdownOptions={sourceDropdownOptions}
            onSelect={onSelect}
            connectionsCount={connections.length}
          />
          <DestinationConnectionTable connections={connections} />
        </FlexContainer>
      ) : (
        <ConnectorEmptyStateContent
          connectorId={destination.destinationId}
          icon={destination.icon}
          connectorType="destination"
          connectorName={destination.name}
        />
      )}
    </>
  );
};
