import { useMemo } from "react";
import { useNavigate } from "react-router-dom";

import { ConnectorIcon } from "components/common/ConnectorIcon";
import { ConnectorEmptyStateContent } from "components/connector/ConnectorEmptyStateContent";
import { TableItemTitle } from "components/ConnectorBlocks";
import { DestinationConnectionTable } from "components/destination/DestinationConnectionTable";
import { DropdownMenuOptionType } from "components/ui/DropdownMenu";
import { FlexContainer } from "components/ui/Flex";

import { useConnectionList } from "hooks/services/useConnectionHook";
import { useSourceList } from "hooks/services/useSourceHook";
import { RoutePaths } from "pages/routePaths";

import { useGetDestinationFromParams } from "./useGetDestinationFromParams";

export const DestinationOverviewPage = () => {
  const navigate = useNavigate();

  const destination = useGetDestinationFromParams();

  // We load only connections attached to this destination to be shown in the connections grid
  const { connections } = useConnectionList({ destinationId: [destination.destinationId] });

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

  const onSelect = (data?: DropdownMenuOptionType) => {
    const path = `../../../${RoutePaths.Connections}/${RoutePaths.ConnectionNew}`;
    const state =
      data && data.value !== "create-new-item"
        ? {
            sourceId: data.value,
            destinationId: destination.destinationId,
          }
        : { destinationId: destination.destinationId };
    navigate(path, { state });
  };

  return (
    <>
      {connections.length ? (
        <FlexContainer direction="column" gap="xl">
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
          onButtonClick={() => onSelect()}
          icon={destination.icon}
          connectorType="destination"
          connectorName={destination.name}
        />
      )}
    </>
  );
};

export default DestinationOverviewPage;
