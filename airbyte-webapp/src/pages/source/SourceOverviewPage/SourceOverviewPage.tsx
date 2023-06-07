import React, { useMemo } from "react";
import { useNavigate } from "react-router-dom";

import { ConnectorIcon } from "components/common/ConnectorIcon";
import { ConnectorEmptyStateContent } from "components/connector/ConnectorEmptyStateContent";
import { TableItemTitle } from "components/ConnectorBlocks";
import { DropdownMenuOptionType } from "components/ui/DropdownMenu";
import { FlexContainer } from "components/ui/Flex/FlexContainer";

import { useConnectionList } from "hooks/services/useConnectionHook";
import { useDestinationList } from "hooks/services/useDestinationHook";
import { RoutePaths } from "pages/routePaths";

import { useGetSourceFromParams } from "./useGetSourceFromParams";

const SourceConnectionTable = React.lazy(() => import("./SourceConnectionTable"));

export const SourceOverviewPage = () => {
  const source = useGetSourceFromParams();

  const { connections } = useConnectionList({ sourceId: [source.sourceId] });

  // We load all destinations so the add destination button has a pre-filled list of options.
  const { destinations } = useDestinationList();

  const navigate = useNavigate();

  const destinationDropdownOptions: DropdownMenuOptionType[] = useMemo(
    () =>
      destinations.map((destination) => {
        return {
          as: "button",
          icon: <ConnectorIcon icon={destination.icon} />,
          iconPosition: "right",
          displayName: destination.name,
          value: destination.destinationId,
        };
      }),
    [destinations]
  );

  const onSelect = (data?: DropdownMenuOptionType) => {
    const path = `../../../${RoutePaths.Connections}/${RoutePaths.ConnectionNew}`;
    const state =
      data && data.value !== "create-new-item"
        ? {
            destinationId: data.value,
            sourceId: source.sourceId,
          }
        : { sourceId: source.sourceId };

    navigate(path, { state });
  };

  return (
    <>
      {connections.length ? (
        <FlexContainer direction="column" gap="xl">
          <TableItemTitle
            type="destination"
            dropdownOptions={destinationDropdownOptions}
            onSelect={onSelect}
            connectionsCount={connections ? connections.length : 0}
          />
          <SourceConnectionTable connections={connections} />{" "}
        </FlexContainer>
      ) : (
        <ConnectorEmptyStateContent
          onButtonClick={() => onSelect()}
          icon={source.icon}
          connectorType="source"
          connectorName={source.name}
        />
      )}
    </>
  );
};
