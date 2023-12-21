import React from "react";

import { ImplementationTable } from "components/EntityTable";
import { getEntityTableData } from "components/EntityTable/utils";

import { useConnectionList } from "core/api";
import { DestinationRead } from "core/api/types/AirbyteClient";

interface DestinationsTableProps {
  destinations: DestinationRead[];
}

export const DestinationsTable: React.FC<DestinationsTableProps> = ({ destinations }) => {
  const connectionList = useConnectionList({ destinationId: destinations.map(({ destinationId }) => destinationId) });
  const connections = connectionList?.connections ?? [];

  const data = getEntityTableData(destinations, connections, "destination");

  return <ImplementationTable data={data} entity="destination" />;
};
