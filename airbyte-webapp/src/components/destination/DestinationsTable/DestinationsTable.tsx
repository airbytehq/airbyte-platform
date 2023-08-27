import React from "react";
import { useNavigate } from "react-router-dom";

import { ImplementationTable } from "components/EntityTable";
import { EntityTableDataItem } from "components/EntityTable/types";
import { getEntityTableData } from "components/EntityTable/utils";

import { useConnectionList } from "core/api";
import { DestinationRead } from "core/api/types/AirbyteClient";

interface DestinationsTableProps {
  destinations: DestinationRead[];
}

export const DestinationsTable: React.FC<DestinationsTableProps> = ({ destinations }) => {
  const navigate = useNavigate();
  const { connections } = useConnectionList({ destinationId: destinations.map(({ destinationId }) => destinationId) });

  const data = getEntityTableData(destinations, connections, "destination");

  const clickRow = (destination: EntityTableDataItem) => navigate(`${destination.entityId}`);

  return <ImplementationTable data={data} onClickRow={clickRow} entity="destination" />;
};
