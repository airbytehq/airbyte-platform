import React from "react";

import { ImplementationTable } from "components/EntityTable";
import { getEntityTableData } from "components/EntityTable/utils";

import { useConnectionList } from "core/api";
import { SourceRead } from "core/api/types/AirbyteClient";

interface SourcesTableProps {
  sources: SourceRead[];
}

export const SourcesTable: React.FC<SourcesTableProps> = ({ sources }) => {
  const connectionList = useConnectionList({ sourceId: sources.map(({ sourceId }) => sourceId) });
  const connections = connectionList?.connections ?? [];

  const data = getEntityTableData(sources, connections, "source");

  return <ImplementationTable data={data} entity="source" />;
};
