import React from "react";
import { useNavigate } from "react-router-dom";

import { ImplementationTable } from "components/EntityTable";
import { EntityTableDataItem } from "components/EntityTable/types";
import { getEntityTableData } from "components/EntityTable/utils";

import { useConnectionList } from "core/api";
import { SourceRead } from "core/request/AirbyteClient";

interface SourcesTableProps {
  sources: SourceRead[];
}

export const SourcesTable: React.FC<SourcesTableProps> = ({ sources }) => {
  const navigate = useNavigate();

  const connectionList = useConnectionList({ sourceId: sources.map(({ sourceId }) => sourceId) });
  const connections = connectionList?.connections ?? [];

  const data = getEntityTableData(sources, connections, "source");

  const clickRow = (source: EntityTableDataItem) => navigate(`${source.entityId}`);

  return <ImplementationTable data={data} onClickRow={clickRow} entity="source" />;
};
