import React from "react";

import { ConnectionTable } from "components/EntityTable";
import { getConnectionTableData } from "components/EntityTable/utils";
import { Box } from "components/ui/Box";
import { ScrollParent } from "components/ui/ScrollParent";

import { WebBackendConnectionListItem } from "core/api/types/AirbyteClient";

interface IProps {
  connections: WebBackendConnectionListItem[];
}

export const DestinationConnectionTable: React.FC<IProps> = ({ connections }) => {
  const data = getConnectionTableData(connections, "destination");

  return (
    <ScrollParent>
      <Box m="xl" mt="none">
        <ConnectionTable data={data} entity="destination" />
      </Box>
    </ScrollParent>
  );
};
