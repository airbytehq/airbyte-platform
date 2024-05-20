import React from "react";
import { useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";

import { SyncCatalogTable } from "./SyncCatalogTable/SyncCatalogTable";

export const SyncCatalogCardNext: React.FC = () => {
  const { formatMessage } = useIntl();

  return (
    <Card noPadding title={formatMessage({ id: "connection.schema" })}>
      <Box mb="xl" data-testid="catalog-tree-table-body">
        <SyncCatalogTable />
      </Box>
    </Card>
  );
};
