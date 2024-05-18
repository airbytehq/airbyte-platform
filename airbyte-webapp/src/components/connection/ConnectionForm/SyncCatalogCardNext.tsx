import React from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { Heading } from "components/ui/Heading";

import { SyncCatalogTable } from "./SyncCatalogTable/SyncCatalogTable";

export const SyncCatalogCardNext: React.FC = () => {
  return (
    <Card noPadding>
      <Box m="xl">
        <Heading as="h2" size="sm">
          <FormattedMessage id="connection.schema" />
        </Heading>
      </Box>
      <Box mb="xl" data-testid="catalog-tree-table-body">
        <SyncCatalogTable />
      </Box>
    </Card>
  );
};
