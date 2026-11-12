import React from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Table } from "components/ui/Table";
import { Text } from "components/ui/Text";

import { columns } from "./columns";
import { RowError } from "./RowError";
import styles from "./SecretStorageDetails.module.scss";
import { SecretStorageRow } from "./types";

const isRowError = ({ original: dataItem }: { original: SecretStorageRow }): boolean => dataItem.error !== undefined;

// The table builds its row component from this function, so it must be the same function on every render.
// A new one each time would remount every row, and with it everything the cells fetch.
const renderRowError = ({ row: { original: dataItem } }: { row: { original: SecretStorageRow } }) => (
  <RowError error={dataItem.error} />
);

interface SecretStorageDetailsProps {
  rows: SecretStorageRow[];
  checkIntervalMinutes: number;
}

export const SecretStorageDetails: React.FC<SecretStorageDetailsProps> = ({ rows, checkIntervalMinutes }) => (
  <FlexContainer direction="column" gap="sm">
    <Text size="sm" bold>
      <FormattedMessage
        id="settings.organization.regions.dataplane.diagnostics.secretStorage.details.title"
        values={{ minutes: checkIntervalMinutes }}
      />
    </Text>

    <Table
      columns={columns}
      data={rows}
      rowId="id"
      variant="inBlock"
      className={styles.table}
      sorting={false}
      getRowCanExpand={isRowError}
      getIsRowExpanded={isRowError}
      expandedRow={renderRowError}
    />
  </FlexContainer>
);
