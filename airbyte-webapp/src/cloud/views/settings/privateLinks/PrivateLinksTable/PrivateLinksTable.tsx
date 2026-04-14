import { createColumnHelper } from "@tanstack/react-table";
import React, { useMemo } from "react";
import { FormattedMessage } from "react-intl";

import { Table } from "components/ui/Table";
import { Text } from "components/ui/Text";

import { PrivateLinkRead } from "core/api/types/AirbyteClient";

import { ActionsCell } from "./ActionsCell";
import { DnsNameCell } from "./DnsNameCell";
import styles from "./PrivateLinksTable.module.scss";
import { StatusCell } from "./StatusCell";

const columnHelper = createColumnHelper<PrivateLinkRead>();

interface PrivateLinksTableProps {
  privateLinks: PrivateLinkRead[];
  onDelete: (link: PrivateLinkRead) => void;
  onViewDetails: (link: PrivateLinkRead) => void;
}

export const PrivateLinksTable: React.FC<PrivateLinksTableProps> = ({ privateLinks, onDelete, onViewDetails }) => {
  const columns = useMemo(
    () => [
      columnHelper.accessor("name", {
        header: () => <FormattedMessage id="settings.privateLinks.table.name" />,
        cell: (props) => <Text size="sm">{props.getValue()}</Text>,
      }),
      columnHelper.accessor("status", {
        header: () => <FormattedMessage id="settings.privateLinks.table.status" />,
        cell: (props) => <StatusCell status={props.getValue()} />,
      }),
      columnHelper.accessor("dnsName", {
        header: () => <FormattedMessage id="settings.privateLinks.table.dnsName" />,
        cell: (props) => <DnsNameCell dnsName={props.getValue()} status={props.row.original.status} />,
      }),
      columnHelper.display({
        id: "actions",
        header: () => "",
        size: 50,
        cell: (props) => <ActionsCell link={props.row.original} onDelete={onDelete} onViewDetails={onViewDetails} />,
      }),
    ],
    [onDelete, onViewDetails]
  );

  return (
    <div className={styles.tableWrapper}>
      <Table columns={columns} data={privateLinks} initialSortBy={[{ id: "name", desc: false }]} />
    </div>
  );
};
