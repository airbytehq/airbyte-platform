import { ColumnFiltersState, ExpandedState, flexRender, HeaderGroup, Row, TableState } from "@tanstack/react-table";
import classnames from "classnames";
import React, { FC, useContext } from "react";
import { FormattedMessage } from "react-intl";
import { ItemProps, TableComponents, TableVirtuoso, FlatIndexLocationWithAlign } from "react-virtuoso";

import { ScrollParentContext } from "components/ui/ScrollParent";
import { ColumnMeta } from "components/ui/Table/types";
import { Text } from "components/ui/Text";

import { useFormMode } from "core/services/ui/FormModeContext";

import { useNamespaceRowInView } from "../hooks/useNamespaceRowInView";
import { SyncCatalogUIModel } from "../SyncCatalogTable";
import styles from "../SyncCatalogTable.module.scss";
import { generateTestId, getRowChangeStatus, isNamespaceRow } from "../utils";

export interface SyncCatalogVirtuosoTableProps {
  rows: Array<Row<SyncCatalogUIModel>>;
  getHeaderGroups: () => Array<HeaderGroup<SyncCatalogUIModel>>;
  getState: () => TableState;
  initialTopMostItemIndex: FlatIndexLocationWithAlign | undefined;
  stickyRowIndex: number;
  setStickyRowIndex: (index: number) => void;
  columnFilters: ColumnFiltersState;
  stickyIndexes: number[];
  expanded: ExpandedState;
}

interface SyncCatalogVirtuosoTableContext {
  columnCount: number;
  filteringValue: string;
}

export const SyncCatalogVirtuosoTable: FC<SyncCatalogVirtuosoTableProps> = ({
  rows,
  getHeaderGroups,
  getState,
  initialTopMostItemIndex,
  stickyRowIndex,
  setStickyRowIndex,
  stickyIndexes,
}) => {
  const { mode } = useFormMode();
  const customScrollParent = useContext(ScrollParentContext);

  const Table: TableComponents["Table"] = ({ style, ...props }) => (
    <table className={classnames(styles.table)} {...props} style={style} data-testid="sync-catalog-table" />
  );

  const TableHead: TableComponents["TableHead"] = React.forwardRef(({ style, ...restProps }, ref) => (
    <thead ref={ref} className={classnames(styles.stickyTableHeader)} {...restProps} />
  ));
  TableHead.displayName = "TableHead";

  const headerContent = () =>
    getHeaderGroups().map((headerGroup) => (
      <tr key={headerGroup.id} className={classnames(styles.tr)}>
        {headerGroup.headers.map((header) => {
          const meta = header.column.columnDef.meta as ColumnMeta | undefined;
          return (
            <th key={header.id} colSpan={header.colSpan} className={classnames(styles.th, meta?.thClassName)}>
              {flexRender(header.column.columnDef.header, header.getContext())}
            </th>
          );
        })}
      </tr>
    ));

  const TableRow: React.FC<ItemProps<SyncCatalogUIModel>> = (props) => {
    const index = props["data-index"];
    const row = rows[index];
    const { rowChangeStatus } = getRowChangeStatus(row);
    const { ref } = useNamespaceRowInView(index, stickyRowIndex, stickyIndexes, setStickyRowIndex, customScrollParent);

    const rowStatusStyle = classnames(styles.tr, {
      [styles.added]: rowChangeStatus === "added" && mode !== "create",
      [styles.removed]: rowChangeStatus === "removed" && mode !== "create",
      [styles.changed]: rowChangeStatus === "changed" && mode !== "create",
      [styles.disabled]: rowChangeStatus === "disabled",
      [styles.highlighted]: initialTopMostItemIndex?.index === index,
    });

    return (
      <tr
        ref={ref}
        key={`${row.id}-${row.depth}`}
        className={rowStatusStyle}
        {...props}
        // the first row is the namespace row, we need to hide it since header has the same content
        style={index === 0 && isNamespaceRow(row) ? { display: "none" } : undefined}
        data-testid={generateTestId(row)}
      >
        {row.getVisibleCells().map((cell) => {
          const meta = cell.column.columnDef.meta as ColumnMeta | undefined;
          return (
            <td
              className={classnames(styles.td, meta?.tdClassName, {
                [styles.th]: isNamespaceRow(row),
              })}
              key={cell.id}
            >
              {flexRender(cell.column.columnDef.cell, cell.getContext())}
            </td>
          );
        })}
      </tr>
    );
  };

  const EmptyPlaceholder: TableComponents["EmptyPlaceholder"] = ({ context }) => {
    const { columnCount, filteringValue } = context as SyncCatalogVirtuosoTableContext;

    return (
      <tbody>
        <tr className={classnames(styles.tr, styles.emptyPlaceholder)}>
          <td className={styles.td} colSpan={columnCount} style={{ textAlign: "center" }}>
            <Text bold color="grey" align="center">
              <FormattedMessage
                id={filteringValue ? "connection.catalogTree.noMatchingStreams" : "connection.catalogTree.noStreams"}
              />
            </Text>
          </td>
        </tr>
      </tbody>
    );
  };

  return (
    <TableVirtuoso<SyncCatalogUIModel>
      totalCount={rows.length}
      style={{ minHeight: 120 }} // header namespace row height + 2 stream rows height
      initialTopMostItemIndex={initialTopMostItemIndex}
      components={{
        Table,
        TableHead,
        TableRow,
        EmptyPlaceholder,
      }}
      context={
        {
          columnCount: getHeaderGroups()[0]?.headers.length || 0,
          filteringValue: getState().globalFilter,
        } satisfies SyncCatalogVirtuosoTableContext
      }
      fixedHeaderContent={headerContent}
      fixedItemHeight={40}
      atTopStateChange={(atTop) => {
        if (atTop && stickyRowIndex !== 0) {
          setStickyRowIndex(0);
        }
      }}
      increaseViewportBy={50}
      useWindowScroll
      customScrollParent={customScrollParent ?? undefined}
    />
  );
};
