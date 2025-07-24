import {
  ColumnDef,
  flexRender,
  useReactTable,
  VisibilityState,
  Row,
  getSortedRowModel,
  getCoreRowModel,
  ColumnFiltersState,
  getFilteredRowModel,
  SortingState,
  OnChangeFn,
} from "@tanstack/react-table";
import classNames from "classnames";
import React, { PropsWithChildren, useMemo, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { TableVirtuoso, TableComponents, ItemProps } from "react-virtuoso";

import { Text } from "components/ui/Text";

import { SortableTableHeader } from "./SortableTableHeader";
import styles from "./Table.module.scss";
import { ColumnMeta } from "./types";

// We can leave type any here since useReactTable options.columns itself is waiting for Array<ColumnDef<T, any>>
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type TableColumns<T> = Array<ColumnDef<T, any>>;

export interface TableProps<T> {
  className?: string;
  columns: TableColumns<T>;
  data: T[];
  rowId?: keyof T | ((row: T) => string);
  variant?: "default" | "light" | "white" | "inBlock";
  getRowCanExpand?: (data: Row<T>) => boolean;
  getIsRowExpanded?: (data: Row<T>) => boolean;
  expandedRow?: (props: { row: Row<T> }) => React.ReactElement;
  testId?: string;
  columnVisibility?: VisibilityState;
  columnFilters?: ColumnFiltersState;
  manualSorting?: boolean;
  onSortingChange?: OnChangeFn<SortingState>;
  sorting?: boolean;
  stickyHeaders?: boolean;
  getRowClassName?: (data: T) => string | undefined;
  initialSortBy?: Array<{ id: string; desc: boolean }>;
  showTableToggle?: boolean;
  initialExpandedRows?: number;
  /**
   * If true, the table will be rendered using react-virtuoso. Defaults to false.
   */
  virtualized?: boolean;
  /**
   * Props to be passed to react-virtuoso. See https://virtuoso.dev/ for more details.
   */
  virtualizedProps?: Omit<
    React.ComponentProps<typeof TableVirtuoso>,
    "data" | "components" | "totalCount" | "fixedHeaderContent"
  >;
  /**
   * Custom placeholder to be shown when the table is empty. Defaults to a simple "No data" message.
   */
  customEmptyPlaceholder?: React.ReactElement;
  sortingState?: SortingState;
  showEmptyPlaceholder?: boolean;
}

export const Table = <T,>({
  testId,
  className,
  columns,
  data,
  variant = "default",
  rowId,
  getRowCanExpand,
  getIsRowExpanded,
  expandedRow,
  columnVisibility,
  columnFilters,
  getRowClassName,
  stickyHeaders = true,
  manualSorting = false,
  onSortingChange,
  sorting = true,
  initialSortBy,
  virtualized = false,
  virtualizedProps,
  customEmptyPlaceholder,
  showTableToggle = false,
  initialExpandedRows = 6,
  sortingState,
  showEmptyPlaceholder = true,
}: PropsWithChildren<TableProps<T>>) => {
  const table = useReactTable({
    columns,
    data,
    initialState: {
      columnVisibility,
      sorting: initialSortBy,
    },
    manualSorting,
    // Setting onSortingChange to undefined will effectively disable sorting
    ...(onSortingChange !== undefined ? { onSortingChange } : {}),
    state: {
      columnFilters,
      // Setting sortingState to undefined will effectively disable sorting
      ...(sortingState !== undefined ? { sorting: sortingState } : {}),
    },
    getCoreRowModel: getCoreRowModel<T>(),
    getSortedRowModel: getSortedRowModel<T>(),
    getFilteredRowModel: getFilteredRowModel<T>(),
    getRowCanExpand,
    getIsRowExpanded,
    enableSorting: sorting,
    getRowId: rowId
      ? (originalRow) => {
          if (typeof rowId === "function") {
            return rowId(originalRow);
          }

          return String(originalRow[rowId]);
        }
      : undefined,
  });

  const rows = table.getRowModel().rows;

  const Table: TableComponents["Table"] = ({ style, ...props }) => (
    <table
      className={classNames(styles.table, className, {
        [styles["table--default"]]: variant === "default",
        [styles["table--empty"]]: rows.length === 0,
      })}
      {...props}
      style={style}
      data-testid={testId}
    />
  );

  const TableHead: TableComponents["TableHead"] = React.forwardRef(({ style, ...restProps }, ref) => (
    <thead
      ref={ref}
      className={classNames(styles.thead, { [styles["thead--sticky"]]: stickyHeaders })}
      {...restProps}
    />
  ));
  TableHead.displayName = "TableHead";

  const TableRow: React.FC<{ row: Row<T>; restRowProps?: ItemProps<T> }> = useMemo(
    () =>
      // eslint-disable-next-line react/function-component-definition -- using function as it provides the component's DisplayName
      function TableRow({ row, restRowProps }) {
        return (
          <>
            <tr
              className={classNames(styles.tr, getRowClassName?.(row.original))}
              data-testid={`table-row-${row.id}`}
              {...(virtualized && { ...restRowProps })}
            >
              {row.getVisibleCells().map((cell) => {
                const meta = cell.column.columnDef.meta as ColumnMeta | undefined;
                return (
                  <td
                    className={classNames(styles.td, meta?.tdClassName, {
                      [styles["td--responsive"]]: meta?.responsive,
                      [styles["td--noPadding"]]: meta?.noPadding,
                    })}
                    key={`table-cell-${row.id}-${cell.id}`}
                    data-testid={`table-cell-${row.id}-${cell.id}`}
                  >
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </td>
                );
              })}
            </tr>
            {row.getIsExpanded() && expandedRow ? (
              <tr>
                <td colSpan={row.getVisibleCells().length}>{expandedRow({ row })}</td>
              </tr>
            ) : null}
          </>
        );
      },
    [expandedRow, getRowClassName, virtualized]
  );

  // virtuoso uses "data-index" to identify the row, hence we need additional wrapper specifically for virtuoso
  const TableRowVirtualized: React.FC<ItemProps<T>> = (props) => {
    const index = props["data-index"];
    const row = rows[index];

    return <TableRow row={row} restRowProps={props} />;
  };

  const headerContent = () =>
    table.getHeaderGroups().map((headerGroup) => (
      <tr key={`table-header-${headerGroup.id}}`}>
        {headerGroup.headers.map((header) => {
          const meta = header.column.columnDef.meta as ColumnMeta | undefined;
          const isSorted = header.column.getIsSorted();

          const customSortToggle = () => {
            const currentSorting = table.getState().sorting;

            // if table is unsorted, or if we are sorting by another column, or if we are sorting by the same column but desc, toggle sorting to asc
            if (
              currentSorting === undefined ||
              currentSorting.some((sort) => sort.id === header.column.id && sort.desc === true) ||
              !currentSorting.some((sort) => sort.id === header.column.id)
            ) {
              // the first arg of toggleSorting() from react-table is desc: boolean
              header.column.toggleSorting(false);
            } else {
              header.column.toggleSorting(true);
            }
          };

          return (
            <th
              colSpan={header.colSpan}
              className={classNames(
                styles.th,
                {
                  [styles["th--default"]]: variant === "default",
                  [styles["th--light"]]: variant === "light",
                  [styles["th--white"]]: variant === "white",
                  [styles["th--inBlock"]]: variant === "inBlock",
                  [styles["th--sorted"]]: isSorted,
                },
                meta?.thClassName
              )}
              key={`table-column-${headerGroup.id}-${header.id}`}
            >
              {header.column.getCanSort() === true ? (
                <SortableTableHeader
                  onClick={() => customSortToggle()}
                  isActive={isSorted !== false}
                  isAscending={isSorted === "asc"}
                >
                  {flexRender(header.column.columnDef.header, header.getContext())}
                </SortableTableHeader>
              ) : (
                flexRender(header.column.columnDef.header, header.getContext())
              )}
            </th>
          );
        })}
      </tr>
    ));

  const EmptyPlaceholder: TableComponents["EmptyPlaceholder"] = () => (
    <tbody>
      <tr className={classNames(styles.tr, styles.emptyPlaceholder)}>
        <td className={styles.td} colSpan={columns.length} style={{ textAlign: "center" }}>
          <Text bold color="grey" align="center">
            {customEmptyPlaceholder ? customEmptyPlaceholder : <FormattedMessage id="tables.empty" />}
          </Text>
        </td>
      </tr>
    </tbody>
  );

  const [isExpanded, setIsExpanded] = useState(false);
  const { formatMessage } = useIntl();

  return virtualized ? (
    <TableVirtuoso<T>
      // the parent container should have exact height to make "AutoSizer" work properly
      style={{
        height: "100%",
        minHeight: showEmptyPlaceholder ? 100 : undefined, // for empty state placeholder
      }}
      totalCount={rows.length}
      {...virtualizedProps}
      components={{
        Table,
        TableHead,
        TableRow: TableRowVirtualized,
        EmptyPlaceholder: showEmptyPlaceholder ? EmptyPlaceholder : undefined,
      }}
      fixedHeaderContent={headerContent}
    />
  ) : (
    <table
      className={classNames(styles.table, className, {
        [styles["table--default"]]: variant === "default",
      })}
      data-testid={testId}
    >
      <thead className={classNames(styles.thead, { [styles["thead--sticky"]]: stickyHeaders })}>
        {headerContent()}
      </thead>
      {rows.length === 0 ? (
        showEmptyPlaceholder ? (
          <EmptyPlaceholder />
        ) : null
      ) : (
        <tbody className={styles.tbody}>
          {!showTableToggle || isExpanded
            ? rows.map((row) => <TableRow key={`table-row-${row.id}`} row={row} />)
            : rows.slice(0, initialExpandedRows).map((row) => <TableRow key={`table-row-${row.id}`} row={row} />)}
        </tbody>
      )}
      {showTableToggle && rows.length > initialExpandedRows && (
        <button
          type="button"
          className={styles.viewMoreButton}
          onClick={() => {
            setIsExpanded((prev) => !prev);
          }}
        >
          {formatMessage({ id: isExpanded ? "tables.viewLess" : "tables.viewMore" })}
        </button>
      )}
    </table>
  );
};
