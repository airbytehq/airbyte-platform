import { ColumnDef, flexRender, useReactTable, getCoreRowModel, VisibilityState, Row } from "@tanstack/react-table";
import classNames from "classnames";
import { Fragment, PropsWithChildren } from "react";

import styles from "./Table.module.scss";
import { ColumnMeta } from "./types";

// We can leave type any here since useReactTable options.columns itself is waiting for Array<ColumnDef<T, any>>
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type TableColumns<T> = Array<ColumnDef<T, any>>;

export interface TableProps<T> {
  className?: string;
  columns: TableColumns<T>;
  /**
   * If the table data is sorted outside this component you can pass the id of the column by which its sorted
   * to apply the correct sorting style to that column.
   */
  sortedByColumn?: string;
  data: T[];
  variant?: "default" | "light" | "white";
  onClickRow?: (data: T) => void;
  getRowCanExpand?: (data: Row<T>) => boolean;
  getIsRowExpanded?: (data: Row<T>) => boolean;
  expandedRow?: (props: { row: Row<T> }) => React.ReactElement;
  testId?: string;
  columnVisibility?: VisibilityState;
  getRowClassName?: (data: T) => string | undefined;
}

export const Table = <T,>({
  testId,
  className,
  columns,
  data,
  variant = "default",
  onClickRow,
  getRowCanExpand,
  getIsRowExpanded,
  expandedRow,
  columnVisibility,
  sortedByColumn,
  getRowClassName,
}: PropsWithChildren<TableProps<T>>) => {
  const table = useReactTable({
    columns,
    data,
    initialState: {
      columnVisibility,
    },
    getCoreRowModel: getCoreRowModel<T>(),
    getRowCanExpand,
    getIsRowExpanded,
  });

  return (
    <table
      className={classNames(styles.table, className, {
        [styles["table--default"]]: variant === "default",
      })}
      data-testid={testId}
    >
      <thead className={styles.thead}>
        {table.getHeaderGroups().map((headerGroup) => (
          <tr key={`table-header-${headerGroup.id}}`}>
            {headerGroup.headers.map((header) => {
              const meta = header.column.columnDef.meta as ColumnMeta | undefined;
              const isSorted = (sortedByColumn && sortedByColumn === header.column.id) || header.column.getIsSorted();
              return (
                <th
                  colSpan={header.colSpan}
                  className={classNames(
                    styles.th,
                    {
                      [styles["th--default"]]: variant === "default",
                      [styles["th--light"]]: variant === "light",
                      [styles["th--white"]]: variant === "white",
                      [styles["th--sorted"]]: isSorted,
                    },
                    meta?.thClassName
                  )}
                  key={`table-column-${headerGroup.id}-${header.id}`}
                >
                  {flexRender(header.column.columnDef.header, header.getContext())}
                </th>
              );
            })}
          </tr>
        ))}
      </thead>
      <tbody>
        {table.getRowModel().rows.map((row) => {
          return (
            <Fragment key={`table-row-${row.id}`}>
              <tr
                className={classNames(
                  styles.tr,
                  {
                    [styles["tr--clickable"]]: !!onClickRow,
                  },
                  getRowClassName?.(row.original)
                )}
                data-testid={`table-row-${row.id}`}
                onClick={() => onClickRow?.(row.original)}
              >
                {row.getVisibleCells().map((cell) => {
                  const meta = cell.column.columnDef.meta as ColumnMeta | undefined;
                  return (
                    <td
                      className={classNames(styles.td, meta?.tdClassName, {
                        [styles["td--responsive"]]: meta?.responsive,
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
            </Fragment>
          );
        })}
      </tbody>
    </table>
  );
};
