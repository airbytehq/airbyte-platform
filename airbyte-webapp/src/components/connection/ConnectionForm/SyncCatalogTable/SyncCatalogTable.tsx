import {
  ColumnFiltersState,
  createColumnHelper,
  ExpandedState,
  flexRender,
  getCoreRowModel,
  getExpandedRowModel,
  getFilteredRowModel,
  getGroupedRowModel,
  getSortedRowModel,
  useReactTable,
} from "@tanstack/react-table";
import classnames from "classnames";
import set from "lodash/set";
import React, { FC, useCallback, useEffect, useMemo, useState, useDeferredValue } from "react";
import { useFieldArray, useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import { ItemProps, TableComponents, TableVirtuoso } from "react-virtuoso";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { SearchInput } from "components/ui/SearchInput";
import { ColumnMeta } from "components/ui/Table/types";
import { Text } from "components/ui/Text";

import { AirbyteStreamAndConfiguration, AirbyteStreamConfiguration } from "core/api/types/AirbyteClient";
import { SyncSchemaField } from "core/domain/catalog";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";

import { CursorCell } from "./components/CursorCell";
import { FieldCursorCell } from "./components/FieldCursorCell";
import { FieldPKCell } from "./components/FieldPKCell";
import { FormControls } from "./components/FormControls";
import { PKCell } from "./components/PKCell";
import { SelectedFieldsCell } from "./components/SelectedFieldsCell";
import { StreamFieldNameCell } from "./components/StreamFieldCell";
import { StreamNameCell } from "./components/StreamNameCell";
import { FilterTabId, StreamsFilterTabs } from "./components/StreamsFilterTabs";
import { SyncModeCell } from "./components/SyncModeCell";
import { TableControls } from "./components/TableControls";
import { useInitialRowIndex } from "./hooks/useInitialRowIndex";
import styles from "./SyncCatalogTable.module.scss";
import { getRowChangeStatus, getStreamFieldRows, getStreamRows, isStreamRow } from "./utils";
import { FormConnectionFormValues, SyncStreamFieldWithId } from "../formConfig";

export interface SyncCatalogUIModel {
  /**
   * react-form syncCatalog streamNode field
   */
  streamNode: SyncStreamFieldWithId;
  /**
   * initial react-form syncCatalog streamNode field
   * used for comparing with the current streamNode field
   */
  initialStreamNode?: AirbyteStreamAndConfiguration;
  /**
   * Stream and Field share the same "name" prop for filtering and sorting
   */
  name: string;
  /**
   * Destination namespace
   */
  namespace?: string;
  /**
   * used for filtering enabled/disabled streams
   */
  isEnabled?: boolean;
  /**
   * syncMode, primaryKey and cursorField exist just for column mapping
   */
  syncMode?: string;
  primaryKey?: string;
  cursorField?: string;
  /**
   * used for updating stream fields
   */
  traversedFields?: SyncSchemaField[];
  /**
   * react-table subRows should have the same type as parentRow
   * flattenedFields transforms to subRows, each subRow has access to streamNode and field
   */
  flattenedFields?: SyncSchemaField[];
  field?: SyncSchemaField;
}

export const SyncCatalogTable: FC = () => {
  const { formatMessage } = useIntl();
  const { mode, initialValues } = useConnectionFormService();
  const { control, trigger } = useFormContext<FormConnectionFormValues>();
  const { fields: streams, update } = useFieldArray({
    name: "syncCatalog.streams",
    control,
  });
  const prefix = useWatch<FormConnectionFormValues>({ name: "prefix", control });

  const debugTable = false;
  const [expanded, setExpanded] = React.useState<ExpandedState>({});
  const [columnFilters, setColumnFilters] = useState<ColumnFiltersState>([]);
  const [filtering, setFiltering] = useState("");
  const deferredFilteringValue = useDeferredValue(filtering);

  // Update stream
  const onUpdateStreamConfigWithStreamNode = useCallback(
    (streamNode: SyncStreamFieldWithId, updatedConfig: Partial<AirbyteStreamConfiguration>) => {
      const updatedStreamNode = set(streamNode, "config", {
        ...streamNode.config,
        ...updatedConfig,
      });

      const streamNodeIndex = streams.findIndex((s) => s.id === streamNode.id);
      update(streamNodeIndex, updatedStreamNode);

      // force validation of the form
      trigger(`syncCatalog.streams`);
    },
    [streams, trigger, update]
  );

  // Columns
  const columnHelper = createColumnHelper<SyncCatalogUIModel>();
  const columns = [
    columnHelper.accessor("isEnabled", {
      id: "stream.selected",
      header: () => {},
      filterFn: (row, _, filterValue) => row.original.streamNode.config?.selected === filterValue,
      enableGlobalFilter: false, // since it's boolean value we don't need to filter it globally
    }),
    columnHelper.accessor("name", {
      id: "stream.name",
      header: () => {},
      cell: ({ row, getValue }) =>
        isStreamRow(row) ? (
          <StreamNameCell
            value={getValue()}
            row={row}
            updateStreamField={onUpdateStreamConfigWithStreamNode}
            globalFilterValue={filtering}
          />
        ) : (
          <StreamFieldNameCell
            row={row}
            updateStreamField={onUpdateStreamConfigWithStreamNode}
            globalFilterValue={filtering}
          />
        ),
      meta: {
        thClassName: styles.streamOrFieldNameCell,
        tdClassName: styles.streamOrFieldNameCell,
      },
    }),
    columnHelper.accessor("syncMode", {
      header: () => (
        <Text size="sm" color="grey500">
          <FormattedMessage id="form.syncMode" />
        </Text>
      ),
      cell: ({ row }) =>
        isStreamRow(row) ? <SyncModeCell row={row} updateStreamField={onUpdateStreamConfigWithStreamNode} /> : null,
      meta: {
        thClassName: styles.syncModeCell,
        tdClassName: styles.syncModeCell,
      },
    }),
    columnHelper.accessor("primaryKey", {
      header: () => {},
      cell: ({ row }) =>
        isStreamRow(row) ? (
          <PKCell row={row} />
        ) : (
          <FieldPKCell row={row} updateStreamField={onUpdateStreamConfigWithStreamNode} />
        ),
      meta: {
        thClassName: styles.pkCell,
        tdClassName: styles.pkCell,
      },
    }),
    columnHelper.accessor("cursorField", {
      header: () => {},
      cell: ({ row }) =>
        isStreamRow(row) ? (
          <CursorCell row={row} />
        ) : (
          <FieldCursorCell row={row} updateStreamField={onUpdateStreamConfigWithStreamNode} />
        ),
      meta: {
        thClassName: styles.cursorCell,
        tdClassName: styles.cursorCell,
      },
    }),
    columnHelper.accessor("flattenedFields", {
      header: () => (
        <Text size="sm" color="grey500">
          <FormattedMessage id="form.fields" />
        </Text>
      ),
      cell: ({ row }) => (isStreamRow(row) ? <SelectedFieldsCell row={row} /> : null),
      meta: {
        thClassName: styles.amountOfFieldsCell,
        tdClassName: styles.amountOfFieldsCell,
      },
    }),
  ];

  const preparedData = useMemo(
    () => getStreamRows(streams, initialValues.syncCatalog.streams, prefix),
    [initialValues.syncCatalog.streams, prefix, streams]
  );

  const { getHeaderGroups, getRowModel, getState, toggleAllRowsExpanded, getIsAllRowsExpanded } =
    useReactTable<SyncCatalogUIModel>({
      columns,
      data: preparedData,
      getSubRows: getStreamFieldRows,
      state: {
        expanded,
        globalFilter: deferredFilteringValue,
        columnFilters,
        columnVisibility: {
          "stream.selected": false,
        },
      },
      initialState: {
        sorting: [{ id: "stream.name", desc: false }],
      },
      getExpandedRowModel: getExpandedRowModel(),
      getGroupedRowModel: getGroupedRowModel(),
      getCoreRowModel: getCoreRowModel(),
      getSortedRowModel: getSortedRowModel(),
      getFilteredRowModel: getFilteredRowModel(),
      autoResetExpanded: false,
      filterFromLeafRows: true,
      enableGlobalFilter: true,
      onExpandedChange: setExpanded,
      onColumnFiltersChange: setColumnFilters,
      onGlobalFilterChange: setFiltering,
      getRowCanExpand: (row) => isStreamRow(row),
      debugTable,
    });

  useEffect(() => {
    // collapse all rows if global filter is empty and all rows are expanded
    if (!filtering && getIsAllRowsExpanded()) {
      toggleAllRowsExpanded(!getIsAllRowsExpanded());
      return;
    }

    // if global filter is empty or all rows already expanded then return
    if (!filtering || (filtering && getIsAllRowsExpanded())) {
      return;
    }

    toggleAllRowsExpanded();
  }, [filtering, getIsAllRowsExpanded, toggleAllRowsExpanded]);

  const rows = getRowModel().rows;
  const initialTopMostItemIndex = useInitialRowIndex(rows);

  const Table: TableComponents["Table"] = ({ style, ...props }) => (
    <table className={classnames(styles.table)} {...props} style={style} />
  );

  const TableHead: TableComponents["TableHead"] = React.forwardRef((props, ref) => (
    <thead ref={ref} className={classnames(styles.thead)} {...props} />
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

    const rowStatusStyle = classnames(styles.tr, {
      [styles.added]: rowChangeStatus === "added",
      [styles.removed]: rowChangeStatus === "removed",
      [styles.changed]: rowChangeStatus === "changed",
      [styles.disabled]: rowChangeStatus === "disabled",
      [styles.highlighted]: initialTopMostItemIndex?.index === index,
    });

    return (
      <tr key={`${row.id}-${row.depth}`} className={rowStatusStyle} {...props}>
        {row.getVisibleCells().map((cell) => {
          const meta = cell.column.columnDef.meta as ColumnMeta | undefined;
          return (
            <td className={classnames(styles.td, meta?.tdClassName)} key={cell.id}>
              {flexRender(cell.column.columnDef.cell, cell.getContext())}
            </td>
          );
        })}
      </tr>
    );
  };

  const EmptyPlaceholder: TableComponents["EmptyPlaceholder"] = () => (
    <tbody>
      <tr className={classnames(styles.tr, styles.emptyPlaceholder)}>
        <td className={styles.td} colSpan={columns.length} style={{ textAlign: "center" }}>
          No results
        </td>
      </tr>
    </tbody>
  );

  const onTabSelect = (tabId: FilterTabId) => {
    // all
    if (tabId === "all") {
      setColumnFilters((prevState) => [...prevState.filter((column) => column.id !== "stream.selected")]);
      return;
    }
    // enabled / disabled
    setColumnFilters((prevState) => [
      ...prevState.filter((column) => column.id !== "stream.selected"),
      { id: "stream.selected", value: tabId === "enabledStreams" },
    ]);
  };

  return (
    <>
      <Box p="md" pl="xl">
        {debugTable && (
          <Box p="md">
            {JSON.stringify(
              {
                globalFilter: getState().globalFilter,
                columnFilters: getState().columnFilters,
                totalRows: rows.length,
              },
              null,
              2
            )}
          </Box>
        )}
        <FlexContainer alignItems="center" justifyContent="space-between">
          <SearchInput
            value={filtering}
            placeholder={formatMessage({
              id: "form.streamOrFieldSearch",
            })}
            containerClassName={styles.searchInputContainer}
            onChange={(e) => setFiltering(e.target.value)}
          />
          <FlexContainer>
            <FlexContainer justifyContent="flex-end" alignItems="center" direction="row" gap="lg">
              {mode === "create" ? (
                <TableControls
                  isAllRowsExpanded={getIsAllRowsExpanded()}
                  toggleAllRowsExpanded={toggleAllRowsExpanded}
                />
              ) : (
                <FormControls>
                  <TableControls
                    isAllRowsExpanded={getIsAllRowsExpanded()}
                    toggleAllRowsExpanded={toggleAllRowsExpanded}
                  />
                </FormControls>
              )}
            </FlexContainer>
          </FlexContainer>
        </FlexContainer>
      </Box>
      <Box pl="xl">
        <StreamsFilterTabs columnFilters={columnFilters} onTabSelect={onTabSelect} />
      </Box>
      <TableVirtuoso<SyncCatalogUIModel>
        style={{ height: "50vh" }}
        totalCount={rows.length}
        initialTopMostItemIndex={initialTopMostItemIndex}
        components={{
          Table,
          TableHead,
          TableRow,
          EmptyPlaceholder,
        }}
        fixedHeaderContent={headerContent}
        fixedItemHeight={40}
        increaseViewportBy={50}
      />
    </>
  );
};
