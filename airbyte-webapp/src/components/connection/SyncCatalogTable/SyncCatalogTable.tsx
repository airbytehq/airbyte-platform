import {
  ColumnFiltersState,
  createColumnHelper,
  ExpandedState,
  flexRender,
  getCoreRowModel,
  getGroupedRowModel,
  getSortedRowModel,
  Row,
  useReactTable,
} from "@tanstack/react-table";
import classnames from "classnames";
import set from "lodash/set";
import React, { FC, useCallback, useMemo, useState, useContext } from "react";
import { useFieldArray, useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import { ItemProps, TableComponents, TableVirtuoso } from "react-virtuoso";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { ScrollParentContext } from "components/ui/ScrollParent";
import { SearchInput } from "components/ui/SearchInput";
import { ColumnMeta } from "components/ui/Table/types";
import { Text } from "components/ui/Text";
import { InfoTooltip, TooltipLearnMoreLink } from "components/ui/Tooltip";

import { AirbyteStreamAndConfiguration, AirbyteStreamConfiguration } from "core/api/types/AirbyteClient";
import { SyncSchemaField } from "core/domain/catalog";
import { FeatureItem, useFeature } from "core/services/features";
import { links } from "core/utils/links";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useExperiment } from "hooks/services/Experiment";

import { ExpandCollapseAllControl } from "./components/ExpandCollapseAllControl";
import { FieldCursorCell } from "./components/FieldCursorCell";
import { FieldHashMapping } from "./components/FieldHashMapping";
import { FieldPKCell } from "./components/FieldPKCell";
import { FormControls } from "./components/FormControls";
import { NamespaceNameCell } from "./components/NamespaceNameCell";
import { RefreshSchemaControl } from "./components/RefreshSchemaControl";
import { SelectedFieldsCell } from "./components/SelectedFieldsCell";
import { StreamCursorCell } from "./components/StreamCursorCell";
import { StreamFieldNameCell } from "./components/StreamFieldCell";
import { StreamNameCell } from "./components/StreamNameCell";
import { StreamPKCell } from "./components/StreamPKCell";
import { FilterTabId, StreamsFilterTabs } from "./components/StreamsFilterTabs";
import { SyncModeCell } from "./components/SyncModeCell";
import { getExpandedRowModel } from "./getExpandedRowModel";
import { getFilteredRowModel } from "./getFilteredRowModel";
import { useInitialRowIndex } from "./hooks/useInitialRowIndex";
import { useNamespaceRowInView } from "./hooks/useNamespaceRowInView";
import styles from "./SyncCatalogTable.module.scss";
import {
  findRow,
  generateTestId,
  getNamespaceRowId,
  getRowChangeStatus,
  getSyncCatalogRows,
  isNamespaceRow,
  isStreamRow,
} from "./utils/utils";
import { FormConnectionFormValues, SyncStreamFieldWithId, useInitialFormValues } from "../ConnectionForm/formConfig";

export interface SyncCatalogUIModel {
  rowType: "namespace" | "stream" | "field" | "nestedField";
  /**
   * react-form syncCatalog streamNode field
   */
  streamNode?: SyncStreamFieldWithId;
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
  subRows?: SyncCatalogUIModel[];
}

export const SyncCatalogTable: FC = () => {
  const { formatMessage } = useIntl();
  const { mode, connection } = useConnectionFormService();
  const initialValues = useInitialFormValues(connection, mode);
  const { control, trigger } = useFormContext<FormConnectionFormValues>();
  const {
    fields: streams,
    update,
    replace,
  } = useFieldArray({
    name: "syncCatalog.streams",
    control,
  });
  const prefix = useWatch<FormConnectionFormValues>({ name: "prefix", control });
  const watchedNamespaceDefinition = useWatch<FormConnectionFormValues>({ name: "namespaceDefinition", control });
  const watchedNamespaceFormat = useWatch<FormConnectionFormValues>({ name: "namespaceFormat", control });

  const isHashingSupported = useFeature(FeatureItem.FieldHashing);
  const isHashingEnabled = useExperiment("connection.hashingUI");
  const showHashing = isHashingSupported && isHashingEnabled;

  const [columnFilters, setColumnFilters] = useState<ColumnFiltersState>([]);
  const [filtering, setFiltering] = useState("");
  const deferredFilteringValue = filtering;

  const customScrollParent = useContext(ScrollParentContext);

  // Update stream
  const onUpdateStreamConfigWithStreamNode = useCallback(
    (streamNode: SyncStreamFieldWithId, updatedConfig: Partial<AirbyteStreamConfiguration>) => {
      const updatedStreamNode = set(streamNode, "config", {
        ...streamNode.config,
        ...updatedConfig,
      });

      // if this stream is disabled, remove any hashedFields configured
      if (updatedStreamNode.config?.selected === false) {
        updatedStreamNode.config.hashedFields = undefined;
      }

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
      filterFn: (row, _, filterValue) => row.original.streamNode?.config?.selected === filterValue,
      enableGlobalFilter: false, // since it's boolean value we don't need to filter it globally
    }),
    columnHelper.accessor("name", {
      id: "stream.name",
      header: ({ table }) =>
        stickyRow && (
          <NamespaceNameCell
            row={stickyRow}
            streams={streams}
            onStreamsChanged={replace}
            syncCheckboxDisabled={!!table.getState().globalFilter.length}
            namespaceFormat={watchedNamespaceFormat}
            namespaceDefinition={watchedNamespaceDefinition}
            columnFilters={columnFilters}
          />
        ),
      cell: ({ row, getValue, table }) => (
        <div
          style={{
            // shift row depending on depth: namespace -> stream -> field -> nestedField
            paddingLeft: `${row.depth === 0 ? 0 : row.depth * 20}px`,
          }}
        >
          {isNamespaceRow(row) ? (
            <NamespaceNameCell
              row={row}
              streams={streams}
              onStreamsChanged={replace}
              syncCheckboxDisabled={!!table.getState().globalFilter.length}
              namespaceFormat={watchedNamespaceFormat}
              namespaceDefinition={watchedNamespaceDefinition}
              columnFilters={columnFilters}
            />
          ) : isStreamRow(row) ? (
            <StreamNameCell
              value={getValue()}
              row={row}
              updateStreamField={onUpdateStreamConfigWithStreamNode}
              globalFilterValue={table.getState().globalFilter}
            />
          ) : (
            <StreamFieldNameCell
              row={row}
              updateStreamField={onUpdateStreamConfigWithStreamNode}
              globalFilterValue={table.getState().globalFilter}
            />
          )}
        </div>
      ),
      meta: {
        thClassName: styles.streamOrFieldNameCell,
        tdClassName: styles.streamOrFieldNameCell,
      },
    }),
    columnHelper.display({
      id: "hashing",
      header: () => (
        <FlexContainer alignItems="center" gap="none">
          <Text size="sm" color="grey500">
            <FormattedMessage id="connectionForm.hashing.title" />
          </Text>
          <InfoTooltip>
            <FormattedMessage id="connectionForm.hashing.info" />
          </InfoTooltip>
        </FlexContainer>
      ),
      cell: ({ row }) =>
        isNamespaceRow(row) || isStreamRow(row) ? null : (
          <FieldHashMapping row={row} updateStreamField={onUpdateStreamConfigWithStreamNode} />
        ),
      meta: {
        thClassName: styles.hashCell,
        tdClassName: styles.hashCell,
      },
    }),
    columnHelper.accessor("syncMode", {
      header: () => (
        <FlexContainer alignItems="center" gap="none">
          <Text size="sm" color="grey500">
            <FormattedMessage id="form.syncMode" />
          </Text>
          <InfoTooltip>
            <FormattedMessage id="connectionForm.syncType.info" />
            <TooltipLearnMoreLink url={links.syncModeLink} />
          </InfoTooltip>
        </FlexContainer>
      ),
      cell: ({ row }) =>
        isNamespaceRow(row) ? (
          <FlexContainer alignItems="center" gap="none">
            <Text size="sm" color="grey500">
              <FormattedMessage id="form.syncMode" />
            </Text>
            <InfoTooltip>
              <FormattedMessage id="connectionForm.syncType.info" />
              <TooltipLearnMoreLink url={links.syncModeLink} />
            </InfoTooltip>
          </FlexContainer>
        ) : isStreamRow(row) ? (
          <SyncModeCell row={row} updateStreamField={onUpdateStreamConfigWithStreamNode} />
        ) : null,
      meta: {
        thClassName: styles.syncModeCell,
        tdClassName: styles.syncModeCell,
      },
    }),
    columnHelper.accessor("primaryKey", {
      header: () => {},
      cell: ({ row }) =>
        isStreamRow(row) ? (
          <StreamPKCell row={row} updateStreamField={onUpdateStreamConfigWithStreamNode} />
        ) : (
          <FieldPKCell row={row} />
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
          <StreamCursorCell row={row} updateStreamField={onUpdateStreamConfigWithStreamNode} />
        ) : (
          <FieldCursorCell row={row} />
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
      cell: ({ row }) =>
        isNamespaceRow(row) ? (
          <Text size="sm" color="grey500">
            <FormattedMessage id="form.fields" />
          </Text>
        ) : isStreamRow(row) ? (
          <SelectedFieldsCell row={row} />
        ) : null,
      meta: {
        thClassName: styles.amountOfFieldsCell,
        tdClassName: styles.amountOfFieldsCell,
      },
    }),
  ];

  const preparedData = useMemo(() => {
    return getSyncCatalogRows(streams, initialValues.syncCatalog.streams, prefix);
  }, [initialValues.syncCatalog.streams, prefix, streams]);

  /**
   * Get initial expanded state for all namespaces rows
   *  { [rowIndex]: boolean }, where rowIndex is the index of the namespace row
   */
  const initialExpandedState = useMemo(
    () => Object.fromEntries(preparedData.map((_, index) => [index, true])),
    [preparedData]
  );
  const [expanded, setExpanded] = React.useState<ExpandedState>(initialExpandedState);

  const { getHeaderGroups, getRowModel, toggleAllRowsExpanded } = useReactTable<SyncCatalogUIModel>({
    columns,
    data: preparedData,
    getSubRows: (row) => row.subRows,
    state: {
      expanded,
      globalFilter: deferredFilteringValue,
      columnFilters,
      columnVisibility: {
        "stream.selected": false,
        hashing: showHashing,
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
    getRowCanExpand: (row) => !!row.subRows.length,
  });

  const rows = getRowModel().rows;

  const initialTopMostItemIndex = useInitialRowIndex(rows);

  const [isAllStreamRowsExpanded, setIsAllStreamRowsExpanded] = useState(false);
  const toggleAllStreamRowsExpanded = useCallback(
    (expanded: boolean) => {
      if (!expanded) {
        setExpanded(initialExpandedState);
        setIsAllStreamRowsExpanded(expanded);
        return;
      }

      toggleAllRowsExpanded(expanded);
      setIsAllStreamRowsExpanded(expanded);
    },
    [initialExpandedState, toggleAllRowsExpanded]
  );

  const [stickyRowIndex, setStickyRowIndex] = useState<number>(0);
  const stickyIndexes = useMemo(
    () =>
      rows.reduce((indexes, row, index) => {
        if (row.depth === 0) {
          indexes.push(index);
        }
        return indexes;
      }, [] as number[]),
    [rows]
  );

  const stickyRow: Row<SyncCatalogUIModel> | undefined = useMemo(() => {
    if (!rows.length) {
      return;
    }

    const row = rows[stickyRowIndex];
    // handle index out of bounds in case of collapsing all rows
    if (!row) {
      return;
    }

    if (isNamespaceRow(row)) {
      return row;
    }
    return findRow(rows, getNamespaceRowId(row));
    /**
     * adding rows as a dependency will cause a millisecond flicker after toggling  expand/collapse all
     * we can't remove it since we need to react on any rows change(tab change, filter change, etc)
     */
  }, [stickyRowIndex, rows]);

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

  const EmptyPlaceholder: TableComponents["EmptyPlaceholder"] = () => (
    <tbody>
      <tr className={classnames(styles.tr, styles.emptyPlaceholder)}>
        <td className={styles.td} colSpan={columns.length} style={{ textAlign: "center" }}>
          <Text bold color="grey" align="center">
            <FormattedMessage
              id={
                deferredFilteringValue.length
                  ? "connection.catalogTree.noMatchingStreams"
                  : "connection.catalogTree.noStreams"
              }
            />
          </Text>
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
      <Box p="md" pl="xl" pr="xl" className={styles.stickyControlsContainer}>
        <FlexContainer alignItems="center" justifyContent="space-between">
          <SearchInput
            value={filtering}
            placeholder={formatMessage({
              id: "form.streamOrFieldSearch",
            })}
            containerClassName={styles.searchInputContainer}
            onChange={(e) => setFiltering(e.target.value)}
            onKeyDown={(e: React.KeyboardEvent<HTMLInputElement>) => {
              // We do not want to submit the connection form when pressing Enter in the search field
              e.key === "Enter" && e.preventDefault();
            }}
            data-testid="sync-catalog-search"
          />
          <FlexContainer>
            <FlexContainer justifyContent="flex-end" alignItems="center" direction="row" gap="lg">
              {mode === "create" ? (
                <>
                  <RefreshSchemaControl />
                  <ExpandCollapseAllControl
                    isAllRowsExpanded={isAllStreamRowsExpanded}
                    toggleAllRowsExpanded={toggleAllStreamRowsExpanded}
                  />
                </>
              ) : (
                <>
                  <FormControls>
                    <RefreshSchemaControl />
                  </FormControls>
                  <ExpandCollapseAllControl
                    isAllRowsExpanded={isAllStreamRowsExpanded}
                    toggleAllRowsExpanded={toggleAllStreamRowsExpanded}
                  />
                </>
              )}
            </FlexContainer>
          </FlexContainer>
        </FlexContainer>
      </Box>
      <Box pl="xl" className={styles.stickyTabsContainer}>
        <StreamsFilterTabs columnFilters={columnFilters} onTabSelect={onTabSelect} />
      </Box>
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
    </>
  );
};
