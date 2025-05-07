import { ColumnDef, ColumnFiltersState, createColumnHelper, ExpandedState, Row } from "@tanstack/react-table";
import set from "lodash/set";
import React, { FC, useCallback, useMemo, useState } from "react";
import { useFieldArray, useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";
import { InfoTooltip, TooltipLearnMoreLink } from "components/ui/Tooltip";

import { AirbyteStreamAndConfiguration, AirbyteStreamConfiguration } from "core/api/types/AirbyteClient";
import { SyncSchemaField } from "core/domain/catalog";
import { FeatureItem, useFeature } from "core/services/features";
import { useFormMode } from "core/services/ui/FormModeContext";
import { links } from "core/utils/links";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useExperiment } from "hooks/services/Experiment";

import {
  FieldCursorCell,
  FieldPKCell,
  SelectedFieldsCell,
  StreamFieldNameCell,
  StreamNameCell,
  StreamPKCell,
  NamespaceNameCell,
  StreamCursorCell,
} from "./components/cells";
import { FieldHashMapping } from "./components/FieldHashMapping";
import { SearchAndFilterControls, STREAMS_AND_FIELDS } from "./components/SearchAndFilterControls";
import { FilterTabId } from "./components/StreamsFilterTabs";
import { SyncCatalogVirtuosoTable } from "./components/SyncCatalogVirtuosoTable";
import { SyncModeCell } from "./components/SyncModeCell";
import { useInitialRowIndex } from "./hooks/useInitialRowIndex";
import { useSyncCatalogReactTable } from "./hooks/useSyncCatalogReactTable";
import styles from "./SyncCatalogTable.module.scss";
import { findRow, getNamespaceRowId, getSyncCatalogRows, isNamespaceRow, isStreamRow } from "./utils";
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
  const { connection } = useConnectionFormService();
  const { mode } = useFormMode();
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
  const isMappingEnabled = useExperiment("connection.mappingsUI");
  const showHashing = isHashingSupported && isHashingEnabled && !isMappingEnabled;

  const [columnFilters, setColumnFilters] = useState<ColumnFiltersState>([]);
  const [filtering, setFiltering] = useState("");
  const [filteringDepth, setFilteringDepth] = useState<number>(STREAMS_AND_FIELDS);
  const deferredFilteringValue = filtering;

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

  const { getHeaderGroups, getRowModel, toggleAllRowsExpanded, getState } = useSyncCatalogReactTable({
    columns: columns as Array<ColumnDef<SyncCatalogUIModel>>,
    data: preparedData,
    expanded,
    setExpanded,
    globalFilter: deferredFilteringValue,
    globalFilterMaxDepth: filteringDepth,
    columnFilters,
    setColumnFilters,
    showHashing,
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
      <SearchAndFilterControls
        filtering={filtering}
        setFiltering={setFiltering}
        filteringDepth={filteringDepth}
        setFilteringDepth={setFilteringDepth}
        isAllStreamRowsExpanded={isAllStreamRowsExpanded}
        toggleAllStreamRowsExpanded={toggleAllStreamRowsExpanded}
        columnFilters={columnFilters}
        onTabSelect={onTabSelect}
      />
      <SyncCatalogVirtuosoTable
        rows={rows}
        getHeaderGroups={getHeaderGroups}
        getState={getState}
        initialTopMostItemIndex={initialTopMostItemIndex}
        stickyRowIndex={stickyRowIndex}
        setStickyRowIndex={setStickyRowIndex}
        columnFilters={columnFilters}
        stickyIndexes={stickyIndexes}
        expanded={expanded}
      />
    </>
  );
};
