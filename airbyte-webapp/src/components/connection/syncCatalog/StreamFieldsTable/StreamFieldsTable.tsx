import { createColumnHelper } from "@tanstack/react-table";
import classNames from "classnames";
import isEqual from "lodash/isEqual";
import React, { useCallback, useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Switch } from "components/ui/Switch";
import { Table } from "components/ui/Table";
import { TextWithOverflowTooltip } from "components/ui/Text";

import { getDataType } from "area/connection/utils";
import { AirbyteStreamConfiguration } from "core/api/types/AirbyteClient";
import { SyncSchemaField, SyncSchemaFieldObject } from "core/domain/catalog";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useExperiment } from "hooks/services/Experiment";

import { CursorCell } from "./CursorCell";
import { PKCell } from "./PKCell";
import styles from "./StreamFieldsTable.module.scss";
import { SyncFieldCell } from "./SyncFieldCell";
import { CellText } from "../CellText";
import { ConnectorHeader } from "../ConnectorHeader";
import { getFieldPathDisplayName } from "../utils";

export interface TableStream {
  field: SyncSchemaField;
  isFieldSelected: boolean;
  path: string[];
  dataType: string;
  cursorDefined?: boolean;
  primaryKeyDefined?: boolean;
}

export interface StreamFieldsTableProps {
  config?: AirbyteStreamConfiguration;
  handleFieldToggle: (fieldPath: string[], isSelected: boolean) => void;
  onCursorSelect: (cursorPath: string[]) => void;
  onPkSelect: (pkPath: string[]) => void;
  shouldDefinePk: boolean;
  shouldDefineCursor: boolean;
  isCursorDefinitionSupported: boolean;
  isPKDefinitionSupported: boolean;
  syncSchemaFields: SyncSchemaField[];
  toggleAllFieldsSelected: () => void;
}

export function isCursor(config: AirbyteStreamConfiguration | undefined, path: string[]): boolean {
  return config ? isEqual(config?.cursorField, path) : false;
}

export function isChildFieldCursor(config: AirbyteStreamConfiguration | undefined, path: string[]): boolean {
  return config?.cursorField ? isEqual([config.cursorField[0]], path) : false;
}

export function isPrimaryKey(config: AirbyteStreamConfiguration | undefined, path: string[]): boolean {
  return !!config?.primaryKey?.some((p) => isEqual(p, path));
}

export function isChildFieldPrimaryKey(config: AirbyteStreamConfiguration | undefined, path: string[]): boolean {
  return !!config?.primaryKey?.some((p) => isEqual([p[0]], path));
}

export const StreamFieldsTable: React.FC<StreamFieldsTableProps> = ({
  config,
  handleFieldToggle,
  onPkSelect,
  onCursorSelect,
  shouldDefineCursor,
  shouldDefinePk,
  isCursorDefinitionSupported,
  isPKDefinitionSupported,
  syncSchemaFields,
  toggleAllFieldsSelected,
}) => {
  const { formatMessage } = useIntl();
  const isColumnSelectionEnabled = useExperiment("connection.columnSelection", true);
  const checkIsCursor = useCallback((path: string[]) => isCursor(config, path), [config]);
  const checkIsChildFieldCursor = useCallback((path: string[]) => isChildFieldCursor(config, path), [config]);
  const checkIsPrimaryKey = useCallback((path: string[]) => isPrimaryKey(config, path), [config]);
  const checkIsChildFieldPrimaryKey = useCallback((path: string[]) => isChildFieldPrimaryKey(config, path), [config]);

  const checkIsFieldSelected = useCallback(
    (field: SyncSchemaField): boolean => {
      // If the stream is disabled, effectively each field is unselected
      if (!config?.selected) {
        return false;
      }

      // All fields are implicitly selected if field selection is disabled
      if (!config?.fieldSelectionEnabled) {
        return true;
      }

      // path[0] is the top-level field name for all nested fields
      return !!config?.selectedFields?.find((f) => isEqual(f.fieldPath, [field.path[0]]));
    },
    [config?.selected, config?.fieldSelectionEnabled, config?.selectedFields]
  );

  // header group icons:
  const {
    connection: { source, destination },
    mode,
  } = useConnectionFormService();

  // prepare data for table
  const tableData: TableStream[] = useMemo(
    () =>
      syncSchemaFields.map((stream) => ({
        field: stream,
        isFieldSelected: checkIsFieldSelected(stream),
        path: stream.path,
        dataType: getDataType(stream),
        cursorDefined: shouldDefineCursor && SyncSchemaFieldObject.isPrimitive(stream),
        primaryKeyDefined: shouldDefinePk && SyncSchemaFieldObject.isPrimitive(stream),
      })),
    [shouldDefineCursor, shouldDefinePk, syncSchemaFields, checkIsFieldSelected]
  );

  const columnHelper = createColumnHelper<TableStream>();

  const sourceColumns = useMemo(
    () => [
      columnHelper.accessor("path", {
        id: "sourcePath",
        header: () => (
          <FlexContainer alignItems="center">
            {isColumnSelectionEnabled && (
              <FlexContainer className={styles.columnSelectionSwitchContainer}>
                <Switch
                  size="xs"
                  indeterminate={config?.fieldSelectionEnabled && !!config?.selectedFields?.length && config.selected}
                  checked={!config?.fieldSelectionEnabled && config?.selected}
                  onChange={toggleAllFieldsSelected}
                  disabled={mode === "readonly" || !config?.selected}
                />
              </FlexContainer>
            )}
            <FormattedMessage id="form.field.name" />
          </FlexContainer>
        ),
        cell: ({ getValue, row }) => (
          <CellText size="small">
            <FlexContainer alignItems="center">
              {isColumnSelectionEnabled && (
                <SyncFieldCell
                  field={row.original.field}
                  streamIsDisabled={!config?.selected}
                  isFieldSelected={row.original.isFieldSelected}
                  handleFieldToggle={handleFieldToggle}
                  checkIsCursor={checkIsCursor}
                  checkIsChildFieldCursor={checkIsChildFieldCursor}
                  checkIsPrimaryKey={checkIsPrimaryKey}
                  checkIsChildFieldPrimaryKey={checkIsChildFieldPrimaryKey}
                  syncMode={config?.syncMode}
                  destinationSyncMode={config?.destinationSyncMode}
                  className={styles.columnSelectionSwitchContainer}
                />
              )}
              <TextWithOverflowTooltip size="sm" data-testid="stream-source-field-name">
                {getFieldPathDisplayName(getValue())}
              </TextWithOverflowTooltip>
            </FlexContainer>
          </CellText>
        ),
        meta: {
          thClassName: styles.headerCell,
          tdClassName: styles.textCell,
        },
      }),
      columnHelper.accessor("dataType", {
        id: "sourceDataType",
        header: () => <FormattedMessage id="form.field.dataType" />,
        cell: ({ getValue }) => (
          <span data-testid="stream-source-field-data-type">
            <FormattedMessage id={`${getValue()}`} defaultMessage={formatMessage({ id: "airbyte.datatype.unknown" })} />
          </span>
        ),
        meta: {
          thClassName: styles.headerCell,
          tdClassName: styles.dataTypeCell,
        },
      }),
      columnHelper.accessor("cursorDefined", {
        id: "sourceCursorDefined",
        header: () => <FormattedMessage id="form.field.cursorField" />,
        cell: (props) => (
          <CursorCell
            isCursor={checkIsCursor}
            isCursorDefinitionSupported={isCursorDefinitionSupported}
            onCursorSelect={onCursorSelect}
            {...props}
          />
        ),
        meta: {
          thClassName: styles.headerCell,
          tdClassName: styles.cursorCell,
        },
      }),
      columnHelper.accessor("primaryKeyDefined", {
        id: "sourcePrimaryKeyDefined",
        header: () => <FormattedMessage id="form.field.primaryKey" />,
        cell: (props) => (
          <PKCell
            isPKDefinitionSupported={isPKDefinitionSupported}
            isPrimaryKey={checkIsPrimaryKey}
            onPkSelect={onPkSelect}
            {...props}
          />
        ),

        meta: {
          thClassName: styles.headerCell,
          tdClassName: styles.pkCell,
        },
      }),
    ],
    [
      columnHelper,
      config?.fieldSelectionEnabled,
      config?.selectedFields?.length,
      formatMessage,
      handleFieldToggle,
      isColumnSelectionEnabled,
      checkIsCursor,
      checkIsChildFieldCursor,
      checkIsPrimaryKey,
      checkIsChildFieldPrimaryKey,
      isCursorDefinitionSupported,
      isPKDefinitionSupported,
      onCursorSelect,
      onPkSelect,
      toggleAllFieldsSelected,
      config?.selected,
      config?.syncMode,
      config?.destinationSyncMode,
      mode,
    ]
  );

  const destinationColumns = useMemo(
    () => [
      columnHelper.accessor("path", {
        id: "destinationPath",
        header: () => <FormattedMessage id="form.field.name" />,
        cell: ({ getValue }) => (
          <CellText size="small">
            <TextWithOverflowTooltip size="sm" data-testid="stream-destination-field-name">
              {getFieldPathDisplayName(getValue())}
            </TextWithOverflowTooltip>
          </CellText>
        ),
        meta: {
          thClassName: styles.headerCell,
          tdClassName: styles.textCell,
        },
      }),
    ],
    [columnHelper]
  );

  const columns = useMemo(
    () => [
      columnHelper.group({
        id: "source",
        header: () => <ConnectorHeader type="source" icon={source.icon} />,
        columns: sourceColumns,
        meta: {
          thClassName: classNames(styles.headerGroupCell, styles.light),
        },
      }),
      columnHelper.group({
        id: "arrow",
        header: () => <Icon type="arrowRight" />,
        columns: [
          {
            id: "_", // leave the column name empty
            cell: () => <Icon type="arrowRight" />,
            meta: {
              thClassName: styles.headerCell,
              tdClassName: styles.arrowCell,
            },
          },
        ],
        meta: {
          thClassName: classNames(styles.headerGroupCell, styles.light),
        },
      }),
      columnHelper.group({
        id: "destination",
        header: () => <ConnectorHeader type="destination" icon={destination.icon} />,
        columns: destinationColumns,
        meta: {
          thClassName: classNames(styles.headerGroupCell, styles.light),
          tdClassName: styles.bodyCell,
        },
      }),
    ],
    [columnHelper, destination.icon, destinationColumns, source.icon, sourceColumns]
  );

  return (
    <Table<TableStream>
      variant="light"
      columns={columns}
      data={tableData}
      className={styles.customTableStyle}
      sorting={false}
      virtualized
      virtualizedProps={{
        /**
         * improve performance: since all rows have the same height - there is no need to recalculate the height
         */
        fixedItemHeight: 28,
        /**
         * reduce the number of rendered rows to improve performance
         */
        increaseViewportBy: 50,
      }}
    />
  );
};
