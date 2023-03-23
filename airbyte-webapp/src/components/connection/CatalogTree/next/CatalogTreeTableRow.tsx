import classNames from "classnames";
import React, { useMemo, useRef } from "react";
import { FormattedMessage } from "react-intl";

import { ArrowRightIcon } from "components/icons/ArrowRightIcon";
import { Row } from "components/SimpleTableComponents";
import { CheckBox } from "components/ui/CheckBox";
import { Switch } from "components/ui/Switch";
import { Text } from "components/ui/Text";

import { useBulkEditSelect } from "hooks/services/BulkEdit/BulkEditService";
import { useExperiment } from "hooks/services/Experiment";

import { CatalogTreeTableCell } from "./CatalogTreeTableCell";
import styles from "./CatalogTreeTableRow.module.scss";
import { CatalogTreeTableRowIcon } from "./CatalogTreeTableRowIcon";
import { FieldSelectionStatus, FieldSelectionStatusVariant } from "./FieldSelectionStatus";
import { StreamPathSelect } from "./StreamPathSelect";
import { SyncModeSelect } from "./SyncModeSelect";
import { useCatalogTreeTableRowProps } from "./useCatalogTreeTableRowProps";
import { useScrollIntoViewStream } from "./useScrollIntoViewStream";
import { StreamHeaderProps } from "../StreamHeader";

export const CatalogTreeTableRow: React.FC<StreamHeaderProps> = ({
  stream,
  destName,
  destNamespace,
  onSelectSyncMode,
  onSelectStream,
  availableSyncModes,
  pkType,
  onPrimaryKeyChange,
  onCursorChange,
  primitiveFields,
  cursorType,
  // isRowExpanded,
  fields,
  onExpand,
  disabled,
  configErrors,
}) => {
  const isColumnSelectionEnabled = useExperiment("connection.columnSelection", false);
  const { primaryKey, cursorField, syncMode, destinationSyncMode, selectedFields } = stream.config ?? {};
  const { defaultCursorField } = stream.stream ?? {};
  const syncSchema = useMemo(
    () => ({
      syncMode,
      destinationSyncMode,
    }),
    [syncMode, destinationSyncMode]
  );

  const [isSelected, selectForBulkEdit] = useBulkEditSelect(stream.id);

  const paths = useMemo(() => primitiveFields.map((field) => field.path), [primitiveFields]);
  const fieldCount = fields?.length ?? 0;
  const selectedFieldCount = selectedFields?.length ?? fieldCount;
  const onRowClick = fieldCount > 0 ? () => onExpand() : undefined;

  const { streamHeaderContentStyle, pillButtonVariant } = useCatalogTreeTableRowProps(stream);

  const streamNameRef = useRef<HTMLParagraphElement>(null);
  const namespaceRef = useRef<HTMLParagraphElement>(null);
  const { isVisible } = useScrollIntoViewStream(stream, namespaceRef, streamNameRef);

  return (
    <Row
      onClick={onRowClick}
      className={classNames(streamHeaderContentStyle, { [styles.highlight]: isVisible })}
      data-testid={`catalog-tree-table-row-${stream.stream?.namespace || "no-namespace"}-${stream.stream?.name}`}
    >
      <CatalogTreeTableCell size="fixed" className={styles.streamRowCheckboxCell}>
        {!disabled && (
          <>
            <CatalogTreeTableRowIcon stream={stream} />
            <CheckBox checkboxSize="sm" checked={isSelected} onChange={selectForBulkEdit} />
          </>
        )}
      </CatalogTreeTableCell>
      <CatalogTreeTableCell size="fixed" className={styles.syncCell}>
        <Switch
          size="sm"
          checked={stream.config?.selected}
          onChange={onSelectStream}
          disabled={disabled}
          data-testid="selected-switch"
        />
      </CatalogTreeTableCell>
      {isColumnSelectionEnabled && (
        <CatalogTreeTableCell size="fixed" className={styles.fieldsCell}>
          <FieldSelectionStatus
            selectedFieldCount={selectedFieldCount}
            totalFieldCount={fieldCount}
            variant={pillButtonVariant as FieldSelectionStatusVariant}
          />
        </CatalogTreeTableCell>
      )}
      <CatalogTreeTableCell withTooltip data-testid="source-namespace-cell">
        <Text size="md" className={styles.cellText} ref={namespaceRef}>
          {stream.stream?.namespace || <FormattedMessage id="form.noNamespace" />}
        </Text>
      </CatalogTreeTableCell>
      <CatalogTreeTableCell withTooltip data-testid="source-stream-name-cell">
        <Text size="md" className={styles.cellText} ref={streamNameRef}>
          {stream.stream?.name}
        </Text>
      </CatalogTreeTableCell>
      <CatalogTreeTableCell size="fixed" className={styles.syncModeCell}>
        <SyncModeSelect
          options={availableSyncModes}
          onChange={onSelectSyncMode}
          value={syncSchema}
          variant={pillButtonVariant}
          disabled={disabled}
        />
      </CatalogTreeTableCell>
      <CatalogTreeTableCell withTooltip>
        {cursorType && (
          <StreamPathSelect
            type="cursor"
            pathType={cursorType}
            paths={paths}
            path={cursorType === "sourceDefined" ? defaultCursorField : cursorField}
            onPathChange={onCursorChange}
            variant={pillButtonVariant}
            hasError={!!configErrors?.cursorField}
          />
        )}
      </CatalogTreeTableCell>
      <CatalogTreeTableCell withTooltip={pkType === "sourceDefined"}>
        {pkType && (
          <StreamPathSelect
            type="primary-key"
            pathType={pkType}
            paths={paths}
            path={primaryKey}
            isMulti
            onPathChange={onPrimaryKeyChange}
            variant={pillButtonVariant}
            hasError={!!configErrors?.primaryKey}
          />
        )}
      </CatalogTreeTableCell>
      <CatalogTreeTableCell size="fixed" className={styles.arrowCell}>
        <ArrowRightIcon />
      </CatalogTreeTableCell>
      <CatalogTreeTableCell withTooltip data-testid="destination-namespace-cell">
        <Text size="md" className={styles.cellText}>
          {destNamespace}
        </Text>
      </CatalogTreeTableCell>
      <CatalogTreeTableCell withTooltip data-testid="destination-stream-name-cell">
        <Text size="md" className={styles.cellText}>
          {destName}
        </Text>
      </CatalogTreeTableCell>
    </Row>
  );
};
