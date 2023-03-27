import classNames from "classnames";
import React, { useMemo, useRef } from "react";
import { FormattedMessage } from "react-intl";

import { ArrowRightIcon } from "components/icons/ArrowRightIcon";
import { Row } from "components/SimpleTableComponents";
import { CheckBox } from "components/ui/CheckBox";
import { DropDownOptionDataItem } from "components/ui/DropDown";
import { Switch } from "components/ui/Switch";
import { Text } from "components/ui/Text";

import { Path, SyncSchemaField, SyncSchemaStream } from "core/domain/catalog";
import { useBulkEditSelect } from "hooks/services/BulkEdit/BulkEditService";
import { useExperiment } from "hooks/services/Experiment";

import { FieldSelectionStatus, FieldSelectionStatusVariant } from "./FieldSelectionStatus";
import styles from "./StreamsConfigTableRow.module.scss";
import { StreamsConfigTableRowStatus } from "./StreamsConfigTableRowStatus";
import { useScrollIntoViewStream } from "./useScrollIntoViewStream";
import { useStreamsConfigTableRowProps } from "./useStreamsConfigTableRowProps";
import { CellText } from "../CellText";
import { IndexerType, StreamPathSelect } from "../StreamPathSelect";
import { SyncModeOption, SyncModeSelect } from "../SyncModeSelect";

interface StreamsConfigTableRowProps {
  stream: SyncSchemaStream;
  destName: string;
  destNamespace: string;
  availableSyncModes: SyncModeOption[];
  onSelectSyncMode: (selectedMode: DropDownOptionDataItem) => void;
  onSelectStream: () => void;
  primitiveFields: SyncSchemaField[];
  pkType: IndexerType;
  onPrimaryKeyChange: (pkPath: Path[]) => void;
  cursorType: IndexerType;
  onCursorChange: (cursorPath: Path) => void;
  fields: SyncSchemaField[];
  onExpand: () => void;
  changedSelected: boolean;
  hasError: boolean;
  configErrors?: Record<string, string>;
  disabled?: boolean;
}

export const StreamsConfigTableRow: React.FC<StreamsConfigTableRowProps> = ({
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

  const { streamHeaderContentStyle, pillButtonVariant } = useStreamsConfigTableRowProps(stream);

  const streamNameRef = useRef<HTMLParagraphElement>(null);
  const namespaceRef = useRef<HTMLParagraphElement>(null);
  const { isVisible } = useScrollIntoViewStream(stream, namespaceRef, streamNameRef);

  return (
    <Row
      onClick={onRowClick}
      className={classNames(streamHeaderContentStyle, { [styles.highlight]: isVisible })}
      data-testid={`catalog-tree-table-row-${stream.stream?.namespace || "no-namespace"}-${stream.stream?.name}`}
    >
      <CellText size="fixed" className={styles.streamRowCheckboxCell}>
        {!disabled && (
          <>
            <StreamsConfigTableRowStatus stream={stream} />
            <CheckBox checkboxSize="sm" checked={isSelected} onChange={selectForBulkEdit} />
          </>
        )}
      </CellText>
      <CellText size="fixed" className={styles.syncCell}>
        <Switch
          size="sm"
          checked={stream.config?.selected}
          onChange={onSelectStream}
          disabled={disabled}
          data-testid="selected-switch"
        />
      </CellText>
      {isColumnSelectionEnabled && (
        <CellText size="fixed" className={styles.fieldsCell}>
          <FieldSelectionStatus
            selectedFieldCount={selectedFieldCount}
            totalFieldCount={fieldCount}
            variant={pillButtonVariant as FieldSelectionStatusVariant}
          />
        </CellText>
      )}
      <CellText withTooltip data-testid="source-namespace-cell">
        <Text size="md" className={styles.cellText} ref={namespaceRef}>
          {stream.stream?.namespace || <FormattedMessage id="form.noNamespace" />}
        </Text>
      </CellText>
      <CellText withTooltip data-testid="source-stream-name-cell">
        <Text size="md" className={styles.cellText} ref={streamNameRef}>
          {stream.stream?.name}
        </Text>
      </CellText>
      <CellText size="fixed" className={styles.syncModeCell}>
        <SyncModeSelect
          options={availableSyncModes}
          onChange={onSelectSyncMode}
          value={syncSchema}
          variant={pillButtonVariant}
          disabled={disabled}
        />
      </CellText>
      <CellText withTooltip>
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
      </CellText>
      <CellText withTooltip={pkType === "sourceDefined"}>
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
      </CellText>
      <CellText size="fixed" className={styles.arrowCell}>
        <ArrowRightIcon />
      </CellText>
      <CellText withTooltip data-testid="destination-namespace-cell">
        <Text size="md" className={styles.cellText}>
          {destNamespace}
        </Text>
      </CellText>
      <CellText withTooltip data-testid="destination-stream-name-cell">
        <Text size="md" className={styles.cellText}>
          {destName}
        </Text>
      </CellText>
    </Row>
  );
};
