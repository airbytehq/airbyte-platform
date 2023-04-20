import classNames from "classnames";
import React, { useEffect, useMemo, useRef, useState } from "react";
import { FormattedMessage } from "react-intl";

import { ArrowRightIcon } from "components/icons/ArrowRightIcon";
import { Row } from "components/SimpleTableComponents";
import { CheckBox } from "components/ui/CheckBox";
import { DropDownOptionDataItem } from "components/ui/DropDown";
import { Switch } from "components/ui/Switch";
import { TextWithOverflowTooltip } from "components/ui/Text";

import { Path, SyncSchemaField, SyncSchemaStream } from "core/domain/catalog";
import { useBulkEditSelect } from "hooks/services/BulkEdit/BulkEditService";
import { useExperiment } from "hooks/services/Experiment";

import { FieldSelectionStatus, FieldSelectionStatusVariant } from "./FieldSelectionStatus";
import styles from "./StreamsConfigTableRow.module.scss";
import { StreamsConfigTableRowStatus } from "./StreamsConfigTableRowStatus";
import { useRedirectedReplicationStream } from "./useRedirectedReplicationStream";
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
  const onRowClick: React.MouseEventHandler<HTMLElement> | undefined =
    fieldCount > 0
      ? (e) => {
          let target: Element | null = e.target as Element;

          // if the target is or has a *[data-noexpand] ancestor
          // then exit, otherwise toggle expand
          while (target) {
            if (target.hasAttribute("data-noexpand")) {
              return;
            }
            target = target.parentElement;
          }

          onExpand();
        }
      : undefined;

  const { streamHeaderContentStyle, pillButtonVariant } = useStreamsConfigTableRowProps(stream);

  const rowRef = useRef<HTMLDivElement>(null);
  const [highlighted, setHighlighted] = useState(false);
  const { doesStreamExist, redirectionAction } = useRedirectedReplicationStream(stream);

  useEffect(() => {
    let highlightTimeout: number;
    let openTimeout: number;

    // Scroll to the stream and highlight it
    if (doesStreamExist && (redirectionAction === "showInReplicationTable" || redirectionAction === "openDetails")) {
      rowRef.current?.scrollIntoView({ block: "center" });
      setHighlighted(true);
      highlightTimeout = window.setTimeout(() => {
        setHighlighted(false);
      }, 1500);
    }

    // Open the stream details
    if (doesStreamExist && redirectionAction === "openDetails") {
      openTimeout = window.setTimeout(() => {
        rowRef.current?.click();
      }, 750);
    }

    return () => {
      window.clearTimeout(highlightTimeout);
      window.clearTimeout(openTimeout);
    };
  }, [stream, rowRef, redirectionAction, doesStreamExist]);

  return (
    <Row
      onClick={onRowClick}
      className={classNames(streamHeaderContentStyle, { [styles.highlighted]: highlighted })}
      data-testid={`catalog-tree-table-row-${stream.stream?.namespace || "no-namespace"}-${stream.stream?.name}`}
      ref={rowRef}
    >
      <CellText size="fixed" className={styles.streamRowCheckboxCell} data-noexpand>
        {!disabled && (
          <>
            <StreamsConfigTableRowStatus stream={stream} />
            <CheckBox checkboxSize="sm" checked={isSelected} onChange={selectForBulkEdit} />
          </>
        )}
      </CellText>
      <CellText size="fixed" className={styles.syncCell} data-noexpand>
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
      <CellText data-testid="source-namespace-cell">
        <TextWithOverflowTooltip size="md">
          {stream.stream?.namespace || <FormattedMessage id="form.noNamespace" />}
        </TextWithOverflowTooltip>
      </CellText>
      <CellText data-testid="source-stream-name-cell">
        <TextWithOverflowTooltip size="md">{stream.stream?.name}</TextWithOverflowTooltip>
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
      <CellText>
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
      <CellText>
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
      <CellText data-testid="destination-namespace-cell">
        <TextWithOverflowTooltip size="md">{destNamespace}</TextWithOverflowTooltip>
      </CellText>
      <CellText data-testid="destination-stream-name-cell">
        <TextWithOverflowTooltip size="md">{destName}</TextWithOverflowTooltip>
      </CellText>
    </Row>
  );
};
