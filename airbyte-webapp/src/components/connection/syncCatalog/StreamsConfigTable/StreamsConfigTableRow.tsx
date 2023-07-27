import classNames from "classnames";
import React, { useEffect, useMemo, useRef, useState } from "react";
import { FormattedMessage } from "react-intl";

import { Row } from "components/SimpleTableComponents";
import { DropDownOptionDataItem } from "components/ui/DropDown";
import { FlexContainer } from "components/ui/Flex";
import { Switch } from "components/ui/Switch";
import { Text, TextWithOverflowTooltip } from "components/ui/Text";

import { Path, SyncSchemaField, SyncSchemaStream } from "core/domain/catalog";

import { FieldSelectionStatus, FieldSelectionStatusVariant } from "./FieldSelectionStatus";
import styles from "./StreamsConfigTableRow.module.scss";
import { StreamsConfigTableRowStatus } from "./StreamsConfigTableRowStatus";
import { useRedirectedReplicationStream } from "./useRedirectedReplicationStream";
import { useStreamsConfigTableRowProps } from "./useStreamsConfigTableRowProps";
import { CellText } from "../CellText";
import { SyncModeOption, SyncModeSelect } from "../SyncModeSelect";
import { FieldPathType } from "../utils";

interface StreamsConfigTableRowProps {
  stream: SyncSchemaStream;
  destName: string;
  destNamespace: string;
  availableSyncModes: SyncModeOption[];
  onSelectSyncMode: (selectedMode: DropDownOptionDataItem) => void;
  onSelectStream: () => void;
  primitiveFields: SyncSchemaField[];
  pkType: FieldPathType;
  onPrimaryKeyChange: (pkPath: Path[]) => void;
  cursorType: FieldPathType;
  onCursorChange: (cursorPath: Path) => void;
  fields: SyncSchemaField[];
  openStreamDetailsPanel: () => void;
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
  cursorType,
  fields,
  openStreamDetailsPanel,
  disabled,
  configErrors,
}) => {
  const { primaryKey, cursorField, syncMode, destinationSyncMode, selectedFields } = stream.config ?? {};

  const pathDisplayName = (path: Path): string => path.join(".");

  const { defaultCursorField } = stream.stream ?? {};

  const isCursorUndefined = useMemo(() => {
    if (cursorType === "sourceDefined" && defaultCursorField?.length) {
      return false;
    } else if (cursorType === "required" && cursorField?.length) {
      return false;
    }
    return true;
  }, [cursorField?.length, cursorType, defaultCursorField?.length]);

  const isPrimaryKeyUndefined = useMemo(() => {
    if (!primaryKey?.length) {
      return true;
    }
    return false;
  }, [primaryKey?.length]);

  const cursorFieldString = useMemo(() => {
    if (cursorType === "sourceDefined") {
      if (defaultCursorField?.length) {
        return pathDisplayName(defaultCursorField);
      }
      return <FormattedMessage id="connection.catalogTree.sourceDefined" />;
    } else if (cursorType === "required" && cursorField?.length) {
      return pathDisplayName(cursorField);
    }
    return <FormattedMessage id="form.error.missing" />;
  }, [cursorType, cursorField, defaultCursorField]);

  const primaryKeyString = useMemo(() => {
    if (!primaryKey?.length) {
      return <FormattedMessage id="form.error.missing" />;
    }
    return primaryKey.map(pathDisplayName).join(", ");
  }, [primaryKey]);

  const syncSchema = useMemo(
    () => ({
      syncMode,
      destinationSyncMode,
    }),
    [syncMode, destinationSyncMode]
  );

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

          openStreamDetailsPanel();
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
  }, [rowRef, redirectionAction, doesStreamExist]);

  return (
    <Row
      onClick={onRowClick}
      className={classNames(streamHeaderContentStyle, { [styles.highlighted]: highlighted })}
      data-testid={`catalog-tree-table-row-${stream.stream?.namespace || "no-namespace"}-${stream.stream?.name}`}
      ref={rowRef}
    >
      <CellText size="fixed" className={styles.syncCell} data-noexpand>
        <Switch
          size="sm"
          checked={stream.config?.selected}
          onChange={onSelectStream}
          disabled={disabled}
          data-testid="selected-switch"
        />
        <StreamsConfigTableRowStatus stream={stream} />
      </CellText>
      <CellText size="fixed" className={styles.dataDestinationCell} data-testid="destination-namespace-cell">
        <TextWithOverflowTooltip size="md" color="grey600">
          {destNamespace}
        </TextWithOverflowTooltip>
      </CellText>
      <CellText data-testid="destination-stream-name-cell">
        <TextWithOverflowTooltip size="md">{destName}</TextWithOverflowTooltip>
      </CellText>
      <CellText className={styles.syncModeCell}>
        <SyncModeSelect
          options={availableSyncModes}
          onChange={onSelectSyncMode}
          value={syncSchema}
          variant={pillButtonVariant}
          disabled={disabled}
        />
      </CellText>
      <CellText>
        {(cursorType || pkType) && (
          <FlexContainer direction="column" gap="xs">
            {cursorType && (
              <FlexContainer direction="row" gap="xs" alignItems="baseline" data-testid="cursor-field-cell">
                <Text as="span" color={!!configErrors?.cursorField ? "red" : "grey"}>
                  <FormattedMessage id="form.cursorField" />
                </Text>
                <TextWithOverflowTooltip
                  color={!!configErrors?.cursorField ? "red" : "grey600"}
                  className={classNames({
                    [styles.undefinedField]: isCursorUndefined,
                  })}
                >
                  {cursorFieldString}
                </TextWithOverflowTooltip>
              </FlexContainer>
            )}
            {pkType && (
              <FlexContainer direction="row" gap="xs" alignItems="baseline" data-testid="primary-key-cell">
                <Text as="span" color={!!configErrors?.primaryKey ? "red" : "grey"}>
                  <FormattedMessage id="form.primaryKey" />
                </Text>
                <TextWithOverflowTooltip
                  color={!!configErrors?.primaryKey ? "red" : "grey600"}
                  className={classNames({
                    [styles.undefinedField]: isPrimaryKeyUndefined,
                  })}
                >
                  {primaryKeyString}
                </TextWithOverflowTooltip>
              </FlexContainer>
            )}
          </FlexContainer>
        )}
      </CellText>
      <CellText size="fixed" className={styles.fieldsCell}>
        <FieldSelectionStatus
          selectedFieldCount={selectedFieldCount}
          totalFieldCount={fieldCount}
          variant={pillButtonVariant as FieldSelectionStatusVariant}
        />
      </CellText>
    </Row>
  );
};
