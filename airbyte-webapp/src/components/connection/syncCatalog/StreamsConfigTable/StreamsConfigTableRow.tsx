import classNames from "classnames";
import React, { memo, useMemo, useRef, useState } from "react";
import { FormattedMessage } from "react-intl";
import { useLocation, useNavigate } from "react-router-dom";
import { useEffectOnce } from "react-use";

import { FlexContainer } from "components/ui/Flex";
import { Switch } from "components/ui/Switch";
import { Text, TextWithOverflowTooltip } from "components/ui/Text";

import { Path, SyncSchemaField } from "core/domain/catalog";

import { FieldSelectionStatus, FieldSelectionStatusVariant } from "./FieldSelectionStatus";
import styles from "./StreamsConfigTableRow.module.scss";
import { StreamsConfigTableRowStatus } from "./StreamsConfigTableRowStatus";
import { useStreamsConfigTableRowProps } from "./useStreamsConfigTableRowProps";
import { SyncStreamFieldWithId } from "../../ConnectionForm/formConfig";
import { LocationWithState } from "../../ConnectionForm/SyncCatalogCard";
import { CellText } from "../CellText";
import { SyncModeSelect, SyncModeValue } from "../SyncModeSelect";
import { FieldPathType } from "../utils";

interface StreamsConfigTableRowInnerProps {
  stream: SyncStreamFieldWithId;
  destName: string;
  destNamespace: string;
  availableSyncModes: SyncModeValue[];
  onSelectSyncMode: (data: SyncModeValue) => void;
  onSelectStream: () => void;
  primitiveFields: SyncSchemaField[];
  pkType: FieldPathType;
  onPrimaryKeyChange: (pkPath: Path[]) => void;
  cursorType: FieldPathType;
  onCursorChange: (cursorPath: Path) => void;
  fields: SyncSchemaField[];
  openStreamDetailsPanel: () => void;
  configErrors?: Record<string, string>;
  disabled?: boolean;
}

/**
 * react-hook-form sync catalog table row component
 */
const StreamsConfigTableRowInner: React.FC<StreamsConfigTableRowInnerProps & { className?: string }> = ({
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
  className,
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

  const syncSchema: SyncModeValue | undefined = useMemo(() => {
    if (!syncMode || !destinationSyncMode) {
      return undefined;
    }
    return {
      syncMode,
      destinationSyncMode,
    };
  }, [syncMode, destinationSyncMode]);

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

  const { state: locationState, pathname } = useLocation() as LocationWithState;
  const navigate = useNavigate();
  const rowRef = useRef<HTMLDivElement>(null);
  const [highlighted, setHighlighted] = useState(false);

  useEffectOnce(() => {
    let highlightTimeout: number;
    let openTimeout: number;

    // Is it the stream we are looking for?
    if (locationState?.streamName === stream.stream?.name && locationState?.namespace === stream.stream?.namespace) {
      // Scroll to the stream and highlight it
      if (locationState?.action === "showInReplicationTable" || locationState?.action === "openDetails") {
        setHighlighted(true);
        highlightTimeout = window.setTimeout(() => {
          setHighlighted(false);
        }, 1500);
      }

      // Open the stream details
      if (locationState?.action === "openDetails") {
        openTimeout = window.setTimeout(() => {
          rowRef.current?.click();
        }, 750);
      }
      // remove the redirection info from the location state
      navigate(pathname, { replace: true });
    }

    return () => {
      window.clearTimeout(highlightTimeout);
      window.clearTimeout(openTimeout);
    };
  });

  return (
    <FlexContainer
      justifyContent="flex-start"
      alignItems="center"
      onClick={onRowClick}
      className={classNames(streamHeaderContentStyle, className, { [styles.highlighted]: highlighted })}
      data-testid={`catalog-tree-table-row-${stream.stream?.namespace || "no-namespace"}-${stream.stream?.name}`}
      ref={rowRef}
    >
      <CellText size="fixed" className={styles.syncCell} data-noexpand>
        <Switch
          size="sm"
          checked={stream.config?.selected} // stream sync enabled or disabled
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
    </FlexContainer>
  );
};

export const StreamsConfigTableRow = memo(StreamsConfigTableRowInner);
