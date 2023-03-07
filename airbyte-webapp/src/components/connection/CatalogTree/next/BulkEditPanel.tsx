import classNames from "classnames";
import intersection from "lodash/intersection";
import React, { useMemo } from "react";
import { createPortal } from "react-dom";
import { FormattedMessage } from "react-intl";

import { SUPPORTED_MODES } from "components/connection/ConnectionForm/formConfig";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Switch } from "components/ui/Switch";

import { SyncSchemaField, SyncSchemaFieldObject, SyncSchemaStream, traverseSchemaToField } from "core/domain/catalog";
import { DestinationSyncMode, SyncMode } from "core/request/AirbyteClient";
import { useBulkEditService } from "hooks/services/BulkEdit/BulkEditService";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { isCloudApp } from "utils/app";

import styles from "./BulkEditPanel.module.scss";
import { StreamPathSelect } from "./StreamPathSelect";
import { SyncModeOption, SyncModeSelect } from "./SyncModeSelect";
import { pathDisplayName } from "../PathPopout";
import { HeaderCell } from "../styles";
import { flatten, getPathType } from "../utils";

export function calculateSharedFields(selectedBatchNodes: SyncSchemaStream[]) {
  const primitiveFieldsByStream = selectedBatchNodes.map(({ stream }) => {
    const traversedFields = traverseSchemaToField(stream?.jsonSchema, stream?.name);
    const flattenedFields = flatten(traversedFields);

    return flattenedFields.filter(SyncSchemaFieldObject.isPrimitive);
  });

  const pathMap = new Map<string, SyncSchemaField>();

  // calculate intersection of primitive fields across all selected streams
  primitiveFieldsByStream.forEach((fields, index) => {
    if (index === 0) {
      fields.forEach((field) => pathMap.set(pathDisplayName(field.path), field));
    } else {
      const fieldMap = new Set(fields.map((f) => pathDisplayName(f.path)));
      pathMap.forEach((_, k) => (!fieldMap.has(k) ? pathMap.delete(k) : null));
    }
  });

  return Array.from(pathMap.values());
}

export const getAvailableSyncModesOptions = (
  nodes: SyncSchemaStream[],
  syncModes?: DestinationSyncMode[]
): SyncModeOption[] =>
  SUPPORTED_MODES.filter(([syncMode, destinationSyncMode]) => {
    const supportableModes = intersection(...nodes.map((n) => n.stream?.supportedSyncModes));
    return supportableModes.includes(syncMode) && syncModes?.includes(destinationSyncMode);
  }).map(([syncMode, destinationSyncMode]) => ({
    value: { syncMode, destinationSyncMode },
  }));

export const BulkEditPanel: React.FC = () => {
  const {
    destDefinitionSpecification: { supportedDestinationSyncModes },
  } = useConnectionFormService();
  const { selectedBatchNodes, options, onChangeOption, onApply, isActive, onCancel } = useBulkEditService();
  const availableSyncModesOptions = useMemo(
    () => getAvailableSyncModesOptions(selectedBatchNodes, supportedDestinationSyncModes),
    [selectedBatchNodes, supportedDestinationSyncModes]
  );

  const primitiveFields: SyncSchemaField[] = useMemo(
    () => calculateSharedFields(selectedBatchNodes),
    [selectedBatchNodes]
  );

  const pkRequired = options.destinationSyncMode === DestinationSyncMode.append_dedup;
  const shouldDefinePk = selectedBatchNodes.every((n) => n.stream?.sourceDefinedPrimaryKey?.length === 0) && pkRequired;
  const cursorRequired = options.syncMode === SyncMode.incremental;
  const shouldDefineCursor = selectedBatchNodes.every((n) => !n.stream?.sourceDefinedCursor) && cursorRequired;
  const numStreamsSelected = selectedBatchNodes.length;

  const pkType = getPathType(pkRequired, shouldDefinePk);
  const cursorType = getPathType(cursorRequired, shouldDefineCursor);

  const paths = primitiveFields.map((f) => f.path);

  return createPortal(
    <FlexContainer
      gap="none"
      alignItems="center"
      className={classNames(styles.container, { [styles.active]: isActive, [styles.cloud]: isCloudApp() })}
      aria-hidden={!isActive}
    >
      <HeaderCell flex={0} className={classNames(styles.headerCell, styles.streamsCounterCell)}>
        <p className={classNames(styles.text, styles.streamsCountNumber)}>{numStreamsSelected}</p>
        <p className={classNames(styles.text, styles.streamsCountText)}>
          <FormattedMessage id="connection.streams" />
        </p>
      </HeaderCell>
      <HeaderCell flex={0} className={classNames(styles.headerCell, styles.syncCell)}>
        <p className={classNames(styles.text, styles.headerText)}>
          <FormattedMessage id="sources.sync" />
        </p>
        <div className={styles.syncCellContent}>
          <Switch
            variant="strong-blue"
            size="sm"
            checked={options.selected}
            onChange={() => onChangeOption({ selected: !options.selected })}
          />
        </div>
      </HeaderCell>
      <HeaderCell flex={1} className={styles.headerCell}>
        <p className={classNames(styles.text, styles.headerText)}>
          <FormattedMessage id="form.syncMode" />
        </p>
        <div className={styles.syncCellContent}>
          <SyncModeSelect
            className={styles.syncModeSelect}
            variant="strong-blue"
            value={{
              syncMode: options.syncMode,
              destinationSyncMode: options.destinationSyncMode,
            }}
            options={availableSyncModesOptions}
            onChange={({ value }) => onChangeOption({ ...value })}
          />
        </div>
      </HeaderCell>
      <HeaderCell flex={1} className={styles.headerCell}>
        <p className={classNames(styles.text, styles.headerText)}>
          <FormattedMessage id="form.cursorField" />
        </p>
        <div className={styles.syncCellContent}>
          <StreamPathSelect
            type="cursor"
            withSourceDefinedPill
            disabled={!cursorType}
            variant="strong-blue"
            isMulti={false}
            onPathChange={(path) => onChangeOption({ cursorField: path })}
            pathType={cursorType}
            paths={paths}
            path={options.cursorField}
          />
        </div>
      </HeaderCell>
      <HeaderCell flex={1} className={styles.headerCell}>
        <p className={classNames(styles.text, styles.headerText)}>
          <FormattedMessage id="form.primaryKey" />
        </p>
        <div className={styles.syncCellContent}>
          <StreamPathSelect
            type="primary-key"
            withSourceDefinedPill
            disabled={!pkType}
            variant="strong-blue"
            isMulti
            onPathChange={(path) => onChangeOption({ primaryKey: path })}
            pathType={pkType}
            paths={paths}
            path={options.primaryKey}
          />
        </div>
      </HeaderCell>
      <HeaderCell flex={0} className={styles.buttonCell}>
        <Button className={styles.cancelButton} size="xs" variant="secondary" onClick={onCancel}>
          <FormattedMessage id="connectionForm.bulkEdit.cancel" />
        </Button>
        <Button className={styles.applyButton} size="xs" onClick={onApply}>
          <FormattedMessage id="connectionForm.bulkEdit.apply" />
        </Button>
      </HeaderCell>
    </FlexContainer>,
    document.body
  );
};
