import classNames from "classnames";
import React, { useMemo } from "react";
import { createPortal } from "react-dom";
import { FormattedMessage } from "react-intl";
import styled from "styled-components";

import { Cell } from "components";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Switch } from "components/ui/Switch";

import { SyncSchemaField } from "core/domain/catalog";
import { DestinationSyncMode, SyncMode } from "core/request/AirbyteClient";
import { useBulkEditService } from "hooks/services/BulkEdit/BulkEditService";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { isCloudApp } from "utils/app";

import styles from "./BulkEditPanel.module.scss";
import {
  calculateSelectedSyncMode,
  calculateSharedFields,
  calculateSyncSwitchState,
  getAvailableSyncModesOptions,
} from "./utils";
import { StreamPathSelect } from "../StreamPathSelect";
import { SyncModeSelect } from "../SyncModeSelect";
import { getFieldPathType } from "../utils";

export const HeaderCell = styled(Cell)`
  font-size: 10px;
  line-height: 13px;
`;

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

  const pkType = getFieldPathType(pkRequired, shouldDefinePk);
  const cursorType = getFieldPathType(cursorRequired, shouldDefineCursor);

  const paths = primitiveFields.map((f) => f.path);

  const { syncSwitchChecked, syncSwitchMixed } = useMemo(
    () => calculateSyncSwitchState(selectedBatchNodes, options),
    [selectedBatchNodes, options]
  );

  const selectedSyncMode = useMemo(
    () => calculateSelectedSyncMode(selectedBatchNodes, options),
    [options, selectedBatchNodes]
  );

  const onChangeSyncSwitch = () => {
    const isChecked = "selected" in options ? options.selected : syncSwitchChecked;
    onChangeOption({ selected: !isChecked });
  };

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
            indeterminate={syncSwitchMixed}
            checked={syncSwitchChecked}
            onChange={onChangeSyncSwitch}
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
            value={selectedSyncMode}
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
