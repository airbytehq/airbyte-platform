import { Row } from "@tanstack/react-table";
import isEqual from "lodash/isEqual";
import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { CheckBox } from "components/ui/CheckBox";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text, TextWithOverflowTooltip } from "components/ui/Text";
import { TextHighlighter } from "components/ui/TextHighlighter";
import { Tooltip } from "components/ui/Tooltip";

import { getDataType } from "area/connection/utils";
import { AirbyteStreamConfiguration, DestinationSyncMode, SyncMode } from "core/api/types/AirbyteClient";
import { SyncSchemaFieldObject } from "core/domain/catalog";
import { useFormMode } from "core/services/ui/FormModeContext";
import { useExperiment } from "hooks/services/Experiment";

import styles from "./StreamFieldNameCell.module.scss";
import { SyncStreamFieldWithId } from "../../../ConnectionForm/formConfig";
import { SyncCatalogUIModel } from "../../SyncCatalogTable";
import {
  isChildFieldCursor as checkIsChildFieldCursor,
  isChildFieldPrimaryKey as checkIsChildFieldPrimaryKey,
  isCursor as checkIsCursor,
  isPrimaryKey as checkIsPrimaryKey,
  getFieldPathDisplayName,
  checkIsFieldSelected,
  checkIsFieldHashed,
  getSelectedMandatoryFields,
  updateFieldSelected,
} from "../../utils";

interface StreamFieldNameCellProps {
  row: Row<SyncCatalogUIModel>;
  updateStreamField: (streamNode: SyncStreamFieldWithId, updatedConfig: Partial<AirbyteStreamConfiguration>) => void;
  globalFilterValue?: string;
  destinationSupportsFileTransfer: boolean;
}

export const StreamFieldNameCell: React.FC<StreamFieldNameCellProps> = ({
  row,
  updateStreamField,
  globalFilterValue = "",
  destinationSupportsFileTransfer,
}) => {
  const isMappingsUIEnabled = useExperiment("connection.mappingsUI");
  const isColumnSelectionEnabled = useExperiment("connection.columnSelection");
  const { formatMessage } = useIntl();
  const { mode } = useFormMode();

  if (!row.original.streamNode) {
    return null;
  }

  const {
    streamNode: { config, stream },
    field,
    traversedFields,
  } = row.original;

  if (!field || !config || !stream || !traversedFields) {
    return null;
  }
  const isNestedField = SyncSchemaFieldObject.isNestedField(field);
  const isCursor = checkIsCursor(config, field.path);
  const isChildFieldCursor = checkIsChildFieldCursor(config, field.path);
  const isPrimaryKey = checkIsPrimaryKey(config, field.path);
  const isChildFieldPrimaryKey = checkIsChildFieldPrimaryKey(config, field.path);
  const isHashed = checkIsFieldHashed(field, config);

  const isDisabled =
    config?.selected &&
    ((config.syncMode === SyncMode.incremental && (isCursor || isChildFieldCursor)) ||
      (config.destinationSyncMode === DestinationSyncMode.append_dedup && (isPrimaryKey || isChildFieldPrimaryKey)) ||
      (config.destinationSyncMode === DestinationSyncMode.overwrite_dedup && (isPrimaryKey || isChildFieldPrimaryKey)));

  const isUnsupportedFileBasedStream = stream?.isFileBased && !destinationSupportsFileTransfer;

  const showTooltip = isDisabled && mode !== "readonly";

  const isFieldSelected = checkIsFieldSelected(field, config);

  const renderDisabledReasonMessage = () => {
    if (isPrimaryKey || isChildFieldPrimaryKey) {
      return <FormattedMessage id="form.field.sync.primaryKeyTooltip" />;
    }
    if (isCursor || isChildFieldCursor) {
      return <FormattedMessage id="form.field.sync.cursorFieldTooltip" />;
    }
    return null;
  };

  const onToggleFieldSelected = (fieldPath: string[], isSelected: boolean) => {
    if (!row.original.streamNode) {
      return;
    }

    const numberOfFieldsInStream = Object.keys(stream?.jsonSchema?.properties ?? {}).length ?? 0;
    const updatedConfig = updateFieldSelected({
      config,
      fields: traversedFields,
      fieldPath,
      isSelected,
      numberOfFieldsInStream,
    });

    const mandatorySelectedFields = getSelectedMandatoryFields(config);

    updateStreamField(row.original.streamNode, {
      ...updatedConfig,
      // any field selection immediately enables the disabled stream
      ...(isSelected && !config?.selected && { selected: true }),
      // disable the stream if all fields are unselected
      ...(updatedConfig?.selectedFields?.length === 0 && updatedConfig?.fieldSelectionEnabled && { selected: false }),
      selectedFields: !updatedConfig?.fieldSelectionEnabled
        ? []
        : [...(updatedConfig?.selectedFields ?? []), ...mandatorySelectedFields],
      // remove this field if it was part of hashedFields
      hashedFields: config.hashedFields?.filter((f) => !isEqual(f.fieldPath, fieldPath)),
    });
  };

  return (
    <FlexContainer alignItems="center" {...(isUnsupportedFileBasedStream && { className: styles.disabled })}>
      <FlexContainer alignItems="center" gap="xs">
        {isNestedField && <Icon type="nested" color="disabled" size="lg" />}
        {!showTooltip && !isNestedField && isColumnSelectionEnabled && (
          <CheckBox
            checkboxSize="sm"
            checked={isFieldSelected}
            disabled={isDisabled || isUnsupportedFileBasedStream || mode === "readonly"}
            onChange={() => onToggleFieldSelected(field.path, !isFieldSelected)}
            data-testid="sync-field-checkbox"
          />
        )}
        {showTooltip && !isNestedField && isColumnSelectionEnabled && (
          <Tooltip
            control={
              <FlexContainer alignItems="center">
                <CheckBox
                  checkboxSize="sm"
                  disabled
                  checked={isFieldSelected}
                  readOnly
                  data-testid="sync-field-checkbox"
                />
              </FlexContainer>
            }
          >
            {renderDisabledReasonMessage()}
          </Tooltip>
        )}
      </FlexContainer>
      <TextWithOverflowTooltip size="sm">
        {isHashed && !isMappingsUIEnabled ? (
          <>
            {getFieldPathDisplayName(field.path)}
            <Text as="span" bold>
              _hashed
            </Text>
          </>
        ) : (
          <TextHighlighter searchWords={[globalFilterValue]} textToHighlight={getFieldPathDisplayName(field.path)} />
        )}
      </TextWithOverflowTooltip>
      <Text size="sm" color="grey300" bold={isHashed && !isMappingsUIEnabled}>
        <FormattedMessage
          id={isHashed && !isMappingsUIEnabled ? "airbyte.datatype.string" : `${getDataType(field)}`}
          defaultMessage={formatMessage({ id: "airbyte.datatype.unknown" })}
        />
      </Text>
    </FlexContainer>
  );
};
