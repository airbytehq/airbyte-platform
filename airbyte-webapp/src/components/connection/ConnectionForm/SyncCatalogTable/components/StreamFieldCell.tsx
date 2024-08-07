import { Row } from "@tanstack/react-table";
import classnames from "classnames";
import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { CheckBox } from "components/ui/CheckBox";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text, TextWithOverflowTooltip } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { getDataType } from "area/connection/utils";
import { AirbyteStreamConfiguration, DestinationSyncMode, SyncMode } from "core/api/types/AirbyteClient";
import { SyncSchemaFieldObject } from "core/domain/catalog";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useExperiment } from "hooks/services/Experiment";

import { GlobalFilterHighlighter } from "./GlobalFilterHighlighter";
import styles from "./StreamFieldCell.module.scss";
import {
  isChildFieldCursor as checkIsChildFieldCursor,
  isChildFieldPrimaryKey as checkIsChildFieldPrimaryKey,
  isCursor as checkIsCursor,
  isPrimaryKey as checkIsPrimaryKey,
} from "../../../syncCatalog/StreamFieldsTable/StreamFieldsTable";
import { updateFieldSelected } from "../../../syncCatalog/SyncCatalog/streamConfigHelpers";
import { getFieldPathDisplayName } from "../../../syncCatalog/utils";
import { SyncStreamFieldWithId } from "../../formConfig";
import { SyncCatalogUIModel } from "../SyncCatalogTable";
import { checkIsFieldSelected } from "../utils";

interface StreamFieldNameCellProps {
  row: Row<SyncCatalogUIModel>;
  updateStreamField: (streamNode: SyncStreamFieldWithId, updatedConfig: Partial<AirbyteStreamConfiguration>) => void;
  globalFilterValue?: string;
}

export const StreamFieldNameCell: React.FC<StreamFieldNameCellProps> = ({
  row,
  updateStreamField,
  globalFilterValue = "",
}) => {
  const isColumnSelectionEnabled = useExperiment("connection.columnSelection", true);
  const { formatMessage } = useIntl();
  const { mode } = useConnectionFormService();

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

  const isDisabled =
    !config?.selected ||
    mode === "readonly" ||
    (config.syncMode === SyncMode.incremental && (isCursor || isChildFieldCursor)) ||
    (config.destinationSyncMode === DestinationSyncMode.append_dedup && (isPrimaryKey || isChildFieldPrimaryKey)) ||
    (config.destinationSyncMode === DestinationSyncMode.overwrite_dedup && (isPrimaryKey || isChildFieldPrimaryKey)) ||
    isNestedField;
  const showTooltip = isDisabled && mode !== "readonly" && config?.selected;

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
    const numberOfFieldsInStream = Object.keys(stream?.jsonSchema?.properties ?? {}).length ?? 0;
    const updatedConfig = updateFieldSelected({
      config,
      fields: traversedFields,
      fieldPath,
      isSelected,
      numberOfFieldsInStream,
    });

    updateStreamField(row.original.streamNode, {
      ...updatedConfig,
      selectedFields: !updatedConfig?.fieldSelectionEnabled ? [] : updatedConfig?.selectedFields,
    });
  };

  return (
    <Box className={classnames(styles.secondDepth, { [styles.thirdDepth]: isNestedField })}>
      <FlexContainer alignItems="center">
        <FlexContainer alignItems="center" gap="xs">
          {isNestedField && <Icon type="nested" color="disabled" size="lg" />}
          {!showTooltip && !isNestedField && isColumnSelectionEnabled && (
            <CheckBox
              checkboxSize="sm"
              checked={isFieldSelected}
              disabled={isDisabled}
              onChange={() => onToggleFieldSelected(field.path, !isFieldSelected)}
              data-testid="sync-field-checkbox"
            />
          )}
          {showTooltip && !isNestedField && (
            <Tooltip
              control={
                <FlexContainer alignItems="center">
                  <CheckBox checkboxSize="sm" disabled checked={isFieldSelected} readOnly />
                </FlexContainer>
              }
            >
              {renderDisabledReasonMessage()}
            </Tooltip>
          )}
        </FlexContainer>
        <TextWithOverflowTooltip size="sm">
          <GlobalFilterHighlighter
            searchWords={[globalFilterValue]}
            textToHighlight={getFieldPathDisplayName(field.path)}
          />
        </TextWithOverflowTooltip>
        <Text size="sm" color="grey300">
          <FormattedMessage
            id={`${getDataType(field)}`}
            defaultMessage={formatMessage({ id: "airbyte.datatype.unknown" })}
          />
        </Text>
      </FlexContainer>
    </Box>
  );
};
