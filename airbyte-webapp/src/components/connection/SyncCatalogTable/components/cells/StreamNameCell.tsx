import { Row } from "@tanstack/react-table";
import React from "react";
import { FormattedMessage } from "react-intl";

import { Badge } from "components/ui/Badge";
import { Button } from "components/ui/Button";
import { CheckBox } from "components/ui/CheckBox";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { TextWithOverflowTooltip } from "components/ui/Text";
import { TextHighlighter } from "components/ui/TextHighlighter";
import { Tooltip } from "components/ui/Tooltip";

import { AirbyteStreamConfiguration } from "core/api/types/AirbyteClient";
import { useFormMode } from "core/services/ui/FormModeContext";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";

import styles from "./StreamNameCell.module.scss";
import { SyncStreamFieldWithId } from "../../../ConnectionForm/formConfig";
import { SyncCatalogUIModel } from "../../SyncCatalogTable";

interface StreamNameCellProps {
  value: string;
  row: Row<SyncCatalogUIModel>;
  updateStreamField: (streamNode: SyncStreamFieldWithId, updatedConfig: Partial<AirbyteStreamConfiguration>) => void;
  globalFilterValue?: string;
  destinationName: string;
  destinationSupportsFileTransfer: boolean;
}

export const StreamNameCell: React.FC<StreamNameCellProps> = ({
  value,
  row,
  updateStreamField,
  globalFilterValue = "",
  destinationName,
  destinationSupportsFileTransfer,
}) => {
  const { connection } = useConnectionFormService();
  const { mode } = useFormMode();

  if (!row.original.streamNode) {
    return null;
  }

  const { config, stream } = row.original.streamNode;
  const prefix = connection.prefix || "";
  const isFileBased = stream?.isFileBased ?? false;
  const isUnsupportedFileBasedStream = isFileBased && !destinationSupportsFileTransfer;

  const displayValue = prefix ? (
    <>
      <b>{prefix}</b>
      {value.startsWith(prefix) ? value.replace(prefix, "") : value}
    </>
  ) : (
    value
  );

  // expand stream and field rows
  const onToggleExpand = () => {
    row.getToggleExpandedHandler()();
    if (!row.subRows) {
      return;
    }
    row.subRows.forEach((field) => {
      if (!field.getCanExpand()) {
        return;
      }
      field.getToggleExpandedHandler()();
    });
  };

  const checkBox = (
    <CheckBox
      checkboxSize="sm"
      checked={config?.selected}
      onChange={({ target: { checked } }) =>
        updateStreamField(row.original.streamNode!, {
          selected: checked,
          // enable/disable stream will enable/disable all fields
          fieldSelectionEnabled: false,
          selectedFields: [],
        })
      }
      data-testid="sync-stream-checkbox"
      disabled={mode === "readonly" || isUnsupportedFileBasedStream}
    />
  );

  return (
    <FlexContainer gap="none" alignItems="center">
      {isUnsupportedFileBasedStream ? (
        <Tooltip placement="top" control={checkBox}>
          <FormattedMessage id="connectionForm.fileStream.tooltip.unsupported" values={{ destinationName }} />
        </Tooltip>
      ) : (
        checkBox
      )}
      <Button
        type="button"
        icon={row.getIsExpanded() ? "chevronDown" : "chevronRight"}
        variant="clear"
        onClick={onToggleExpand}
        disabled={!row.getCanExpand()}
        data-testid="expand-collapse-stream-btn"
        aria-expanded={row.getIsExpanded()}
      />
      <FlexContainer alignItems="center" {...(isUnsupportedFileBasedStream && { className: styles.disabled })}>
        <TextWithOverflowTooltip>
          {globalFilterValue ? (
            <TextHighlighter searchWords={[globalFilterValue]} textToHighlight={value} />
          ) : (
            displayValue
          )}
        </TextWithOverflowTooltip>
        {isFileBased && (
          <Tooltip
            placement="top"
            className={styles.tooltip}
            control={
              <Badge variant="blue" uppercase={false}>
                <FlexContainer gap="xs" alignItems="center">
                  <Icon type="file" size="sm" />
                  <FormattedMessage id="connectionForm.fileStream.badge" />
                </FlexContainer>
              </Badge>
            }
          >
            <FormattedMessage id="connectionForm.fileStream.tooltip.supported" />
            <br />
            <br />
            <FormattedMessage id="connectionForm.fileStream.tooltip.supported.note" />
          </Tooltip>
        )}
      </FlexContainer>
    </FlexContainer>
  );
};
