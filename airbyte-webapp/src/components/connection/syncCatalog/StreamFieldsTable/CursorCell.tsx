import { CellContext } from "@tanstack/react-table";
import React from "react";
import { FormattedMessage } from "react-intl";

import { RadioButton } from "components/ui/RadioButton";
import { Tooltip, TooltipLearnMoreLink } from "components/ui/Tooltip";

import { links } from "core/utils/links";

import styles from "./CursorCell.module.scss";
import { TableStream } from "./StreamFieldsTable";

interface CursorCellProps extends CellContext<TableStream, boolean | undefined> {
  isCursorDefinitionSupported: boolean;
  isCursor: (path: string[]) => boolean;
  onCursorSelect: (cursorPath: string[]) => void;
}

export const CursorCell: React.FC<CursorCellProps> = ({
  getValue,
  row,
  isCursorDefinitionSupported,
  isCursor,
  onCursorSelect,
}) => {
  if (!isCursorDefinitionSupported) {
    return null;
  }

  const isSelectedCursor = isCursor(row.original.path);

  const radioButton = (
    <RadioButton
      className={styles.radio}
      checked={isSelectedCursor}
      onChange={() => onCursorSelect(row.original.path)}
      disabled={!getValue()}
      data-testid="field-cursor-radio-button"
    />
  );

  return isSelectedCursor && !getValue() ? (
    <Tooltip placement="bottom" control={radioButton} containerClassName={styles.tooltip}>
      <FormattedMessage id="form.field.sourceDefinedCursor" />
      <TooltipLearnMoreLink url={links.sourceDefinedCursorLink} />
    </Tooltip>
  ) : (
    radioButton
  );
};
