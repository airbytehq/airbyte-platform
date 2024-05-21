import React from "react";
import { useFormState } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { Tooltip } from "components/ui/Tooltip";

import { FormConnectionFormValues } from "../../formConfig";
import { useRefreshSourceSchemaWithConfirmationOnDirty } from "../../refreshSourceSchemaWithConfirmationOnDirty";

interface TableControlsProps {
  isAllRowsExpanded: boolean;
  toggleAllRowsExpanded: (expanded: boolean) => void;
}

export const TableControls: React.FC<TableControlsProps> = ({ isAllRowsExpanded, toggleAllRowsExpanded }) => {
  const { isDirty } = useFormState<FormConnectionFormValues>();
  const refreshSchema = useRefreshSourceSchemaWithConfirmationOnDirty(isDirty);

  return (
    <>
      <Tooltip
        placement="top"
        control={<Button variant="secondary" icon="rotate" type="button" onClick={refreshSchema} />}
      >
        <FormattedMessage id="connection.updateSchema" />
      </Tooltip>
      <Tooltip
        placement="top"
        control={
          <Button
            icon={isAllRowsExpanded ? "collapseAll" : "expandAll"}
            variant="secondary"
            type="button"
            onClick={() => toggleAllRowsExpanded(!isAllRowsExpanded)}
          />
        }
      >
        <FormattedMessage id={isAllRowsExpanded ? "tables.collapseAll" : "tables.expandAll"} />
      </Tooltip>
    </>
  );
};
