import React from "react";
import { useFormState } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { Tooltip } from "components/ui/Tooltip";

import { useFormMode } from "core/services/ui/FormModeContext";

import { FormConnectionFormValues } from "../../ConnectionForm/formConfig";
import { useRefreshSourceSchemaWithConfirmationOnDirty } from "../../ConnectionForm/refreshSourceSchemaWithConfirmationOnDirty";

export const RefreshSchemaControl: React.FC = () => {
  const { mode } = useFormMode();
  const { isDirty } = useFormState<FormConnectionFormValues>();
  const refreshSchema = useRefreshSourceSchemaWithConfirmationOnDirty(isDirty);

  return (
    <Tooltip
      placement="top"
      control={
        <Button
          variant="secondary"
          icon="rotate"
          type="button"
          onClick={refreshSchema}
          disabled={mode === "readonly"}
          data-testid="refresh-schema-btn"
        />
      }
    >
      <FormattedMessage id="connection.updateSchema" />
    </Tooltip>
  );
};
