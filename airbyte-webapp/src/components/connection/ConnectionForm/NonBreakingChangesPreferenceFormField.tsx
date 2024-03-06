import { useMemo } from "react";
import { useWatch } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { FormControl } from "components/forms";
import { Message } from "components/ui/Message";

import { NonBreakingChangesPreference } from "core/api/types/AirbyteClient";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";

import { CardFormFieldLayout } from "./CardFormFieldLayout";
import { FormConnectionFormValues } from "./formConfig";

export const NonBreakingChangesPreferenceFormField = () => {
  const { formatMessage } = useIntl();
  const { connection, mode } = useConnectionFormService();

  const watchedNonBreakingChangesPreference = useWatch<FormConnectionFormValues>({
    name: "nonBreakingChangesPreference",
  });

  const showAutoPropagationMessage =
    mode === "edit" &&
    watchedNonBreakingChangesPreference !== connection.nonBreakingChangesPreference &&
    (watchedNonBreakingChangesPreference === "propagate_columns" ||
      watchedNonBreakingChangesPreference === "propagate_fully");

  const preferenceOptions = useMemo(() => {
    const supportedPreferences = [
      NonBreakingChangesPreference.propagate_columns,
      NonBreakingChangesPreference.propagate_fully,
      NonBreakingChangesPreference.ignore,
      NonBreakingChangesPreference.disable,
    ];
    return supportedPreferences.map((value) => ({
      value,
      label: formatMessage({ id: `connectionForm.nonBreakingChangesPreference.autopropagation.${value}` }),
      "data-testid": value,
    }));
  }, [formatMessage]);

  return (
    <CardFormFieldLayout>
      <FormControl
        name="nonBreakingChangesPreference"
        fieldType="dropdown"
        label={formatMessage({
          id: "connectionForm.nonBreakingChangesPreference.autopropagation.label",
        })}
        labelTooltip={formatMessage({
          id: "connectionForm.nonBreakingChangesPreference.message",
        })}
        options={preferenceOptions}
        inline
        data-testid="nonBreakingChangesPreference"
      />
      {showAutoPropagationMessage && (
        <Message
          type="info"
          text={<FormattedMessage id="connectionForm.nonBreakingChangesPreference.autopropagation.message" />}
        />
      )}
    </CardFormFieldLayout>
  );
};
