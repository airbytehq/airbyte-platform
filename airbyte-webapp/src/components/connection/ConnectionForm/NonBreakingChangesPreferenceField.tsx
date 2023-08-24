import { FieldProps } from "formik";
import { useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { ControlLabels } from "components";
import { DropDown } from "components/ui/DropDown";
import { Message } from "components/ui/Message";

import { NonBreakingChangesPreference } from "core/request/AirbyteClient";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useExperiment } from "hooks/services/Experiment";

import { FormFieldLayout } from "./FormFieldLayout";

export const NonBreakingChangesPreferenceField: React.FC<FieldProps<string>> = ({ field, form }) => {
  const { connection, mode } = useConnectionFormService();
  const autoPropagationEnabled = useExperiment("autopropagation.enabled", true);
  const autoPropagationPrefix = autoPropagationEnabled ? "autopropagation." : "";
  const labelKey = autoPropagationEnabled
    ? "connectionForm.nonBreakingChangesPreference.autopropagation.label"
    : "connectionForm.nonBreakingChangesPreference.label";

  const showAutoPropagationMessage =
    mode === "edit" &&
    field.value !== connection.nonBreakingChangesPreference &&
    (field.value === "propagate_columns" || field.value === "propagate_fully");

  const supportedPreferences = useMemo(() => {
    if (autoPropagationEnabled) {
      return [
        NonBreakingChangesPreference.ignore,
        NonBreakingChangesPreference.disable,
        NonBreakingChangesPreference.propagate_columns,
        NonBreakingChangesPreference.propagate_fully,
      ];
    }
    return [NonBreakingChangesPreference.ignore, NonBreakingChangesPreference.disable];
  }, [autoPropagationEnabled]);

  const { formatMessage } = useIntl();

  const preferenceOptions = useMemo(() => {
    return supportedPreferences.map((value) => ({
      value,
      label: formatMessage({ id: `connectionForm.nonBreakingChangesPreference.${autoPropagationPrefix}${value}` }),
      testId: `nonBreakingChangesPreference-${value}`,
    }));
  }, [formatMessage, supportedPreferences, autoPropagationPrefix]);

  return (
    <>
      <FormFieldLayout>
        <ControlLabels
          nextLine
          label={formatMessage({
            id: labelKey,
          })}
          infoTooltipContent={formatMessage({
            id: "connectionForm.nonBreakingChangesPreference.message",
          })}
        />
        <DropDown
          {...field}
          options={preferenceOptions}
          error={form.touched[field.name] && !!form.errors[field.name]}
          data-testid="nonBreakingChangesPreference"
          value={field.value}
          isDisabled={form.isSubmitting || mode === "readonly"}
          onChange={({ value }) => form.setFieldValue(field.name, value)}
        />
      </FormFieldLayout>
      {showAutoPropagationMessage && (
        <Message
          type="info"
          text={<FormattedMessage id="connectionForm.nonBreakingChangesPreference.autopropagtion.message" />}
        />
      )}
    </>
  );
};
