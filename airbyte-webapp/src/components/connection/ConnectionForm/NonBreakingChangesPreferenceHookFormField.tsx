import { useMemo } from "react";
import { useWatch } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { FormControl } from "components/forms";
import { Message } from "components/ui/Message";

import { NonBreakingChangesPreference } from "core/request/AirbyteClient";
import { useConnectionHookFormService } from "hooks/services/ConnectionForm/ConnectionHookFormService";
import { useExperiment } from "hooks/services/Experiment";

import { HookFormConnectionFormValues } from "./hookFormConfig";
import { HookFormFieldLayout } from "./HookFormFieldLayout";

export const NonBreakingChangesPreferenceHookFormField = () => {
  const { formatMessage } = useIntl();
  const { connection, mode } = useConnectionHookFormService();
  const autoPropagationEnabled = useExperiment("autopropagation.enabled", true);
  const autoPropagationPrefix = autoPropagationEnabled ? "autopropagation." : "";
  const labelKey = autoPropagationEnabled
    ? "connectionForm.nonBreakingChangesPreference.autopropagation.label"
    : "connectionForm.nonBreakingChangesPreference.label";

  const watchedNonBreakingChangesPreference = useWatch<HookFormConnectionFormValues>({
    name: "nonBreakingChangesPreference",
  });

  const showAutoPropagationMessage =
    mode === "edit" &&
    watchedNonBreakingChangesPreference !== connection.nonBreakingChangesPreference &&
    (watchedNonBreakingChangesPreference === "propagate_columns" ||
      watchedNonBreakingChangesPreference === "propagate_fully");

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

  const preferenceOptions = useMemo(() => {
    return supportedPreferences.map((value) => ({
      value,
      label: formatMessage({ id: `connectionForm.nonBreakingChangesPreference.${autoPropagationPrefix}${value}` }),
      testId: `nonBreakingChangesPreference-${value}`,
    }));
  }, [formatMessage, supportedPreferences, autoPropagationPrefix]);

  return (
    <HookFormFieldLayout>
      <FormControl
        name="nonBreakingChangesPreference"
        fieldType="dropdown"
        label={formatMessage({
          id: labelKey,
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
          text={<FormattedMessage id="connectionForm.nonBreakingChangesPreference.autopropagtion.message" />}
        />
      )}
    </HookFormFieldLayout>
  );
};
