import { ComponentProps, useMemo } from "react";
import { Controller, useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { FormConnectionFormValues } from "components/connection/ConnectionForm/formConfig";
import { FormFieldLayout } from "components/connection/ConnectionForm/FormFieldLayout";
import { RadioButtonTiles } from "components/connection/CreateConnection/RadioButtonTiles";
import { ControlLabels } from "components/LabeledControl";
import { Message } from "components/ui/Message";
import { Text } from "components/ui/Text";

import { NonBreakingChangesPreference } from "core/api/types/AirbyteClient";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";

export const SimplfiedSchemaChangesFormField = () => {
  const { connection, mode } = useConnectionFormService();
  const { setValue, control } = useFormContext<FormConnectionFormValues>();

  const watchedNonBreakingChangesPreference = useWatch<FormConnectionFormValues>({
    name: "nonBreakingChangesPreference",
  });

  const showAutoPropagationMessage =
    mode === "edit" &&
    watchedNonBreakingChangesPreference !== connection.nonBreakingChangesPreference &&
    (watchedNonBreakingChangesPreference === "propagate_columns" ||
      watchedNonBreakingChangesPreference === "propagate_fully");

  const preferenceOptions = useMemo<
    ComponentProps<typeof RadioButtonTiles<NonBreakingChangesPreference>>["options"]
  >(() => {
    const supportedPreferences = [
      NonBreakingChangesPreference.propagate_columns,
      NonBreakingChangesPreference.propagate_fully,
      NonBreakingChangesPreference.ignore,
      NonBreakingChangesPreference.disable,
    ];
    return supportedPreferences.map((value) => ({
      value,
      label: `connectionForm.nonBreakingChangesPreference.autopropagation.${value}.next`,
      description: `connectionForm.nonBreakingChangesPreference.autopropagation.${value}.description`,
      "data-testid": value,
    }));
  }, []);

  return (
    <Controller
      name="nonBreakingChangesPreference"
      control={control}
      render={({ field }) => (
        <FormFieldLayout alignItems="flex-start" nextSizing>
          <ControlLabels
            label={
              <Text bold>
                <FormattedMessage id="connectionForm.nonBreakingChangesPreference.autopropagation.labelNext" />
              </Text>
            }
          />
          <RadioButtonTiles
            direction="column"
            name="nonBreakingChangesPreference"
            options={preferenceOptions}
            selectedValue={field.value ?? ""}
            onSelectRadioButton={(value) => setValue("nonBreakingChangesPreference", value, { shouldDirty: true })}
          />
          {showAutoPropagationMessage && (
            <Message
              type="info"
              text={<FormattedMessage id="connectionForm.nonBreakingChangesPreference.autopropagation.message" />}
            />
          )}
        </FormFieldLayout>
      )}
    />
  );
};
