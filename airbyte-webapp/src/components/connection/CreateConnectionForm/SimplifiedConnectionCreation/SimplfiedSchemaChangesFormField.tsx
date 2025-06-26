import { ComponentProps, useMemo } from "react";
import { Controller, useFormContext } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { FormConnectionFormValues } from "components/connection/ConnectionForm/formConfig";
import { FormFieldLayout } from "components/connection/ConnectionForm/FormFieldLayout";
import { RadioButtonTiles } from "components/connection/CreateConnection/RadioButtonTiles";
import { ControlLabels } from "components/LabeledControl";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { ExternalLink } from "components/ui/Link";
import { ListBox } from "components/ui/ListBox";
import { Message } from "components/ui/Message";
import { Text } from "components/ui/Text";

import { NonBreakingChangesPreference } from "core/api/types/AirbyteClient";
import { useFormMode } from "core/services/ui/FormModeContext";
import { links } from "core/utils/links";

import { InputContainer } from "./InputContainer";

export const SimplfiedSchemaChangesFormField: React.FC<{ isCreating: boolean; disabled?: boolean }> = ({
  isCreating,
  disabled,
}) => {
  const { formatMessage } = useIntl();
  const { mode } = useFormMode();
  const { setValue, control } = useFormContext<FormConnectionFormValues>();

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
      label: formatMessage({ id: `connectionForm.nonBreakingChangesPreference.autopropagation.${value}` }),
      description: formatMessage({
        id: `connectionForm.nonBreakingChangesPreference.autopropagation.${value}.description`,
      }),
      "data-testid": value,
    }));
  }, [formatMessage]);

  return (
    <Controller
      name="nonBreakingChangesPreference"
      control={control}
      render={({ field, fieldState, formState }) => {
        const defaultValue = formState.defaultValues?.nonBreakingChangesPreference;
        const showAutoPropagationMessage =
          mode === "edit" &&
          fieldState.isDirty &&
          field.value !== defaultValue &&
          (field.value === "propagate_columns" || field.value === "propagate_fully");
        return (
          <FormFieldLayout alignItems="flex-start" nextSizing>
            <ControlLabels
              label={
                <FlexContainer direction="column" gap="sm">
                  <Text bold>
                    <FormattedMessage
                      id={
                        isCreating
                          ? "connectionForm.nonBreakingChangesPreference.autopropagation.labelCreating"
                          : "connectionForm.nonBreakingChangesPreference.autopropagation.label"
                      }
                    />
                  </Text>
                  <Text size="sm" color="grey">
                    <FormattedMessage
                      id="connectionForm.nonBreakingChangesPreference.autopropagation.message"
                      values={{
                        lnk: (children: React.ReactNode) => (
                          <ExternalLink href={links.schemaChangeManagement}>{children}</ExternalLink>
                        ),
                      }}
                    />
                  </Text>
                </FlexContainer>
              }
            />
            {isCreating ? (
              <RadioButtonTiles
                direction="column"
                name="nonBreakingChangesPreference"
                options={preferenceOptions}
                selectedValue={field.value ?? ""}
                onSelectRadioButton={(value) => setValue("nonBreakingChangesPreference", value, { shouldDirty: true })}
              />
            ) : (
              <InputContainer>
                <ListBox
                  isDisabled={disabled}
                  options={preferenceOptions}
                  onSelect={(value: NonBreakingChangesPreference) =>
                    setValue("nonBreakingChangesPreference", value, { shouldDirty: true })
                  }
                  selectedValue={field.value}
                  data-testid="nonBreakingChangesPreference"
                />
              </InputContainer>
            )}
            {showAutoPropagationMessage && (
              <Box mt="md">
                <Message
                  type="info"
                  text={
                    <FormattedMessage id="connectionForm.nonBreakingChangesPreference.autopropagation.messageOnChange" />
                  }
                />
              </Box>
            )}
          </FormFieldLayout>
        );
      }}
    />
  );
};
