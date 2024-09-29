import uniqueId from "lodash/uniqueId";
import { useState } from "react";
import { Controller, useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { FormConnectionFormValues } from "components/connection/ConnectionForm/formConfig";
import { FormFieldLayout } from "components/connection/ConnectionForm/FormFieldLayout";
import { ControlLabels } from "components/LabeledControl";
import { FlexContainer } from "components/ui/Flex";
import { Input } from "components/ui/Input";
import { Text } from "components/ui/Text";

import { InputContainer } from "./InputContainer";

export const SimplifiedDestinationStreamPrefixNameFormField: React.FC<{ disabled?: boolean }> = ({ disabled }) => {
  const { formatMessage } = useIntl();
  const { control } = useFormContext<FormConnectionFormValues>();
  const [controlId] = useState(`input-control-${uniqueId()}`);
  const prefix = useWatch({ name: "prefix", control });

  return (
    <Controller
      name="prefix"
      control={control}
      render={({ field }) => (
        <FormFieldLayout alignItems="flex-start" nextSizing data-testid="stream-prefix">
          <ControlLabels
            htmlFor={controlId}
            label={
              <FlexContainer direction="column" gap="sm">
                <Text bold>
                  <FormattedMessage id="form.prefix" />
                  &nbsp;
                  <Text as="span" size="sm" color="grey" italicized>
                    <FormattedMessage id="form.optional" />
                  </Text>
                </Text>
                <Text size="sm" color="grey">
                  <FormattedMessage id="form.prefix.subtitle" />
                </Text>
              </FlexContainer>
            }
          />
          <InputContainer>
            <Input
              id={controlId}
              name="prefix"
              placeholder={formatMessage({ id: "connectionForm.modal.destinationStreamNames.input.placeholderNext" })}
              inline={false}
              value={field.value}
              onChange={field.onChange}
              disabled={disabled}
              data-testid="stream-prefix-input"
            />
          </InputContainer>
          {prefix && (
            <Text data-testid="stream-prefix-preview">
              <FormattedMessage id="form.prefix.example" values={{ prefix }} />
            </Text>
          )}
        </FormFieldLayout>
      )}
    />
  );
};
