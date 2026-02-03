import uniqueId from "lodash/uniqueId";
import { useState } from "react";
import { Controller, useFormContext } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Input } from "components/ui/Input";
import { ControlLabels } from "components/ui/LabeledControl";
import { Text } from "components/ui/Text";

import { FormConnectionFormValues } from "area/connection/components/ConnectionForm/formConfig";
import { FormFieldLayout } from "area/connection/components/ConnectionForm/FormFieldLayout";

import { InputContainer } from "./InputContainer";

export const SimplifiedConnectionNameFormField = () => {
  const { formatMessage } = useIntl();
  const { control } = useFormContext<FormConnectionFormValues>();
  const [controlId] = useState(`input-control-${uniqueId()}`);

  return (
    <Controller
      name="name"
      control={control}
      render={({ field, fieldState }) => (
        <FormFieldLayout alignItems="flex-start" nextSizing>
          <ControlLabels
            htmlFor={controlId}
            label={
              <FlexContainer direction="column" gap="sm">
                <Text bold>
                  <FormattedMessage id="form.connectionName" />
                </Text>
                <Text size="sm" color="grey">
                  <FormattedMessage id="form.connectionName.subtitle" />
                </Text>
              </FlexContainer>
            }
          />
          <InputContainer>
            <Input
              id={controlId}
              name="name"
              placeholder={formatMessage({ id: "form.connectionName.placeholder" })}
              inline={false}
              value={field.value}
              onChange={field.onChange}
              data-testid="connectionName"
            />
          </InputContainer>
          {fieldState.error && (
            <Box mt="sm">
              <Text color="red">
                <FormattedMessage id={fieldState.error.message} />
              </Text>
            </Box>
          )}
        </FormFieldLayout>
      )}
    />
  );
};
