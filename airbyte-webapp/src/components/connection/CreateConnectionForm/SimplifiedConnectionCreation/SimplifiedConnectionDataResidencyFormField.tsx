import uniqueId from "lodash/uniqueId";
import { useState } from "react";
import { Controller, useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { FormConnectionFormValues } from "components/connection/ConnectionForm/formConfig";
import { FormFieldLayout } from "components/connection/ConnectionForm/FormFieldLayout";
import { StandaloneDataResidencyDropdown } from "components/forms/DataResidencyDropdown";
import { ControlLabels } from "components/LabeledControl";
import { Text } from "components/ui/Text";

import { InputContainer } from "./InputContainer";

export const SimplfiedConnectionDataResidencyFormField: React.FC<{ disabled: boolean }> = ({ disabled }) => {
  const { control } = useFormContext<FormConnectionFormValues>();
  const [controlId] = useState(`input-control-${uniqueId()}`);

  return (
    <Controller
      name="geography"
      control={control}
      render={({ field }) => (
        <FormFieldLayout alignItems="flex-start" nextSizing>
          <ControlLabels
            htmlFor={controlId}
            label={
              <Text bold>
                <FormattedMessage id="connection.geographyTitle" />
              </Text>
            }
          />
          <InputContainer>
            <StandaloneDataResidencyDropdown<FormConnectionFormValues> name={field.name} disabled={disabled} />
          </InputContainer>
        </FormFieldLayout>
      )}
    />
  );
};
