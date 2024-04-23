import uniqueId from "lodash/uniqueId";
import React, { useState } from "react";
import { Controller, useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { FormConnectionFormValues } from "components/connection/ConnectionForm/formConfig";
import { FormFieldLayout } from "components/connection/ConnectionForm/FormFieldLayout";
import { ControlLabels } from "components/LabeledControl";
import { FlexContainer } from "components/ui/Flex";
import { Switch } from "components/ui/Switch";
import { Text } from "components/ui/Text";

export const SimplifiedSchemaChangeNotificationFormField: React.FC<{ disabled?: boolean }> = ({ disabled }) => {
  const { control } = useFormContext<FormConnectionFormValues>();
  const [controlId] = useState(`input-control-${uniqueId()}`);

  return (
    <Controller
      name="notifySchemaChanges"
      control={control}
      render={({ field }) => (
        <FormFieldLayout alignItems="flex-start" nextSizing>
          <ControlLabels
            htmlFor={controlId}
            label={
              <FlexContainer direction="column" gap="sm">
                <Text bold>
                  <FormattedMessage id="connection.schemaUpdateNotifications.titleNext" />
                </Text>
                <Text size="sm" color="grey">
                  <FormattedMessage id="connection.schemaUpdateNotifications.subtitle" />
                </Text>
              </FlexContainer>
            }
          />
          <Switch id={controlId} checked={field.value} onChange={field.onChange} size="lg" disabled={disabled} />
        </FormFieldLayout>
      )}
    />
  );
};
