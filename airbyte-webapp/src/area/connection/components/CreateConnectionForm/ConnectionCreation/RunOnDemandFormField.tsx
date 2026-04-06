import uniqueId from "lodash/uniqueId";
import { useState } from "react";
import { Controller, useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { ControlLabels } from "components/ui/LabeledControl";
import { Switch } from "components/ui/Switch";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { FormConnectionFormValues } from "area/connection/components/ConnectionForm/formConfig";
import { FormFieldLayout } from "area/connection/components/ConnectionForm/FormFieldLayout";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

export const RunOnDemandFormField: React.FC<{ disabled?: boolean }> = ({ disabled }) => {
  const { control } = useFormContext<FormConnectionFormValues>();
  const [controlId] = useState(`input-control-${uniqueId()}`);
  const canManageOnDemand = useGeneratedIntent(Intent.ManageConnectionOnDemandCapacity);

  return (
    <Controller
      name="onDemandEnabled"
      control={control}
      render={({ field }) => (
        <FormFieldLayout alignItems="flex-start" nextSizing>
          <ControlLabels
            htmlFor={controlId}
            label={
              <FlexContainer direction="column" gap="sm">
                <Text bold>
                  <FormattedMessage id="connectionForm.runOnDemand.title" />
                </Text>
                <Text size="sm" color="grey">
                  <FormattedMessage id="connectionForm.runOnDemand.description" />
                </Text>
              </FlexContainer>
            }
          />
          {!canManageOnDemand ? (
            <Tooltip control={<Switch id={controlId} checked={field.value ?? false} size="lg" disabled />}>
              <FormattedMessage id="connectionForm.runOnDemand.adminOnly.tooltip" />
            </Tooltip>
          ) : (
            <Switch
              id={controlId}
              checked={field.value ?? false}
              onChange={field.onChange}
              size="lg"
              disabled={disabled}
            />
          )}
        </FormFieldLayout>
      )}
    />
  );
};
