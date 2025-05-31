import uniqueId from "lodash/uniqueId";
import { useState } from "react";
import { Controller, useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { FormConnectionFormValues } from "components/connection/ConnectionForm/formConfig";
import { FormFieldLayout } from "components/connection/ConnectionForm/FormFieldLayout";
import { ControlLabels } from "components/LabeledControl";
import { FlexContainer } from "components/ui/Flex";
import { Switch } from "components/ui/Switch";
import { Text } from "components/ui/Text";

import { SchemaChangeBackfillPreference } from "core/api/types/AirbyteClient";
import { useIsCloudApp } from "core/utils/app";

export const SimplifiedBackfillFormField: React.FC<{ disabled?: boolean }> = ({ disabled }) => {
  const { control } = useFormContext<FormConnectionFormValues>();
  const [controlId] = useState(`input-control-${uniqueId()}`);
  const isCloudApp = useIsCloudApp();

  return (
    <Controller
      name="backfillPreference"
      control={control}
      render={({ field }) => (
        <FormFieldLayout alignItems="flex-start" nextSizing>
          <ControlLabels
            htmlFor={controlId}
            label={
              <FlexContainer direction="column" gap="sm">
                <Text bold>
                  <FormattedMessage id="connectionForm.backfillColumns.title" />
                </Text>
                <Text size="sm" color="grey">
                  <FormattedMessage
                    id={
                      isCloudApp
                        ? "connectionForm.backfillColumns.descriptionCloud"
                        : "connectionForm.backfillColumns.description"
                    }
                  />
                </Text>
              </FlexContainer>
            }
          />
          <Switch
            id={controlId}
            checked={field.value === SchemaChangeBackfillPreference.enabled}
            onChange={(e) => {
              field.onChange(
                e.currentTarget.checked
                  ? SchemaChangeBackfillPreference.enabled
                  : SchemaChangeBackfillPreference.disabled
              );
            }}
            size="lg"
            disabled={disabled}
          />
        </FormFieldLayout>
      )}
    />
  );
};
