import { Listbox } from "@headlessui/react";
import React, { useMemo } from "react";
import { Controller, useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { FormControlErrorMessage } from "components/forms/FormControl";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Option } from "components/ui/ListBox";
import { FloatLayout } from "components/ui/ListBox/FloatLayout";
import { ListboxButton } from "components/ui/ListBox/ListboxButton";
import { ListboxOption } from "components/ui/ListBox/ListboxOption";
import { ListboxOptions } from "components/ui/ListBox/ListboxOptions";
import { Text } from "components/ui/Text";

import { DataActivationConnectionFormValues } from "area/dataActivation/types";
import { getDestinationOperation } from "area/dataActivation/utils/getDestinationOperation";
import { getDestinationOperationFields } from "area/dataActivation/utils/getDestinationOperationFields";
import { getRequiredFields } from "area/dataActivation/utils/getRequiredFields";
import { DestinationCatalog, DestinationSyncMode } from "core/api/types/AirbyteClient";

interface SelectDestinationSyncModeProps {
  streamIndex: number;
  destinationCatalog: DestinationCatalog;
}

export const SelectDestinationSyncMode: React.FC<SelectDestinationSyncModeProps> = ({
  destinationCatalog,
  streamIndex,
}) => {
  const { control, setValue } = useFormContext<DataActivationConnectionFormValues>();
  const { formatMessage } = useIntl();

  const destinationObjectName = useWatch<DataActivationConnectionFormValues, `streams.${number}.destinationObjectName`>(
    {
      name: `streams.${streamIndex}.destinationObjectName`,
    }
  );

  const fields = useWatch<DataActivationConnectionFormValues, `streams.${number}.fields`>({
    name: `streams.${streamIndex}.fields`,
  });

  const availableOperations = useMemo(() => {
    return destinationCatalog.operations.filter((operation) => operation.objectName === destinationObjectName);
  }, [destinationObjectName, destinationCatalog]);

  const destinationSyncModeOptions: Array<Option<DestinationSyncMode>> = [
    { label: formatMessage({ id: "connection.dataActivation.append" }), value: DestinationSyncMode.append },
    {
      label: formatMessage({ id: "connection.dataActivation.append_dedup" }),
      value: DestinationSyncMode.append_dedup,
    },
    {
      label: formatMessage({ id: "connection.dataActivation.update" }),
      value: DestinationSyncMode.update,
    },
    {
      label: formatMessage({ id: "connection.dataActivation.soft_delete" }),
      value: DestinationSyncMode.soft_delete,
    },
  ].map((option) => ({
    ...option,
    disabled: !availableOperations.some((operation) => operation.syncMode === option.value),
  }));

  // When the destination object changes, we need to do a few things:
  // 1. Make sure any existing field mappings are still valid for the new destination object.
  // 2. Make sure any required fields for the new destination object are added to the form.
  const updateFieldMappings = (value: DestinationSyncMode) => {
    const selectedOperation = getDestinationOperation(availableOperations, destinationObjectName, value);

    if (!selectedOperation) {
      return;
    }

    // 1. Validate existing field mappings
    const availableFields = getDestinationOperationFields(selectedOperation).map(([key]) => key);
    const existingFieldMappings = (fields ?? []).map((field) => {
      return {
        sourceFieldName: field.sourceFieldName,
        destinationFieldName: availableFields.includes(field.destinationFieldName) ? field.destinationFieldName : "",
      };
    });

    // 2. Check for required fields in the selected operation
    const requiredFields = getRequiredFields(selectedOperation);
    const missingFieldNames = requiredFields.filter((field) => !fields?.some((f) => f.destinationFieldName === field));
    const newFieldMappings = missingFieldNames.map((field) => ({
      sourceFieldName: "",
      destinationFieldName: field,
    }));

    // Update the form with the new field mappings
    setValue(`streams.${streamIndex}.fields`, [...existingFieldMappings, ...newFieldMappings]);

    // 3. Set the matching keys to null if the selected operation does not require them
    if (selectedOperation.matchingKeys === undefined || selectedOperation.matchingKeys.length === 0) {
      setValue(`streams.${streamIndex}.matchingKeys`, null);
    } else if (selectedOperation.matchingKeys.length === 1) {
      // If there is only one matching key options, we can set it directly
      setValue(`streams.${streamIndex}.matchingKeys`, selectedOperation.matchingKeys[0]);
    } else {
      // If there are multiple matching key options, we leave it as an empty array for the user to select an option
      setValue(`streams.${streamIndex}.matchingKeys`, []);
    }
  };

  return (
    <Controller
      name={`streams.${streamIndex}.destinationSyncMode`}
      control={control}
      render={({ field, fieldState }) => (
        <FlexContainer direction="column" gap="xs">
          <Listbox
            value={field.value}
            onChange={(value) => {
              if (value === field.value) {
                return;
              }
              if (value === DestinationSyncMode.append) {
                setValue(`streams.${streamIndex}.matchingKeys`, null);
              }
              if (value === DestinationSyncMode.append_dedup) {
                setValue(`streams.${streamIndex}.matchingKeys`, null);
              }
              if (value !== null) {
                updateFieldMappings(value);
              }
              field.onChange(value);
            }}
          >
            <FloatLayout adaptiveWidth>
              <ListboxButton hasError={!!fieldState.error} data-testid="selected-destination-sync-mode-label">
                {field.value ? (
                  <FlexContainer as="span">
                    <Text>{destinationSyncModeOptions.find((option) => option.value === field.value)?.label}</Text>
                    <Text color="grey">
                      <FormattedMessage id="connection.dataActivation.destinationSyncMode" />
                    </Text>
                  </FlexContainer>
                ) : (
                  <Text color="grey">
                    <FormattedMessage id="connection.dataActivation.selectDestinationSyncMode" />
                  </Text>
                )}
              </ListboxButton>
              <ListboxOptions>
                {destinationSyncModeOptions.map((option) => (
                  <ListboxOption key={option.value} value={option.value} disabled={option.disabled}>
                    <Box p="md" pr="none" as="span">
                      <Text>{option.label}</Text>
                    </Box>
                  </ListboxOption>
                ))}
              </ListboxOptions>
            </FloatLayout>
          </Listbox>
          {fieldState.error && <FormControlErrorMessage name={field.name} />}
        </FlexContainer>
      )}
    />
  );
};
