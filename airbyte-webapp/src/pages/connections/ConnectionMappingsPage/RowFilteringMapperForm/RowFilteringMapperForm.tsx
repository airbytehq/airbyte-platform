import { useEffect } from "react";
import { Controller, FormProvider, useForm, useFormContext } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import { z } from "zod";

import { FormControlErrorMessage } from "components/forms/FormControl";
import { ListBox } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import {
  MapperValidationErrorType,
  RowFilteringMapperConfiguration,
  StreamMapperType,
} from "core/api/types/AirbyteClient";

import { formValuesToMapperConfiguration, mapperConfigurationToFormValues } from "./formValueHelpers";
import { autoSubmitResolver } from "../autoSubmitResolver";
import { useMappingContext } from "../MappingContext";
import { MappingFormTextInput, MappingRowContent, MappingRowItem } from "../MappingRow";
import { MappingTypeListBox } from "../MappingTypeListBox";
import { MappingValidationErrorMessage } from "../MappingValidationErrorMessage";
import { SelectTargetField } from "../SelectTargetField";
import { StreamMapperWithId } from "../types";

export enum FilterCondition {
  IN = "IN",
  OUT = "OUT",
}

interface RowFilteringMapperFormProps {
  mapping: StreamMapperWithId<RowFilteringMapperConfiguration>;
  streamDescriptorKey: string;
}

const formSchema = z.object({
  condition: z.nativeEnum(FilterCondition),
  fieldName: z.string().nonempty("form.empty.error"),
  comparisonValue: z.string().nonempty("form.empty.error"),
});

export type RowFilteringMapperFormValues = z.infer<typeof formSchema>;

const createEmptyDefaultValues = (): RowFilteringMapperFormValues => ({
  condition: FilterCondition.IN,
  fieldName: "",
  comparisonValue: "",
});

export const RowFilteringMapperForm: React.FC<RowFilteringMapperFormProps> = ({ mapping, streamDescriptorKey }) => {
  const { formatMessage } = useIntl();
  const { updateLocalMapping, validatingStreams } = useMappingContext();
  const isStreamValidating = validatingStreams.has(streamDescriptorKey);

  const methods = useForm<RowFilteringMapperFormValues>({
    defaultValues: mapping ? mapperConfigurationToFormValues(mapping.mapperConfiguration) : createEmptyDefaultValues(),
    resolver: autoSubmitResolver(formSchema, (values) => {
      const mapperConfiguration = formValuesToMapperConfiguration(values);
      updateLocalMapping(streamDescriptorKey, mapping.id, { mapperConfiguration });
    }),
    mode: "onBlur",
  });

  useEffect(() => {
    updateLocalMapping(streamDescriptorKey, mapping.id, { validationCallback: methods.trigger }, true);
  }, [methods.trigger, streamDescriptorKey, updateLocalMapping, mapping.id]);

  useEffect(() => {
    if (
      mapping.validationError &&
      mapping.validationError.type === MapperValidationErrorType.FIELD_NOT_FOUND &&
      "fieldName" in methods.formState.touchedFields
    ) {
      methods.setError("fieldName", { message: "connections.mappings.error.FIELD_NOT_FOUND" });
    } else {
      methods.clearErrors("fieldName");
    }
  }, [mapping.validationError, methods]);

  if (!mapping) {
    return null;
  }

  return (
    <FormProvider {...methods}>
      <form>
        <MappingRowContent>
          <MappingRowItem>
            <MappingTypeListBox
              disabled={isStreamValidating}
              selectedValue={StreamMapperType["row-filtering"]}
              streamDescriptorKey={streamDescriptorKey}
              mappingId={mapping.id}
            />
          </MappingRowItem>
          <MappingRowItem>
            <SelectFilterType disabled={isStreamValidating} />
          </MappingRowItem>
          <MappingRowItem>
            <Text>
              <FormattedMessage id="connections.mappings.if" />
            </Text>
          </MappingRowItem>
          <SelectTargetField<RowFilteringMapperFormValues>
            name="fieldName"
            mappingId={mapping.id}
            streamDescriptorKey={streamDescriptorKey}
            shouldLimitTypes
            disabled={isStreamValidating}
          />
          <MappingRowItem>
            <Text>
              <FormattedMessage id="connections.mappings.equals" />
            </Text>
          </MappingRowItem>
          <MappingRowItem>
            <MappingFormTextInput
              disabled={isStreamValidating}
              placeholder={formatMessage({ id: "connections.mappings.value" })}
              name="comparisonValue"
              testId="comparisonValue"
            />
            <FormControlErrorMessage<RowFilteringMapperFormValues> name="comparisonValue" />
          </MappingRowItem>
        </MappingRowContent>
        <MappingValidationErrorMessage<RowFilteringMapperFormValues>
          validationError={mapping.validationError}
          touchedFields={methods.formState.touchedFields}
        />
      </form>
    </FormProvider>
  );
};

const SelectFilterType: React.FC<{ disabled: boolean }> = ({ disabled }) => {
  const { control } = useFormContext<RowFilteringMapperFormValues>();

  const { formatMessage } = useIntl();

  return (
    <Controller
      name="condition"
      control={control}
      render={({ field }) => (
        <ListBox<"IN" | "OUT">
          isDisabled={disabled}
          selectedValue={field.value}
          options={[
            { label: formatMessage({ id: "connections.mappings.rowFilter.in" }), value: FilterCondition.IN },
            { label: formatMessage({ id: "connections.mappings.rowFilter.out" }), value: FilterCondition.OUT },
          ]}
          onSelect={(selectedValue) => {
            field.onChange(selectedValue);
            // We're using onBlur mode, so we need to manually trigger the validation
            field.onBlur();
          }}
        />
      )}
    />
  );
};
