import { useEffect } from "react";
import { Controller, FormProvider, useForm, useFormContext } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import * as yup from "yup";

import { FormControlErrorMessage } from "components/forms/FormControl";
import { ListBox } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import { RowFilteringMapperConfiguration, StreamMapperType } from "core/api/types/AirbyteClient";

import { formValuesToMapperConfiguration, mapperConfigurationToFormValues } from "./formValueHelpers";
import { autoSubmitResolver } from "../autoSubmitResolver";
import { useMappingContext } from "../MappingContext";
import { MappingFormTextInput, MappingRowContent, MappingRowItem } from "../MappingRow";
import { MappingTypeListBox } from "../MappingTypeListBox";
import { SelectTargetField } from "../SelectTargetField";
import { StreamMapperWithId } from "../types";
import { useGetFieldsInStream } from "../useGetFieldsInStream";

export enum OperationType {
  equal = "EQUAL",
  not = "NOT",
}

export enum FilterCondition {
  IN = "IN",
  OUT = "OUT",
}

export interface RowFilteringMapperFormValues {
  condition: FilterCondition;
  fieldName: string;
  comparisonValue: string;
}

interface RowFilteringMapperFormProps {
  mapping: StreamMapperWithId<RowFilteringMapperConfiguration>;
  streamDescriptorKey: string;
}

const simpleSchema: yup.SchemaOf<RowFilteringMapperFormValues> = yup.object({
  condition: yup.mixed<FilterCondition>().oneOf([FilterCondition.IN, FilterCondition.OUT]).required("form.empty.error"),
  fieldName: yup.string().required("form.empty.error"),
  comparisonValue: yup.string().required("form.empty.error"),
});

const createEmptyDefaultValues = (): RowFilteringMapperFormValues => ({
  condition: FilterCondition.IN,
  fieldName: "",
  comparisonValue: "",
});

export const RowFilteringMapperForm: React.FC<RowFilteringMapperFormProps> = ({ mapping, streamDescriptorKey }) => {
  const { formatMessage } = useIntl();
  const { updateLocalMapping, validateMappings } = useMappingContext();
  const fieldsInStream = useGetFieldsInStream(streamDescriptorKey);

  const methods = useForm<RowFilteringMapperFormValues>({
    defaultValues: mapping ? mapperConfigurationToFormValues(mapping.mapperConfiguration) : createEmptyDefaultValues(),
    resolver: autoSubmitResolver<RowFilteringMapperFormValues>(simpleSchema, (values) => {
      const mapperConfiguration = formValuesToMapperConfiguration(values);
      updateLocalMapping(streamDescriptorKey, mapping.id, { mapperConfiguration });
      validateMappings();
    }),
    mode: "onBlur",
  });

  useEffect(() => {
    updateLocalMapping(streamDescriptorKey, mapping.id, { validationCallback: methods.trigger });
  }, [methods.trigger, streamDescriptorKey, updateLocalMapping, mapping.id]);

  if (!mapping) {
    return null;
  }

  return (
    <FormProvider {...methods}>
      <form>
        <MappingRowContent>
          <MappingTypeListBox
            selectedValue={StreamMapperType["row-filtering"]}
            streamDescriptorKey={streamDescriptorKey}
            mappingId={mapping.id}
          />
          <SelectFilterType />
          <MappingRowItem>
            <Text>
              <FormattedMessage id="connections.mappings.if" />
            </Text>
          </MappingRowItem>
          <SelectTargetField<RowFilteringMapperFormValues> name="fieldName" targetFieldOptions={fieldsInStream} />
          <MappingRowItem>
            <Text>
              <FormattedMessage id="connections.mappings.equals" />
            </Text>
          </MappingRowItem>
          <MappingRowItem>
            <MappingFormTextInput
              placeholder={formatMessage({ id: "connections.mappings.value" })}
              name="comparisonValue"
              testId="comparisonValue"
            />
            <FormControlErrorMessage<RowFilteringMapperFormValues> name="comparisonValue" />
          </MappingRowItem>
        </MappingRowContent>
      </form>
    </FormProvider>
  );
};

const SelectFilterType = () => {
  const { control } = useFormContext<RowFilteringMapperFormValues>();

  const { formatMessage } = useIntl();

  return (
    <Controller
      name="condition"
      control={control}
      render={({ field }) => (
        <ListBox<"IN" | "OUT">
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
