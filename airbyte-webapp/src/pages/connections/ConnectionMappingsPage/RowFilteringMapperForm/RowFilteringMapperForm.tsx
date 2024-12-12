import { Controller, FormProvider, useForm, useFormContext } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import { v4 as uuidv4 } from "uuid";
import * as yup from "yup";

import { Input } from "components/ui/Input";
import { ListBox } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import { RowFilteringMapperConfiguration, StreamMapperType } from "core/api/types/AirbyteClient";

import { formValuesToMapperConfiguration, mapperConfigurationToFormValues } from "./formValueHelpers";
import { autoSubmitResolver } from "../autoSubmitResolver";
import { useMappingContext } from "../MappingContext";
import { MappingRowContent, MappingRowInputWrapper } from "../MappingRow";
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
  type: StreamMapperType;
  id: string;
  configuration: {
    condition: FilterCondition;
    fieldName: string;
    comparisonValue: string;
  };
}

interface RowFilteringMapperFormProps {
  mapping: StreamMapperWithId<RowFilteringMapperConfiguration>;
  streamName: string;
}

const simpleSchema: yup.SchemaOf<RowFilteringMapperFormValues> = yup.object({
  type: yup.mixed<StreamMapperType>().oneOf([StreamMapperType["row-filtering"]]).required(),
  id: yup.string().required(),
  configuration: yup.object({
    condition: yup.mixed<FilterCondition>().oneOf([FilterCondition.IN, FilterCondition.OUT]).required(),
    fieldName: yup.string().required(),
    comparisonValue: yup.string().required(),
  }),
});

const createEmptyDefaultValues = (): RowFilteringMapperFormValues => ({
  type: StreamMapperType["row-filtering"],
  id: uuidv4(),
  configuration: {
    condition: FilterCondition.IN,
    fieldName: "",
    comparisonValue: "",
  },
});

export const RowFilteringMapperForm: React.FC<RowFilteringMapperFormProps> = ({ mapping, streamName }) => {
  const { formatMessage } = useIntl();
  const { updateLocalMapping, validateMappings } = useMappingContext();
  const fieldsInStream = useGetFieldsInStream(streamName);

  const methods = useForm<RowFilteringMapperFormValues>({
    defaultValues: mapping ? mapperConfigurationToFormValues(mapping) : createEmptyDefaultValues(),
    resolver: autoSubmitResolver<RowFilteringMapperFormValues>(simpleSchema, (values) => {
      const mapperConfiguration = formValuesToMapperConfiguration(values);
      updateLocalMapping(streamName, mapperConfiguration);
      validateMappings();
    }),
    mode: "onBlur",
  });

  if (!mapping) {
    return null;
  }

  return (
    <FormProvider {...methods}>
      <form>
        <MappingRowContent>
          <MappingTypeListBox
            selectedValue={StreamMapperType["row-filtering"]}
            streamName={streamName}
            mappingId={mapping.id}
          />
          <SelectFilterType />
          <Text>
            <FormattedMessage id="connections.mappings.if" />
          </Text>
          <SelectTargetField<RowFilteringMapperFormValues>
            name="configuration.fieldName"
            targetFieldOptions={fieldsInStream}
          />
          <Text>
            <FormattedMessage id="connections.mappings.equals" />
          </Text>
          <MappingRowInputWrapper>
            <Input
              placeholder={formatMessage({ id: "connections.mappings.value" })}
              {...methods.register(`configuration.comparisonValue`)}
            />
          </MappingRowInputWrapper>
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
      name="configuration.condition"
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
