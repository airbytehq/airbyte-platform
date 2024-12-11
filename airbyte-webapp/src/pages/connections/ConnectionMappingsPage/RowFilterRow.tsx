import { useMemo } from "react";
import { Controller, FormProvider, useForm, useFormContext } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import { v4 as uuidv4 } from "uuid";
import * as yup from "yup";

import { FlexContainer } from "components/ui/Flex";
import { Input } from "components/ui/Input";
import { ListBox } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import { ConfiguredStreamMapper, StreamMapperType } from "core/api/types/AirbyteClient";

import { autoSubmitResolver } from "./autoSubmitResolver";
import { useMappingContext } from "./MappingContext";
import styles from "./MappingRow.module.scss";
import { MappingTypeListBox } from "./MappingTypeListBox";
import { SelectTargetField } from "./SelectTargetField";
import { useGetFieldsInStream } from "./useGetFieldsInStream";

export enum OperationType {
  equal = "EQUAL",
  not = "NOT",
}

enum FilterCondition {
  IN = "IN",
  OUT = "OUT",
}

export interface RowFilteringMapperFormValues {
  type: StreamMapperType;
  configuration: {
    id: string;
    condition: FilterCondition;
    fieldName: string;
    comparisonValue: string;
  };
}

const simpleSchema: yup.SchemaOf<RowFilteringMapperFormValues> = yup.object({
  type: yup.mixed<StreamMapperType>().oneOf([StreamMapperType["row-filtering"]]).required(),
  configuration: yup.object({
    id: yup.string().required(),
    condition: yup.mixed<FilterCondition>().oneOf([FilterCondition.IN, FilterCondition.OUT]).required(),
    fieldName: yup.string().required(),
    comparisonValue: yup.string().required(),
  }),
});

function formValuesToMapperConfiguration(values: RowFilteringMapperFormValues): ConfiguredStreamMapper {
  if (values.configuration.condition === FilterCondition.OUT) {
    return {
      type: StreamMapperType["row-filtering"],
      mapperConfiguration: {
        id: values.configuration.id,
        conditions: {
          type: "NOT",
          conditions: [
            {
              type: "EQUAL",
              fieldName: values.configuration.fieldName,
              comparisonValue: values.configuration.comparisonValue,
            },
          ],
        },
      },
    };
  }
  return {
    type: StreamMapperType["row-filtering"],
    mapperConfiguration: {
      id: values.configuration.id,
      conditions: {
        type: "EQUAL",
        fieldName: values.configuration.fieldName,
        comparisonValue: values.configuration.comparisonValue,
      },
    },
  };
}
interface RowFilteringRowProps {
  mappingId: string;
  streamName: string;
}

export const RowFilteringRow: React.FC<RowFilteringRowProps> = ({ mappingId, streamName }) => {
  const { formatMessage } = useIntl();
  const { updateLocalMapping, streamsWithMappings, validateMappings } = useMappingContext();
  const mapping = streamsWithMappings[streamName].find((m) => m.mapperConfiguration.id === mappingId);
  const fieldsInStream = useGetFieldsInStream(streamName);

  const defaultValues = useMemo(() => {
    return {
      type: StreamMapperType["row-filtering"],
      configuration: {
        id: mapping?.mapperConfiguration.id ?? uuidv4(),
        type: mapping?.mapperConfiguration.type ?? FilterCondition.IN,
        fieldName: mapping?.mapperConfiguration.fieldName ?? "",
        conditionValue: mapping?.mapperConfiguration.conditionValue ?? "",
      },
    };
  }, [mapping]);

  const methods = useForm<RowFilteringMapperFormValues>({
    defaultValues,
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
        <FlexContainer direction="row" alignItems="center" className={styles.rowContent}>
          <MappingTypeListBox
            selectedValue={StreamMapperType["row-filtering"]}
            streamName={streamName}
            mappingId={mapping?.mapperConfiguration.id}
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
          <Input
            containerClassName={styles.input}
            placeholder={formatMessage({ id: "connections.mappings.value" })}
            {...methods.register(`configuration.comparisonValue`)}
          />
        </FlexContainer>
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
