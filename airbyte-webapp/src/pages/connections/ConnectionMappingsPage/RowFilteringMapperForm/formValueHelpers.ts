import { ConfiguredStreamMapper, StreamMapperType } from "core/api/types/AirbyteClient";

import { FilterCondition, RowFilteringMapperFormValues } from "./RowFilteringMapperForm";

export function formValuesToMapperConfiguration(values: RowFilteringMapperFormValues): ConfiguredStreamMapper {
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

export function mapperConfigurationToFormValues(
  mapperConfiguration: ConfiguredStreamMapper
): RowFilteringMapperFormValues {
  const { id, conditions } = mapperConfiguration.mapperConfiguration;
  if (conditions.type === "NOT") {
    const {
      conditions: [condition],
    } = conditions;
    return {
      type: StreamMapperType["row-filtering"],
      configuration: {
        id,
        condition: FilterCondition.OUT,
        fieldName: condition.fieldName,
        comparisonValue: condition.comparisonValue,
      },
    };
  }
  return {
    type: StreamMapperType["row-filtering"],
    configuration: {
      id,
      condition: FilterCondition.IN,
      fieldName: conditions.fieldName,
      comparisonValue: conditions.comparisonValue,
    },
  };
}
