import { RowFilteringMapperConfiguration, StreamMapperType } from "core/api/types/AirbyteClient";

import { FilterCondition, RowFilteringMapperFormValues } from "./RowFilteringMapperForm";
import { isRowFilteringOperationEqual, isRowFilteringOperationNot } from "../typeHelpers";
import { StreamMapperWithId } from "../types";

export function formValuesToMapperConfiguration(
  values: RowFilteringMapperFormValues
): StreamMapperWithId<RowFilteringMapperConfiguration> {
  if (values.configuration.condition === FilterCondition.OUT) {
    return {
      type: StreamMapperType["row-filtering"],
      id: values.id,
      mapperConfiguration: {
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
    id: values.id,
    mapperConfiguration: {
      conditions: {
        type: "EQUAL",
        fieldName: values.configuration.fieldName,
        comparisonValue: values.configuration.comparisonValue,
      },
    },
  };
}

export function mapperConfigurationToFormValues(
  mapperWithId: StreamMapperWithId<RowFilteringMapperConfiguration>
): RowFilteringMapperFormValues {
  const { conditions } = mapperWithId.mapperConfiguration;

  if (isRowFilteringOperationNot(conditions)) {
    const {
      conditions: [condition],
    } = conditions;

    if (isRowFilteringOperationEqual(condition)) {
      return {
        type: StreamMapperType["row-filtering"],
        id: mapperWithId.id,
        configuration: {
          condition: FilterCondition.OUT,
          fieldName: condition.fieldName,
          comparisonValue: condition.comparisonValue,
        },
      };
    }
  }
  if (isRowFilteringOperationEqual(conditions)) {
    return {
      type: StreamMapperType["row-filtering"],
      id: mapperWithId.id,
      configuration: {
        condition: FilterCondition.IN,
        fieldName: conditions.fieldName,
        comparisonValue: conditions.comparisonValue,
      },
    };
  }

  throw new Error("Invalid row filtering configuration");
}
