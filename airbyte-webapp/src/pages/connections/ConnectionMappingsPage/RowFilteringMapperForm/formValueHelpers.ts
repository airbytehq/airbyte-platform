import { RowFilteringMapperConfiguration } from "core/api/types/AirbyteClient";

import { FilterCondition, RowFilteringMapperFormValues } from "./RowFilteringMapperForm";
import { isRowFilteringOperationEqual, isRowFilteringOperationNot } from "../typeHelpers";
import { StreamMapperWithId } from "../types";

export function formValuesToMapperConfiguration(
  values: RowFilteringMapperFormValues
): StreamMapperWithId<RowFilteringMapperConfiguration>["mapperConfiguration"] {
  if (values.condition === FilterCondition.OUT) {
    return {
      conditions: {
        type: "NOT",
        conditions: [
          {
            type: "EQUAL",
            fieldName: values.fieldName,
            comparisonValue: values.comparisonValue,
          },
        ],
      },
    };
  }
  return {
    conditions: {
      type: "EQUAL",
      fieldName: values.fieldName,
      comparisonValue: values.comparisonValue,
    },
  };
}

export function mapperConfigurationToFormValues(
  mapperConfiguration: StreamMapperWithId<RowFilteringMapperConfiguration>["mapperConfiguration"]
): RowFilteringMapperFormValues {
  const { conditions } = mapperConfiguration;

  if (isRowFilteringOperationNot(conditions)) {
    const {
      conditions: [condition],
    } = conditions;

    if (isRowFilteringOperationEqual(condition)) {
      return {
        condition: FilterCondition.OUT,
        fieldName: condition.fieldName,
        comparisonValue: condition.comparisonValue,
      };
    }
  }
  if (isRowFilteringOperationEqual(conditions)) {
    return {
      condition: FilterCondition.IN,
      fieldName: conditions.fieldName,
      comparisonValue: conditions.comparisonValue,
    };
  }

  throw new Error("Invalid row filtering configuration");
}
