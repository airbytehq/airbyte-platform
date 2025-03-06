import { RowFilteringMapperConfiguration } from "core/api/types/AirbyteClient";

import { formValuesToMapperConfiguration, mapperConfigurationToFormValues } from "./formValueHelpers";
import { FilterCondition, RowFilteringMapperFormValues } from "./RowFilteringMapperForm";
import { StreamMapperWithId } from "../types";

const mapperConfigurationIn: StreamMapperWithId<RowFilteringMapperConfiguration>["mapperConfiguration"] = {
  conditions: {
    type: "EQUAL",
    fieldName: "fieldName",
    comparisonValue: "comparisonValue",
  },
};

const formValuesIn: RowFilteringMapperFormValues = {
  condition: FilterCondition.IN,
  fieldName: "fieldName",
  comparisonValue: "comparisonValue",
};

const mapperConfigurationNotIn: StreamMapperWithId<RowFilteringMapperConfiguration>["mapperConfiguration"] = {
  conditions: {
    type: "NOT",
    conditions: [
      {
        type: "EQUAL",
        fieldName: "fieldName",
        comparisonValue: "comparisonValue",
      },
    ],
  },
};

const formValuesNotIn: RowFilteringMapperFormValues = {
  condition: FilterCondition.OUT,
  fieldName: "fieldName",
  comparisonValue: "comparisonValue",
};

describe(`${formValuesToMapperConfiguration.name}`, () => {
  describe("filter rows in", () => {
    it("converts mapper configuration to form values", () => {
      expect(formValuesToMapperConfiguration(formValuesIn)).toEqual(mapperConfigurationIn);
    });

    it("converts form values to mapper configuration", () => {
      expect(formValuesToMapperConfiguration(formValuesNotIn)).toEqual(mapperConfigurationNotIn);
    });
  });

  describe("filter rows out", () => {
    it("converts mapper configuration to form values", () => {
      expect(mapperConfigurationToFormValues(mapperConfigurationIn)).toEqual(formValuesIn);
    });

    it("converts form values to mapper configuration", () => {
      expect(mapperConfigurationToFormValues(mapperConfigurationNotIn)).toEqual(formValuesNotIn);
    });
  });
});
