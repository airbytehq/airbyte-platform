import { formValuesToMapperConfiguration, mapperConfigurationToFormValues } from "./formValueHelpers";
import { FilterCondition, RowFilteringMapperFormValues } from "./RowFilteringMapperForm";

const mapperConfigurationIn = {
  type: "row-filtering",
  mapperConfiguration: {
    id: "id",
    conditions: {
      type: "EQUAL",
      fieldName: "fieldName",
      comparisonValue: "comparisonValue",
    },
  },
} as const;

const formValuesIn: RowFilteringMapperFormValues = {
  type: "row-filtering",
  configuration: {
    id: "id",
    condition: FilterCondition.IN,
    fieldName: "fieldName",
    comparisonValue: "comparisonValue",
  },
};

const mapperConfigurationNotIn = {
  type: "row-filtering",
  mapperConfiguration: {
    id: "id",
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
  },
} as const;

const formValuesNotIn: RowFilteringMapperFormValues = {
  type: "row-filtering",
  configuration: {
    id: "id",
    condition: FilterCondition.OUT,
    fieldName: "fieldName",
    comparisonValue: "comparisonValue",
  },
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
