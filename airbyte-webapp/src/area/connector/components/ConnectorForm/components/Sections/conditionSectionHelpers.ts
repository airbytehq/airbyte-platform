import { JSONSchema7Type } from "json-schema";

import { FormConditionItem, FormGroupItem, OneOfControlType } from "core/form/types";

/** An option has fields of its own when it holds anything besides its constant selection value that the user can see. */
export const hasFieldsOfItsOwn = (condition: FormGroupItem): boolean =>
  condition.properties.some((property) => property.const === undefined && !property.airbyte_hidden);

const MAX_SEGMENTED_CONTROL_OPTIONS = 4;
/**
 * A connector spec's explicit display_type wins. Without one, a oneOf with 2 to 4 visible options
 * renders as a segmented control when the choice changes the fields that follow it.
 * Specs are untrusted input, so an unknown display_type falls through to the default rule.
 */
export const getConditionControlType = (
  formField: FormConditionItem,
  visibleConditions: FormGroupItem[]
): OneOfControlType => {
  if (formField.display_type === "radio" || formField.display_type === "dropdown") {
    return formField.display_type;
  }

  const hasSegmentedOptionCount =
    visibleConditions.length >= 2 && visibleConditions.length <= MAX_SEGMENTED_CONTROL_OPTIONS;
  const choiceChangesFields = visibleConditions.some(hasFieldsOfItsOwn);

  return hasSegmentedOptionCount && choiceChangesFields ? "segmented" : "dropdown";
};

export const getVisibleConditionsAndConstValues = (
  formField: FormConditionItem
): { conditions: FormGroupItem[]; selectionConstValues: JSONSchema7Type[] } => {
  const conditions: FormGroupItem[] = [];
  const selectionConstValues: JSONSchema7Type[] = [];

  formField.conditions.forEach((condition, index) => {
    if (!condition.airbyte_hidden) {
      conditions.push(condition);
      selectionConstValues.push(formField.selectionConstValues[index]);
    }
  });

  return {
    conditions,
    selectionConstValues,
  };
};
