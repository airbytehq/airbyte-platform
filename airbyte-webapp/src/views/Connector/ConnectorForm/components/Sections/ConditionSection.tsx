import clone from "lodash/clone";
import React, { useCallback, useMemo } from "react";
import { get, useFormContext, useFormState, useWatch } from "react-hook-form";

import GroupControls from "components/GroupControls";
import { DropDown, DropDownOptionDataItem } from "components/ui/DropDown";

import { FormConditionItem } from "core/form/types";

import styles from "./ConditionSection.module.scss";
import { FormSection } from "./FormSection";
import { GroupLabel } from "./GroupLabel";
import { SectionContainer } from "./SectionContainer";
import { setDefaultValues } from "../../useBuildForm";
import { useSshSslImprovements } from "../../useSshSslImprovements";

interface ConditionSectionProps {
  formField: FormConditionItem;
  path: string;
  disabled?: boolean;
}

/**
 * ConditionSection is responsible for handling oneOf sections of form
 */
export const ConditionSection: React.FC<ConditionSectionProps> = ({ formField, path, disabled }) => {
  const { setValue } = useFormContext();
  const value = useWatch({ name: path });

  const { filterConditions, filterSelectionConstValues } = useSshSslImprovements(path);

  const conditions = filterConditions(formField.conditions);
  const selectionConstValues = filterSelectionConstValues(formField.selectionConstValues);

  // the value at selectionPath determines which condition is selected
  const currentSelectionValue = useWatch({ name: formField.selectionPath });
  let currentlySelectedCondition: number | undefined = selectionConstValues.indexOf(currentSelectionValue);
  if (currentlySelectedCondition === -1) {
    // there should always be a matching condition, but in some edge cases
    // (e.g. breaking changes in specs) it's possible to have no matching value.
    currentlySelectedCondition = undefined;
  }

  const onOptionChange = useCallback(
    (selectedItem: DropDownOptionDataItem) => {
      const newSelectedFormBlock = conditions[selectedItem.value];

      const conditionValues = clone(value || {});
      conditionValues[formField.selectionKey] = selectionConstValues[selectedItem.value];
      setDefaultValues(newSelectedFormBlock, conditionValues, { respectExistingValues: true });
      // do not validate the new oneOf part of the form as the user didn't have a chance to fill it out yet.
      setValue(path, conditionValues, { shouldDirty: true, shouldTouch: true });
    },
    [conditions, formField.selectionKey, selectionConstValues, value, path, setValue]
  );

  const options = useMemo(
    () =>
      conditions.map((condition, index) => ({
        label: condition.title,
        value: index,
      })),
    [conditions]
  );

  const error = get(useFormState({ name: path }).errors, path);

  return (
    <SectionContainer>
      <GroupControls
        key={`form-field-group-${formField.fieldKey}`}
        label={<GroupLabel formField={formField} />}
        control={
          <DropDown
            options={options}
            onChange={onOptionChange}
            value={currentlySelectedCondition}
            name={formField.path}
            isDisabled={disabled}
            error={!error?.types && !!error?.message}
          />
        }
        controlClassName={styles.dropdown}
      >
        {/* currentlySelectedCondition is only falsy if a malformed config is loaded which doesn't have a valid value for the const selection key. In this case, render the selection group as empty. */}
        {typeof currentlySelectedCondition !== "undefined" && (
          <FormSection blocks={conditions[currentlySelectedCondition]} path={path} disabled={disabled} skipAppend />
        )}
      </GroupControls>
    </SectionContainer>
  );
};
