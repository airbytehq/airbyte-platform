import classNames from "classnames";
import { JSONSchema7Type } from "json-schema";
import pick from "lodash/pick";
import React, { useCallback, useMemo } from "react";
import { get, useFormContext, useFormState, useWatch } from "react-hook-form";

import { FlexContainer } from "components/ui/Flex";
import GroupControls from "components/ui/GroupControls";
import { ListBox } from "components/ui/ListBox";
import { RadioButton } from "components/ui/RadioButton";
import { SegmentedControl } from "components/ui/SegmentedControl";
import { Text } from "components/ui/Text";
import { TextWithHTML } from "components/ui/TextWithHTML";

import { useOptionalDocumentationPanelContext } from "area/connector/components/ConnectorDocumentationLayout/DocumentationPanelContext";
import { FormConditionItem, FormGroupItem } from "core/form/types";

import styles from "./ConditionSection.module.scss";
import { FormSection } from "./FormSection";
import { getGroupLabelText, GroupLabel } from "./GroupLabel";
import { SectionContainer } from "./SectionContainer";
import { setDefaultValues } from "../../useBuildForm";

interface ConditionSectionProps {
  formField: FormConditionItem;
  path: string;
  disabled?: boolean;
}

/**
 * ConditionSection is responsible for handling oneOf sections of form
 */
export const ConditionSection: React.FC<ConditionSectionProps> = ({ formField, path, disabled }) => {
  const { setValue, clearErrors } = useFormContext();
  const setFocusedField = useOptionalDocumentationPanelContext()?.setFocusedField;
  const value = useWatch({ name: path });

  const { conditions, selectionConstValues } = useMemo(
    () => getVisibleConditionsAndConstValues(formField),
    [formField]
  );
  const currentSelectionValue = useWatch({ name: `${path}.${formField.selectionKey}` });
  let currentlySelectedCondition: number | undefined = selectionConstValues.indexOf(currentSelectionValue);
  if (currentlySelectedCondition === -1) {
    // there should always be a matching condition, but in some edge cases
    // (e.g. breaking changes in specs) it's possible to have no matching value.
    currentlySelectedCondition = undefined;
  }

  const onOptionChange = useCallback(
    (selectedItem: number) => {
      const newSelectedFormBlock = conditions[selectedItem];

      const conditionValues = value
        ? pick(
            value,
            newSelectedFormBlock.properties.map((property) => property.fieldKey)
          )
        : {};
      conditionValues[formField.selectionKey] = selectionConstValues[selectedItem];
      setDefaultValues(newSelectedFormBlock, conditionValues, { respectExistingValues: true });
      // do not validate the new oneOf part of the form as the user didn't have a chance to fill it out yet.
      setValue(path, conditionValues, { shouldDirty: true, shouldTouch: true });
      setFocusedField?.(`${path}[${selectionConstValues[selectedItem]}]`);
      clearErrors(path);
    },
    [conditions, value, formField.selectionKey, selectionConstValues, setValue, path, setFocusedField, clearErrors]
  );

  const options = useMemo(
    () =>
      conditions.map((condition, index) => ({
        label: condition.title,
        value: index,
        description: condition.description,
      })),
    [conditions]
  );

  const error = get(useFormState({ name: path }).errors, path);
  // errors inside the selected option's fields do not make the selection itself invalid
  const hasSelectionError = error?.message !== undefined;
  const controlType = getConditionControlType(formField, conditions);
  const isLocked = disabled || formField.readOnly;

  return (
    <SectionContainer>
      <GroupControls
        key={`form-field-group-${formField.fieldKey}`}
        label={<GroupLabel formField={formField} />}
        control={
          controlType === "dropdown" ? (
            <ListBox<number>
              options={options}
              onSelect={onOptionChange}
              selectedValue={currentlySelectedCondition}
              isDisabled={isLocked}
              hasError={hasSelectionError}
              data-testid={path}
            />
          ) : undefined
        }
        controlClassName={classNames(styles.dropdown, { [styles.disabled]: isLocked })}
        error={error?.message}
      >
        {controlType === "segmented" && (
          <SegmentedControl<number>
            options={options}
            onSelect={onOptionChange}
            selectedValue={currentlySelectedCondition}
            isDisabled={isLocked}
            hasError={hasSelectionError}
            aria-label={getGroupLabelText(formField)}
            data-testid={path}
          />
        )}
        {controlType === "radio" && (
          <FlexContainer direction="column">
            {options.map((option) => (
              <label
                htmlFor={option.label}
                key={option.value}
                data-testid={`radio-option.${option.value}`}
                className={classNames(styles.tile, {
                  [styles["tile--selected"]]: option.value === currentlySelectedCondition,
                  [styles["tile--disabled"]]: isLocked,
                })}
              >
                <RadioButton
                  id={option.label}
                  name={option.label}
                  value={option.value}
                  key={option.value}
                  disabled={isLocked}
                  checked={option.value === currentlySelectedCondition}
                  onChange={() => onOptionChange(option.value)}
                />
                <FlexContainer direction="column">
                  <Text size="lg">{option.label}</Text>
                  <Text size="sm">
                    <TextWithHTML text={option.description} />
                  </Text>
                </FlexContainer>
              </label>
            ))}
          </FlexContainer>
        )}
        {currentlySelectedCondition !== undefined && (
          <FormSection blocks={conditions[currentlySelectedCondition]} path={path} disabled={disabled} skipAppend />
        )}
      </GroupControls>
    </SectionContainer>
  );
};

const MAX_SEGMENTED_CONTROL_OPTIONS = 4;

/**
 * A connector spec's explicit display_type wins. Without one, a oneOf with 2 to 4 visible options
 * renders as a segmented control when the choice changes the fields that follow it.
 * Specs are untrusted input, so an unknown display_type falls through to the default rule.
 */
const getConditionControlType = (
  formField: FormConditionItem,
  visibleConditions: FormGroupItem[]
): "radio" | "dropdown" | "segmented" => {
  if (formField.display_type === "radio" || formField.display_type === "dropdown") {
    return formField.display_type;
  }

  const hasSegmentedOptionCount =
    visibleConditions.length >= 2 && visibleConditions.length <= MAX_SEGMENTED_CONTROL_OPTIONS;
  const choiceChangesFields = visibleConditions.some((condition) =>
    condition.properties.some((property) => property.const === undefined && !property.airbyte_hidden)
  );

  return hasSegmentedOptionCount && choiceChangesFields ? "segmented" : "dropdown";
};

const getVisibleConditionsAndConstValues = (
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
