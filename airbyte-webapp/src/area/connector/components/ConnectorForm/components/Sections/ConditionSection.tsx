import classNames from "classnames";
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
import { FormConditionItem } from "core/form/types";

import styles from "./ConditionSection.module.scss";
import {
  getConditionControlType,
  getVisibleConditionsAndConstValues,
  hasFieldsOfItsOwn,
} from "./conditionSectionHelpers";
import { FormSection } from "./FormSection";
import { getGroupLabelText, GroupLabel } from "./GroupLabel";
import { SectionContainer } from "./SectionContainer";
import { setDefaultValues } from "../../useBuildForm";

interface ConditionSectionProps {
  formField: FormConditionItem;
  path: string;
  disabled?: boolean;
}

const SINGLE_OPTION_COUNT = 1;

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
  const isLocked = disabled || formField.readOnly;

  // A single visible option needs no control, except when the saved value matches no visible option (for example
  // an option dropped from the spec). Then the field keeps its usual control so the user can pick the option.
  const isOnlyOptionUnselected = options.length === SINGLE_OPTION_COUNT && currentlySelectedCondition === undefined;
  const controlType = getConditionControlType(formField, conditions);

  const hasSingleOptionWithoutFields = options.length === SINGLE_OPTION_COUNT && !hasFieldsOfItsOwn(conditions[0]);

  if (hasSingleOptionWithoutFields && !isOnlyOptionUnselected) {
    return null;
  }

  const getDropdownControl = () => {
    if (options.length > SINGLE_OPTION_COUNT || isOnlyOptionUnselected) {
      return (
        <ListBox<number>
          options={options}
          onSelect={onOptionChange}
          selectedValue={currentlySelectedCondition}
          isDisabled={isLocked}
          hasError={hasSelectionError}
          data-testid={path}
        />
      );
    }

    if (options.length === SINGLE_OPTION_COUNT) {
      return options[0].label;
    }

    return undefined;
  };

  return (
    <SectionContainer>
      <GroupControls
        key={`form-field-group-${formField.fieldKey}`}
        label={<GroupLabel formField={formField} />}
        control={controlType === "dropdown" ? getDropdownControl() : undefined}
        controlClassName={classNames(
          { [styles.dropdown]: options.length > SINGLE_OPTION_COUNT || isOnlyOptionUnselected },
          { [styles.disabled]: isLocked }
        )}
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
