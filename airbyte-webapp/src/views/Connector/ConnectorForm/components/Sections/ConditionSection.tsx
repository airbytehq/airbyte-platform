import classNames from "classnames";
import pick from "lodash/pick";
import React, { useCallback, useMemo } from "react";
import { get, useFormContext, useFormState, useWatch } from "react-hook-form";

import GroupControls from "components/GroupControls";
import { DropDown, DropDownOptionDataItem } from "components/ui/DropDown";
import { FlexContainer } from "components/ui/Flex";
import { RadioButton } from "components/ui/RadioButton";
import { Text } from "components/ui/Text";
import { TextWithHTML } from "components/ui/TextWithHTML";

import { FormConditionItem } from "core/form/types";
import { useOptionalDocumentationPanelContext } from "views/Connector/ConnectorDocumentationLayout/DocumentationPanelContext";

import styles from "./ConditionSection.module.scss";
import { FormSection } from "./FormSection";
import { GroupLabel } from "./GroupLabel";
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

  const { conditions, selectionConstValues } = formField;
  const currentSelectionValue = useWatch({ name: `${path}.${formField.selectionKey}` });
  let currentlySelectedCondition: number | undefined = selectionConstValues.indexOf(currentSelectionValue);
  if (currentlySelectedCondition === -1) {
    // there should always be a matching condition, but in some edge cases
    // (e.g. breaking changes in specs) it's possible to have no matching value.
    currentlySelectedCondition = undefined;
  }

  const onOptionChange = useCallback(
    (selectedItem: DropDownOptionDataItem) => {
      const newSelectedFormBlock = conditions[selectedItem.value];

      const conditionValues = value
        ? pick(
            value,
            newSelectedFormBlock.properties.map((property) => property.fieldKey)
          )
        : {};
      conditionValues[formField.selectionKey] = selectionConstValues[selectedItem.value];
      setDefaultValues(newSelectedFormBlock, conditionValues, { respectExistingValues: true });
      // do not validate the new oneOf part of the form as the user didn't have a chance to fill it out yet.
      setValue(path, conditionValues, { shouldDirty: true, shouldTouch: true });
      setFocusedField?.(`${path}[${selectionConstValues[selectedItem.value]}]`);
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

  return (
    <SectionContainer>
      <GroupControls
        key={`form-field-group-${formField.fieldKey}`}
        label={<GroupLabel formField={formField} />}
        control={
          formField.display_type === "radio" ? undefined : (
            <DropDown
              options={options}
              onChange={onOptionChange}
              value={currentlySelectedCondition}
              name={path}
              isDisabled={disabled || formField.readOnly}
              error={error !== undefined}
            />
          )
        }
        controlClassName={classNames(styles.dropdown, { [styles.disabled]: disabled || formField.readOnly })}
        error={error?.message}
      >
        {formField.display_type === "radio" && (
          <FlexContainer direction="column">
            {options.map((option) => (
              <label
                htmlFor={option.label}
                key={option.value}
                data-testid={`radio-option.${option.value}`}
                className={classNames(styles.tile, {
                  [styles["tile--selected"]]: option.value === currentlySelectedCondition,
                  [styles["tile--disabled"]]: disabled || formField.readOnly,
                })}
              >
                <RadioButton
                  id={option.label}
                  name={option.label}
                  value={option.value}
                  key={option.value}
                  disabled={disabled || formField.readOnly}
                  checked={option.value === currentlySelectedCondition}
                  onChange={() => onOptionChange({ value: option.value })}
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
