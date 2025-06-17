import {
  Listbox,
  ListboxOption as OriginalListboxOption,
  ListboxButton as OriginalListboxButton,
  ListboxOptions as OriginalListboxOptions,
} from "@headlessui/react";
import classNames from "classnames";
import isEqual from "lodash/isEqual";
import React from "react";

import { Text } from "components/ui/Text";

import { FloatLayout } from "./FloatLayout";
import { ListBoxProps, ListBoxControlButtonProps } from "./ListBox";
import styles from "./ListBox.module.scss";
import { Option } from "./Option";
import { FlexContainer, FlexItem } from "../Flex";

export interface BaseListBoxProps<T> extends Omit<ListBoxProps<T>, "controlButton"> {
  controlButton: React.ComponentType<ListBoxControlButtonProps<T>>;
}

export const BaseListBox = <T,>({
  options,
  selectedValue,
  onSelect,
  buttonClassName,
  /**
   * TODO: this is not an actual button, just button content
   * issue_link: https://github.com/airbytehq/airbyte-internal-issues/issues/11011
   */
  controlButton,
  controlButtonAs,
  optionClassName,
  optionTextAs,
  selectedOptionClassName,
  "data-testid": testId,
  hasError,
  id,
  isDisabled,
  placement,
  flip = true,
  adaptiveWidth = true,
  onFocus,
}: BaseListBoxProps<T>) => {
  const selectedOption = options.find((option) => isEqual(option.value, selectedValue));

  const ControlButton = controlButton;

  const onOnSelect = (value: T) => {
    onSelect(value);
  };

  const ListBoxOption: React.FC<Option<T>> = ({ label, value, icon, disabled, ...restOptionProps }, index) => (
    <OriginalListboxOption
      as="li"
      key={typeof label === "string" ? label : index}
      value={value}
      disabled={disabled}
      className={classNames(styles.option, optionClassName, {
        [styles.disabled]: disabled,
      })}
      onClick={(e) => e.stopPropagation()}
      {...(restOptionProps["data-testid"] && {
        "data-testid": `${restOptionProps["data-testid"]}-option`,
      })}
    >
      {({ focus, selected }) => (
        <FlexContainer
          alignItems="center"
          className={classNames(styles.optionValue, selected && selectedOptionClassName, {
            [styles.focus]: focus,
            [styles.selected]: selected,
          })}
        >
          {icon && <FlexItem className={styles.icon}>{icon}</FlexItem>}
          <Text className={styles.label} as={optionTextAs}>
            {label}
          </Text>
        </FlexContainer>
      )}
    </OriginalListboxOption>
  );

  return (
    <Listbox value={selectedValue} onChange={onOnSelect} disabled={isDisabled} by={isEqual}>
      <FloatLayout adaptiveWidth={adaptiveWidth} placement={placement} flip={flip}>
        <OriginalListboxButton
          /**
           * TODO:
           * 1. place butttonClassName to the end of the classNames list to allow overriding styles
           * 2. consider ability to pass Button component props to the ListBoxControlButtonProps
           * (type="clear" for example)
           * issue_link: https://github.com/airbytehq/airbyte-internal-issues/issues/11011
           * */
          className={classNames(buttonClassName, styles.button, { [styles["button--error"]]: hasError })}
          onClick={(e: React.MouseEvent<HTMLButtonElement>) => e.stopPropagation()}
          {...(testId && {
            "data-testid": `${testId}-listbox-button`,
          })}
          id={id}
          as={controlButtonAs}
          onFocus={onFocus}
        >
          <ControlButton selectedOption={selectedOption} isDisabled={isDisabled} />
        </OriginalListboxButton>
        <OriginalListboxOptions
          as="ul"
          className={classNames(styles.optionsMenu, { [styles.nonAdaptive]: !adaptiveWidth })}
          {...(testId && {
            "data-testid": `${testId}-listbox-options`,
          })}
        >
          {options.map(ListBoxOption)}
        </OriginalListboxOptions>
      </FloatLayout>
    </Listbox>
  );
};
