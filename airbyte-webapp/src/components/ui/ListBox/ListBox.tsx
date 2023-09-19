import { Placement } from "@floating-ui/react-dom";
import { Listbox } from "@headlessui/react";
import { Float } from "@headlessui-float/react";
import classNames from "classnames";
import isEqual from "lodash/isEqual";
import React from "react";
import { useIntl } from "react-intl";

import { Text } from "components/ui/Text";

import { ReactComponent as CaretDownIcon } from "./CaretDownIcon.svg";
import styles from "./ListBox.module.scss";
import { FlexContainer, FlexItem } from "../Flex";

export interface ListBoxControlButtonProps<T> {
  selectedOption?: Option<T>;
  isDisabled?: boolean;
}

const DefaultControlButton = <T,>({ selectedOption, isDisabled }: ListBoxControlButtonProps<T>) => {
  const { formatMessage } = useIntl();

  return (
    <>
      {selectedOption ? (
        <Text as="span" size="lg" className={classNames({ [styles.disabledText]: isDisabled })}>
          <FlexContainer as="span" alignItems="center">
            {selectedOption.icon && <FlexItem className={styles.icon}>{selectedOption.icon}</FlexItem>}
            {selectedOption.label}
          </FlexContainer>
        </Text>
      ) : (
        <Text as="span" size="lg" color="grey">
          {formatMessage({ id: "form.selectValue" })}
        </Text>
      )}

      <CaretDownIcon className={styles.caret} />
    </>
  );
};

export interface Option<T> {
  label: React.ReactNode;
  value: T;
  icon?: React.ReactNode;
  disabled?: boolean;
}

export interface ListBoxProps<T> {
  className?: string;
  optionsMenuClassName?: string;
  optionClassName?: string;
  selectedOptionClassName?: string;
  options: Array<Option<T>>;
  selectedValue?: T;
  onSelect: (selectedValue: T) => void;
  buttonClassName?: string;
  isDisabled?: boolean;
  controlButton?: React.ComponentType<ListBoxControlButtonProps<T>>;
  "data-testid"?: string;
  hasError?: boolean;

  /**
   * Floating menu placement
   */
  placement?: Placement;
  /**
   * If true, the width of the ListBox menu will be the same as the width of the control button. Default is true.
   */
  adaptiveWidth?: boolean;
  /**
   * DEPRECATED. This is a way to hack in a custom button at the bottom of the ListBox, but this is not the right way to do this.
   * We should be using a headlessui Menu for this instead of a ListBox: https://github.com/airbytehq/airbyte/issues/24394
   * @deprecated
   */
  footerOption?: React.ReactNode;
}

export const ListBox = <T,>({
  className,
  options,
  selectedValue,
  onSelect,
  buttonClassName,
  controlButton: ControlButton = DefaultControlButton,
  optionsMenuClassName,
  optionClassName,
  selectedOptionClassName,
  "data-testid": testId,
  hasError,
  isDisabled,
  placement = "bottom",
  adaptiveWidth = true,
  footerOption,
}: ListBoxProps<T>) => {
  const selectedOption = options.find((option) => isEqual(option.value, selectedValue));

  const onOnSelect = (value: T) => {
    onSelect(value);
  };

  return (
    <div className={className} data-testid={testId}>
      <Listbox value={selectedValue} onChange={onOnSelect} disabled={isDisabled} by={isEqual}>
        <Float
          adaptiveWidth={adaptiveWidth}
          placement={placement}
          flip
          offset={5} // $spacing-sm
          autoUpdate={{
            elementResize: false, // this will prevent render in wrong place after multiple open/close actions
          }}
        >
          <Listbox.Button
            className={classNames(buttonClassName, styles.button, { [styles["button--error"]]: hasError })}
          >
            <ControlButton selectedOption={selectedOption} isDisabled={isDisabled} />
          </Listbox.Button>
          <Listbox.Options className={classNames(styles.optionsMenu, optionsMenuClassName)}>
            {options.length > 0 && (
              <>
                {options.map(({ label, value, icon, disabled }, index) => (
                  <Listbox.Option
                    key={typeof label === "string" ? label : index}
                    value={value}
                    disabled={disabled}
                    className={classNames(styles.option, optionClassName, {
                      [styles.disabled]: disabled,
                    })}
                  >
                    {({ active, selected }) => (
                      <FlexContainer
                        alignItems="center"
                        className={classNames(styles.optionValue, selected && selectedOptionClassName, {
                          [styles.active]: active,
                          [styles.selected]: selected,
                        })}
                      >
                        {icon && <FlexItem className={styles.icon}>{icon}</FlexItem>}
                        <Text className={styles.label}>{label}</Text>
                      </FlexContainer>
                    )}
                  </Listbox.Option>
                ))}
              </>
            )}
            {footerOption && (
              <Listbox.Option value={undefined} className={classNames(styles.option, optionClassName)}>
                {footerOption}
              </Listbox.Option>
            )}
          </Listbox.Options>
        </Float>
      </Listbox>
    </div>
  );
};
