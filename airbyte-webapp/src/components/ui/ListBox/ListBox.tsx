import { Listbox } from "@headlessui/react";
import classNames from "classnames";
import React from "react";
import { useIntl } from "react-intl";

import { Text } from "components/ui/Text";

import { ReactComponent as CaretDownIcon } from "./CaretDownIcon.svg";
import styles from "./ListBox.module.scss";

export interface ListBoxControlButtonProps<T> {
  selectedOption?: Option<T>;
}

const DefaultControlButton = <T,>({ selectedOption }: ListBoxControlButtonProps<T>) => {
  const { formatMessage } = useIntl();

  return (
    <>
      {selectedOption ? (
        <Text as="span" size="lg">
          {selectedOption.label}
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
  optionClassName?: string;
  selectedOptionClassName?: string;
  options: Array<Option<T>>;
  selectedValue?: T;
  onSelect: (selectedValue: T) => void;
  buttonClassName?: string;
  controlButton?: React.ComponentType<ListBoxControlButtonProps<T>>;
  "data-testid"?: string;
  hasError?: boolean;
}

export const ListBox = <T,>({
  className,
  options,
  selectedValue,
  onSelect,
  buttonClassName,
  controlButton: ControlButton = DefaultControlButton,
  optionClassName,
  selectedOptionClassName,
  "data-testid": testId,
  hasError,
}: ListBoxProps<T>) => {
  const selectedOption = options.find((option) => option.value === selectedValue);

  const onOnSelect = (value: T) => {
    console.log("onOnSelect", value);
    onSelect(value);
  };

  return (
    <div className={className} data-testid={testId}>
      <Listbox value={selectedValue} onChange={onOnSelect}>
        <Listbox.Button className={classNames(buttonClassName, styles.button, { [styles["button--error"]]: hasError })}>
          <ControlButton selectedOption={selectedOption} />
        </Listbox.Button>
        {/* wrap in div to make `position: absolute` on Listbox.Options result in correct vertical positioning */}
        <div className={styles.optionsContainer}>
          <Listbox.Options className={styles.optionsMenu}>
            {options.map(({ label, value, icon, disabled }, index) => (
              <Listbox.Option
                key={typeof label === "string" ? label : index}
                value={value}
                disabled={disabled}
                className={classNames(styles.option, optionClassName, { [styles.disabled]: disabled })}
              >
                {({ active, selected }) => (
                  <div
                    className={classNames(styles.optionValue, selected && selectedOptionClassName, {
                      [styles.active]: active,
                      [styles.selected]: selected,
                    })}
                  >
                    {icon && <span className={styles.icon}>{icon}</span>}
                    <span className={styles.label}>{label}</span>
                  </div>
                )}
              </Listbox.Option>
            ))}
          </Listbox.Options>
        </div>
      </Listbox>
    </div>
  );
};
