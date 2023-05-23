import { Combobox } from "@headlessui/react";
import classNames from "classnames";
import { FieldInputProps } from "formik";
import React, { ReactNode } from "react";

import styles from "./ComboBox.module.scss";
import { FlexContainer } from "../Flex";
import { Input } from "../Input";
import { Text } from "../Text";

export interface Option {
  value: string;
  description?: string;
}

export interface ComboBoxProps {
  options: Option[];
  value: string | undefined;
  onChange: (newValue: string) => void;
  error?: boolean;
  adornment?: ReactNode;
  onBlur?: React.FocusEventHandler<HTMLInputElement>;
  fieldInputProps?: FieldInputProps<unknown>;
  filterOptions?: boolean;
}

export const ComboBox = ({
  options,
  value,
  onChange,
  error,
  adornment,
  onBlur,
  fieldInputProps,
  filterOptions = true,
}: ComboBoxProps) => {
  const filteredOptions =
    filterOptions && value
      ? options.filter((option) => option.value.toLowerCase().includes(value.toLowerCase()))
      : options;
  const displayOptions =
    filteredOptions.length === 0
      ? []
      : value && value.length > 0 && !options.map((option) => option.value).includes(value)
      ? [{ value }, ...filteredOptions]
      : filteredOptions;

  return (
    <Combobox value={value} onChange={onChange}>
      {/* wrapping the input in a button makes the options list always open when the input is focused */}
      <Combobox.Button as="div">
        <Combobox.Input as={React.Fragment} onChange={(event) => onChange(event.target.value)}>
          <Input
            {...fieldInputProps}
            value={value}
            error={error}
            adornment={adornment}
            autoComplete="off"
            onBlur={onBlur ? (e) => onBlur?.(e) : fieldInputProps?.onBlur}
          />
        </Combobox.Input>
      </Combobox.Button>
      {/* wrap in div to make `position: absolute` on Combobox.Options result in correct vertical positioning */}
      {displayOptions.length > 0 && (
        <div className={styles.optionsContainer}>
          <Combobox.Options className={styles.optionsMenu}>
            {displayOptions.map(({ value, description }) => (
              <Combobox.Option className={styles.option} key={value} value={value}>
                {({ active, selected }) => (
                  <FlexContainer
                    className={classNames(styles.optionValue, { [styles.active]: active, [styles.selected]: selected })}
                    alignItems="baseline"
                  >
                    <Text size="md" className={styles.value}>
                      {value}
                    </Text>
                    <Text size="sm" className={styles.description}>
                      {description}
                    </Text>
                  </FlexContainer>
                )}
              </Combobox.Option>
            ))}
          </Combobox.Options>
        </div>
      )}
    </Combobox>
  );
};
