import { Combobox, ComboboxInput, ComboboxOption, ComboboxOptions } from "@headlessui/react";
import classNames from "classnames";
import React, { forwardRef, ReactNode } from "react";
import { ControllerRenderProps, FieldValues } from "react-hook-form";

import styles from "./ComboBox.module.scss";
import { FlexContainer } from "../Flex";
import { Input } from "../Input";
import { TagInput } from "../TagInput";
import { Text } from "../Text";

export interface Option {
  value: string;
  description?: string;
}

interface BaseProps {
  options: Option[];
  error?: boolean;
  fieldInputProps?: ControllerRenderProps<FieldValues, string>;
}

export interface ComboBoxProps extends BaseProps {
  value: string | undefined;
  onChange: (newValue: string) => void;
  adornment?: ReactNode;
  onBlur?: React.FocusEventHandler<HTMLInputElement>;
  filterOptions?: boolean;
}

export interface MultiComboBoxProps extends BaseProps {
  name: string;
  value: string[] | undefined;
  onChange: (newValue: string[]) => void;
}

const Options = forwardRef<HTMLDivElement, { options: Option[] }>(({ options }, ref) => (
  <ComboboxOptions ref={ref} as="ul" className={styles.optionsMenu} modal={false}>
    {options.length > 0 &&
      options.map(({ value, description }) => (
        <ComboboxOption as="li" key={value} value={value}>
          {({ focus, selected }) => (
            <FlexContainer
              className={classNames(styles.optionValue, { [styles.focus]: focus, [styles.selected]: selected })}
              alignItems="baseline"
            >
              <Text size="md">{value}</Text>
              <Text size="sm" className={styles.description}>
                {description}
              </Text>
            </FlexContainer>
          )}
        </ComboboxOption>
      ))}
  </ComboboxOptions>
));
Options.displayName = "Options";

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
  const displayOptions = [
    ...(value && !options.map((option) => option.value).includes(value) ? [{ value }] : []),
    ...filteredOptions,
  ];

  return (
    <Combobox value={value} onChange={onChange} immediate>
      <ComboboxInput as={React.Fragment}>
        <Input
          {...fieldInputProps}
          value={value}
          error={error}
          adornment={adornment}
          autoComplete="off"
          onChange={(event) => onChange(event.target.value)}
          onBlur={onBlur ? (e) => onBlur?.(e) : fieldInputProps?.onBlur}
        />
      </ComboboxInput>
      <Options options={displayOptions} />
    </Combobox>
  );
};

export const MultiComboBox = ({ name, options, value, onChange, error, fieldInputProps }: MultiComboBoxProps) => {
  return (
    <Combobox value={value} onChange={onChange} multiple immediate>
      <ComboboxInput as={React.Fragment}>
        <TagInput
          name={name}
          fieldValue={value ?? []}
          onChange={onChange}
          onBlur={fieldInputProps?.onBlur}
          error={error}
        />
      </ComboboxInput>
      <Options options={options} />
    </Combobox>
  );
};
