import { Combobox } from "@headlessui/react";
import classNames from "classnames";
import React, { PropsWithChildren, ReactNode } from "react";
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
    <Combobox value={value} onChange={onChange}>
      {({ open }) => (
        <>
          {/* wrapping the input in a button makes the options list always open when the input is focused */}
          <Button open={open}>
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
          </Button>
          {/* wrap in div to make `position: absolute` on Combobox.Options result in correct vertical positioning */}
          {displayOptions.length > 0 && <Options options={displayOptions} />}
        </>
      )}
    </Combobox>
  );
};

export const MultiComboBox = ({ name, options, value, onChange, error, fieldInputProps }: MultiComboBoxProps) => {
  return (
    <Combobox value={value} onChange={onChange} multiple>
      {({ open }) => (
        <>
          {/* wrapping the input in a button makes the options list always open when the input is focused */}
          <Button open={open}>
            <Combobox.Input as={React.Fragment}>
              <TagInput
                name={name}
                fieldValue={value ?? []}
                onChange={onChange}
                onBlur={fieldInputProps?.onBlur}
                error={error}
              />
            </Combobox.Input>
          </Button>
          {/* wrap in div to make `position: absolute` on Combobox.Options result in correct vertical positioning */}
          {options.length > 0 && <Options options={options} />}
        </>
      )}
    </Combobox>
  );
};

const Button: React.FC<PropsWithChildren<{ open: boolean }>> = ({ children, open }) => (
  <Combobox.Button
    as="div"
    onClick={(e) => {
      if (open) {
        e.preventDefault();
      }
    }}
  >
    {children}
  </Combobox.Button>
);

const Options = ({ options }: { options: Option[] }) => (
  <div className={styles.optionsContainer}>
    <Combobox.Options className={styles.optionsMenu}>
      {options.map(({ value, description }) => (
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
);
