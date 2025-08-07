import { Combobox, ComboboxButton, ComboboxInput, ComboboxOption, ComboboxOptions } from "@headlessui/react";
import classNames from "classnames";
import isEqual from "lodash/isEqual";
import React, { useMemo, useState } from "react";

import { Icon } from "components/ui/Icon";
import { Input } from "components/ui/Input";
import { Text } from "components/ui/Text";

import styles from "./DACombobox.module.scss";

export interface DAComboboxOption<T> {
  label: string;
  value: T;
}

interface DAComboboxProps<T> {
  icon?: React.ReactNode;
  options: Array<DAComboboxOption<T>>;
  selectedValue?: T | null;
  onChange: (value: T | null) => void;
  placeholder?: string;
  error: boolean;
}

export const DACombobox = <T,>({ icon, options, selectedValue, onChange, placeholder, error }: DAComboboxProps<T>) => {
  const [query, setQuery] = useState("");

  const filteredOptions =
    query === ""
      ? options
      : options.filter((option) => {
          return option.label.toLowerCase().includes(query.toLowerCase());
        });

  const selectedOption = useMemo(() => {
    if (!selectedValue) {
      return null;
    }
    return options.find((option) => isEqual(option.value, selectedValue)) ?? null;
  }, [options, selectedValue]);

  return (
    <Combobox
      by={isEqual}
      as="div"
      immediate
      className={styles.combobox}
      value={selectedOption}
      virtual={{ options: filteredOptions }}
      onChange={(option) => {
        onChange(option?.value ?? null);
      }}
      onClose={() => {
        setQuery("");
      }}
    >
      <ComboboxInput as={React.Fragment}>
        <Input
          error={error}
          icon={icon}
          value={(query || selectedOption?.label) ?? ""}
          placeholder={placeholder}
          onChange={(event) => setQuery(event.target.value)}
          adornment={
            <ComboboxButton className={styles.caretButton}>
              <Icon type="caretDown" />
            </ComboboxButton>
          }
        />
      </ComboboxInput>
      <ComboboxOptions anchor="bottom start" className={styles.combobox__options}>
        {({ option }: { option: DAComboboxOption<T> }) => (
          <ComboboxOption value={option} as={React.Fragment}>
            {({ selected, focus }) => (
              <div
                className={classNames(styles.combobox__option, {
                  [styles["combobox__option--selected"]]: selected,
                  [styles["combobox__option--focus"]]: focus,
                })}
              >
                <Text>{option.label}</Text>
              </div>
            )}
          </ComboboxOption>
        )}
      </ComboboxOptions>
    </Combobox>
  );
};
