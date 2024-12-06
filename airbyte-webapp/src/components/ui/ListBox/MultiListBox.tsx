import {
  Listbox,
  ListboxOption as OriginalListboxOption,
  ListboxButton as OriginalListboxButton,
  ListboxOptions as OriginalListboxOptions,
} from "@headlessui/react";
import { Float } from "@headlessui-float/react";
import classNames from "classnames";
import isEqual from "lodash/isEqual";
import React from "react";

import { Text } from "components/ui/Text";

import styles from "./ListBox.module.scss";
import { Badge } from "../Badge";
import { CheckBox } from "../CheckBox";
import { FlexContainer } from "../Flex";
import { Icon } from "../Icon";

export interface ListBoxControlButtonProps<T> {
  selectedValues: T[];
  isDisabled?: boolean;
  label: string;
}

const DefaultControlButton = <T,>({ selectedValues, label }: ListBoxControlButtonProps<T>) => {
  return (
    <>
      <FlexContainer>
        <Text>{label}</Text>
        {selectedValues.length > 0 && <Badge variant="blue">{selectedValues.length}</Badge>}
      </FlexContainer>
      <Icon type="chevronDown" color="action" />
    </>
  );
};

export interface Option<T> {
  label: React.ReactNode;
  value: T;
  icon?: React.ReactNode;
  disabled?: boolean;
  "data-testid"?: string;
}

export interface MultiListBoxProps<T> {
  options: Array<Option<T>>;
  selectedValues: T[];
  onSelectValues: (selectedValues: T[]) => void;
  isDisabled?: boolean;
  controlButton?: React.ComponentType<ListBoxControlButtonProps<T>>;
  label: string;
}

export const MultiListBox = <T,>({
  options,
  selectedValues,
  onSelectValues,
  controlButton: ControlButton = DefaultControlButton,
  isDisabled,
  label,
}: MultiListBoxProps<T>) => {
  return (
    <Listbox value={selectedValues} onChange={onSelectValues} disabled={isDisabled} by={isEqual} multiple>
      <Float
        placement="bottom-start"
        flip
        offset={5} // $spacing-sm
      >
        <OriginalListboxButton className={classNames(styles.button)}>
          <ControlButton selectedValues={selectedValues} isDisabled={isDisabled} label={label} />
        </OriginalListboxButton>
        <OriginalListboxOptions as="ul" className={classNames(styles.optionsMenu)}>
          {options.map(MultiListBoxOption)}
        </OriginalListboxOptions>
      </Float>
    </Listbox>
  );
};

interface MutliListboxOptionProps<T> {
  label: React.ReactNode;
  value: T;
}

const MultiListBoxOption = <T,>({ label, value }: MutliListboxOptionProps<T>) => (
  <OriginalListboxOption as="li" value={value} className={styles.option}>
    {({ focus, selected }) => (
      <FlexContainer
        alignItems="center"
        className={classNames(styles.optionValue, { [styles.selected]: selected, [styles.focus]: focus })}
      >
        <CheckBox checked={selected} readOnly />
        <Text className={styles.label}>{label}</Text>
      </FlexContainer>
    )}
  </OriginalListboxOption>
);
