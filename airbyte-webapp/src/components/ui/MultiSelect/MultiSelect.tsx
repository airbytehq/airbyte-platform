import { Listbox } from "@headlessui/react";
import { Float } from "@headlessui-float/react";
import React from "react";

import { Box } from "components/ui/Box";
import { CheckBox } from "components/ui/CheckBox";
import { FlexContainer } from "components/ui/Flex";
import { ListboxButton } from "components/ui/ListBox/ListboxButton";
import { ListboxOption } from "components/ui/ListBox/ListboxOption";
import { ListboxOptions } from "components/ui/ListBox/ListboxOptions";
import { Option } from "components/ui/ListBox/Option";
import { Text } from "components/ui/Text";

import styles from "./MultiSelect.module.scss";
import { Badge } from "../Badge";

export interface MultiSelectProps<T> {
  label: string;
  options: Array<Option<T>>;
  selectedValues: T[];
  onSelectValues: (selectedValues: T[]) => void;
}

export const MultiSelect = <T,>({ options, selectedValues, onSelectValues, label }: MultiSelectProps<T>) => {
  return (
    <Listbox multiple value={selectedValues} onChange={onSelectValues}>
      <Float
        placement="bottom"
        flip
        shift={5} // $spacing-sm
        offset={5} // $spacing-sm
      >
        <ListboxButton className={styles.multiSelect__button}>
          <FlexContainer as="span" wrap="wrap" gap="sm">
            <Text>{label}</Text>
            {selectedValues.length > 0 && <Badge variant="blue">{selectedValues.length}</Badge>}
          </FlexContainer>
        </ListboxButton>
        <ListboxOptions>
          {options.map(({ label, value }, index) => (
            <MultiSelectOption label={label} value={value} key={index} />
          ))}
        </ListboxOptions>
      </Float>
    </Listbox>
  );
};

interface MultiSelectOptionProps<T> {
  label: React.ReactNode;
  value: T;
}

const MultiSelectOption = <T,>({ label, value }: MultiSelectOptionProps<T>) => (
  <ListboxOption value={value}>
    {({ selected }) => (
      <Box p="md">
        <FlexContainer alignItems="center" as="span">
          <CheckBox checked={selected} readOnly />
          <Text>{label}</Text>
        </FlexContainer>
      </Box>
    )}
  </ListboxOption>
);
