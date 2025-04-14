import { Listbox } from "@headlessui/react";

import { Box } from "components/ui/Box";
import { CheckBox } from "components/ui/CheckBox";
import { FlexContainer } from "components/ui/Flex";
import { ListboxButton } from "components/ui/ListBox/ListboxButton";
import { ListboxOption } from "components/ui/ListBox/ListboxOption";
import { ListboxOptions } from "components/ui/ListBox/ListboxOptions";
import { Option } from "components/ui/ListBox/Option";
import { Text } from "components/ui/Text";

import styles from "./MultiSelectTags.module.scss";
import { Icon } from "../Icon";
import { FloatLayout } from "../ListBox/FloatLayout";

export interface MultiSelectTagsProps<T> {
  options: Array<Option<T>>;
  selectedValues: T[];
  onSelectValues: (selectedValues: T[]) => void;
  testId?: string;
  disabled?: boolean;
}

export const MultiSelectTags = <T,>({
  options,
  selectedValues,
  onSelectValues,
  testId,
  disabled,
}: MultiSelectTagsProps<T>) => {
  const handleRemoveSingleValue = (value: T) => {
    onSelectValues(selectedValues?.filter((selectedValue) => selectedValue !== value) || []);
  };

  return (
    <div data-testid={testId}>
      <Listbox multiple value={selectedValues} onChange={onSelectValues} disabled={disabled}>
        <FloatLayout adaptiveWidth>
          <ListboxButton className={styles.multiSelect__button} {...{ "data-testid": `${testId}-button` }}>
            <FlexContainer as="span" wrap="wrap" gap="sm">
              <SelectedValueTags
                selectedValues={selectedValues}
                options={options}
                handleRemoveSingleValue={handleRemoveSingleValue}
              />
            </FlexContainer>
          </ListboxButton>
          <ListboxOptions fullWidth>
            {options.map(({ label, value }, index) => {
              return (
                <ListboxOption value={value} key={index}>
                  {({ selected }) => (
                    <Box p="md" pr="none">
                      <FlexContainer alignItems="center" as="span">
                        <CheckBox checked={selected} readOnly />
                        <Text>{label}</Text>
                      </FlexContainer>
                    </Box>
                  )}
                </ListboxOption>
              );
            })}
          </ListboxOptions>
        </FloatLayout>
      </Listbox>
    </div>
  );
};

interface SelectedValueItemsProps<T> {
  selectedValues: T[];
  options: Array<Option<T>>;
  handleRemoveSingleValue: (value: T) => void;
}

const SelectedValueTags = <T,>({ selectedValues, options, handleRemoveSingleValue }: SelectedValueItemsProps<T>) => {
  return (
    <FlexContainer as="span" wrap="wrap" gap="sm">
      {selectedValues?.map((selectedValue, index) => {
        const selectedOption = options.find((option) => option.value === selectedValue);
        return selectedOption ? (
          <FlexContainer className={styles.multiSelect__item} alignItems="center" gap="none" key={index}>
            <Text size="xs" className={styles.multiSelect__itemText}>
              {selectedOption.label}
            </Text>
            <button
              className={styles.multiSelect__removeItem}
              onClick={(e) => {
                // Prevents headless-ui from closing the listbox
                handleRemoveSingleValue(selectedOption.value);
                e.preventDefault();
              }}
            >
              <Icon type="cross" color="action" size="xs" />
            </button>
          </FlexContainer>
        ) : null;
      })}
    </FlexContainer>
  );
};
