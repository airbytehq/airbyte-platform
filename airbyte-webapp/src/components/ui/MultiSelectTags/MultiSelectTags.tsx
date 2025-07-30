import { Listbox } from "@headlessui/react";
import { useCallback, useMemo, useState } from "react";

import { Box } from "components/ui/Box";
import { CheckBox } from "components/ui/CheckBox";
import { FlexContainer } from "components/ui/Flex";
import { ListboxButton } from "components/ui/ListBox/ListboxButton";
import { ListboxOption } from "components/ui/ListBox/ListboxOption";
import { ListboxOptions } from "components/ui/ListBox/ListboxOptions";
import { Option } from "components/ui/ListBox/Option";
import { Text } from "components/ui/Text";

import { useHeadlessUiOnClose } from "core/utils/useHeadlessUiOnClose";

import styles from "./MultiSelectTags.module.scss";

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
  const [valuesSelectedOnOpen, setValuesSelectedOnOpen] = useState(selectedValues ?? []);

  // For better UX, the originally selected options should always be at the top of the list
  const sortedOptions = useMemo(() => {
    const selectedValueSet = new Set(valuesSelectedOnOpen.map((value) => value));

    const topSection: Array<Option<T>> = [];
    const bottomSection: Array<Option<T>> = [];

    options.forEach((option) => {
      selectedValueSet.has(option.value) ? topSection.push(option) : bottomSection.push(option);
    });

    return topSection.concat(bottomSection);
  }, [options, valuesSelectedOnOpen]);

  // When the listbox closes, we update the selected values so that they will be on top next time it opens
  const onCloseListbox = useCallback(() => {
    setValuesSelectedOnOpen(selectedValues);
  }, [selectedValues]);

  const { targetRef } = useHeadlessUiOnClose(onCloseListbox);

  return (
    <div data-testid={testId}>
      <Listbox as="div" multiple value={selectedValues} onChange={onSelectValues} disabled={disabled} ref={targetRef}>
        <ListboxButton className={styles.multiSelect__button} {...{ "data-testid": `${testId}-button` }}>
          <FlexContainer as="span" wrap="wrap" gap="sm">
            <SelectedValueTags selectedValues={selectedValues} options={options} />
          </FlexContainer>
        </ListboxButton>
        <ListboxOptions fullWidth anchor="bottom">
          {sortedOptions.map(({ label, value }, index) => {
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
      </Listbox>
    </div>
  );
};

interface SelectedValueItemsProps<T> {
  selectedValues: T[];
  options: Array<Option<T>>;
}

const SelectedValueTags = <T,>({ selectedValues, options }: SelectedValueItemsProps<T>) => {
  return (
    <FlexContainer as="span" wrap="wrap" gap="sm">
      {selectedValues?.map((selectedValue, index) => {
        const selectedOption = options.find((option) => option.value === selectedValue);
        return selectedOption ? (
          <FlexContainer className={styles.multiSelect__item} alignItems="center" gap="none" key={index}>
            <Text size="xs" className={styles.multiSelect__itemText}>
              {selectedOption.label}
            </Text>
          </FlexContainer>
        ) : null;
      })}
    </FlexContainer>
  );
};
