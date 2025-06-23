import { Listbox } from "@headlessui/react";
import debounce from "lodash/debounce";
import isEqual from "lodash/isEqual";
import React, { useCallback, useMemo, useRef } from "react";
import { IndexLocationWithAlign, Virtuoso, VirtuosoHandle } from "react-virtuoso";

import { Box } from "components/ui/Box";
import { Text } from "components/ui/Text";

import { FloatLayout } from "./FloatLayout";
import { ListBoxProps, ListBoxControlButtonProps } from "./ListBox";
import { ListboxButton } from "./ListboxButton";
import { ListboxOption } from "./ListboxOption";
import { ListboxOptions } from "./ListboxOptions";
import { Option } from "./Option";
import { FlexContainer } from "../Flex";
import { Icon, IconType } from "../Icon";

export interface VirtualListBoxProps<T> extends Omit<ListBoxProps<T>, "controlButtonContent"> {
  controlButtonContent: React.ComponentType<ListBoxControlButtonProps<T>>;
}

export const VirtualListBox = <T,>({
  // Core data props
  options,
  selectedValue,
  onSelect,
  // Button props
  controlButtonContent,
  controlButtonAs,
  buttonClassName,
  hasError,
  isDisabled,
  onFocus,
  // Option props
  optionClassName,
  optionTextAs,
  // Layout props
  placement,
  flip = true,
  adaptiveWidth = true,
  // HTML attributes
  id,
  "data-testid": testId,
}: VirtualListBoxProps<T>) => {
  const virtuosoRef = useRef<VirtuosoHandle | null>(null);
  const selectedOption = options.find((option) => isEqual(option.value, selectedValue));
  const searchTerm = useRef("");

  const ControlButtonContent = controlButtonContent;

  const onOnSelect = (value: T) => {
    onSelect(value);
  };

  const ListBoxOption: React.FC<Option<T>> = ({ label, value, icon, disabled, ...restOptionProps }, index) => (
    <ListboxOption
      as="li"
      key={typeof label === "string" ? label : index}
      value={value}
      disabled={disabled}
      className={optionClassName}
      onClick={(e: React.MouseEvent<HTMLButtonElement>) => e.stopPropagation()}
      {...(restOptionProps["data-testid"] && {
        "data-testid": `${restOptionProps["data-testid"]}-option`,
      })}
    >
      <Box p="md" pr="none" as="span">
        <FlexContainer alignItems="center">
          {icon && (typeof icon === "string" ? <Icon type={icon as IconType} size="sm" /> : icon)}
          <Text as={optionTextAs}>{label}</Text>
        </FlexContainer>
      </Box>
    </ListboxOption>
  );

  const initialTopMostItemIndex: IndexLocationWithAlign | undefined = useMemo(() => {
    const index = options.findIndex((option) => isEqual(option.value, selectedValue));
    return index === -1 ? undefined : { index, align: "center" };
  }, [options, selectedValue]);

  const searchVirtualizedList = useCallback(() => {
    const foundOptionIndex = options.findIndex((option) => {
      if (option.disabled) {
        return false;
      }
      return String(option.value).toLocaleLowerCase().startsWith(searchTerm.current.toLocaleLowerCase());
    });
    if (foundOptionIndex > -1 && virtuosoRef.current) {
      virtuosoRef.current?.scrollToIndex(foundOptionIndex);
      onSelect(options[foundOptionIndex].value);
    }
  }, [options, onSelect]);

  const debouncedClearSearchTerm = useMemo(() => debounce(() => (searchTerm.current = ""), 350), []);

  const handleKeydownForVirtualizedList: React.KeyboardEventHandler<HTMLUListElement> = useCallback(
    (e) => {
      // We don't want e.g. "Shift" being added to the search term, only single characters
      if (e.key.length === 1) {
        searchTerm.current += e.key;
      }
      searchVirtualizedList();
      debouncedClearSearchTerm();
    },
    [debouncedClearSearchTerm, searchVirtualizedList]
  );

  return (
    <Listbox value={selectedValue} onChange={onOnSelect} disabled={isDisabled} by={isEqual}>
      <FloatLayout adaptiveWidth={adaptiveWidth} placement={placement} flip={flip}>
        <ListboxButton
          id={id}
          className={buttonClassName}
          hasError={hasError}
          onClick={(e: React.MouseEvent<HTMLButtonElement>) => e.stopPropagation()}
          as={controlButtonAs}
          onFocus={onFocus}
          {...(testId && {
            "data-testid": `${testId}-listbox-button`,
          })}
        >
          <ControlButtonContent selectedOption={selectedOption} isDisabled={isDisabled} />
        </ListboxButton>
        <ListboxOptions
          as="ul"
          onKeyDown={handleKeydownForVirtualizedList}
          fullWidth={!adaptiveWidth}
          {...(testId && {
            "data-testid": `${testId}-listbox-options`,
          })}
        >
          <Virtuoso<Option<T>>
            style={{ height: "300px" }} // $height-long-listbox-options-list
            data={options}
            ref={virtuosoRef}
            increaseViewportBy={{ top: 100, bottom: 100 }}
            itemContent={(index, option) => <ListBoxOption key={index} {...option} />}
            {...(initialTopMostItemIndex && { initialTopMostItemIndex })} // scroll to selected value
          />
        </ListboxOptions>
      </FloatLayout>
    </Listbox>
  );
};
