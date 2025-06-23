import { Listbox } from "@headlessui/react";
import isEqual from "lodash/isEqual";
import React from "react";

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

export interface BaseListBoxProps<T> extends Omit<ListBoxProps<T>, "controlButtonContent"> {
  controlButtonContent: React.ComponentType<ListBoxControlButtonProps<T>>;
}

export const BaseListBox = <T,>({
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
}: BaseListBoxProps<T>) => {
  const selectedOption = options.find((option) => isEqual(option.value, selectedValue));

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
      <Box p="md" as="span">
        <FlexContainer alignItems="center">
          {icon && (typeof icon === "string" ? <Icon type={icon as IconType} size="sm" /> : icon)}
          <Text as={optionTextAs}>{label}</Text>
        </FlexContainer>
      </Box>
    </ListboxOption>
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
          fullWidth={!adaptiveWidth}
          {...(testId && {
            "data-testid": `${testId}-listbox-options`,
          })}
        >
          {options.map(ListBoxOption)}
        </ListboxOptions>
      </FloatLayout>
    </Listbox>
  );
};
