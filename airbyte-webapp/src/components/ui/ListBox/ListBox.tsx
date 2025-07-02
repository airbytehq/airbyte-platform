import { ListboxButton } from "@headlessui/react";
import { FloatProps } from "@headlessui-float/react";
import React, { ComponentPropsWithoutRef, useMemo } from "react";
import { useIntl } from "react-intl";

import { Text } from "components/ui/Text";

import { BaseListBox } from "./BaseListBox";
import { Option } from "./Option";
import { VirtualListBox } from "./VirtualListbox";
import { FlexContainer } from "../Flex";
import { Icon, IconType } from "../Icon";

export interface ListBoxControlButtonProps<T> {
  selectedOption?: Option<T>;
  isDisabled?: boolean;
  placeholder?: string;
}

const DefaultControlButtonContent = <T,>({ placeholder, selectedOption, isDisabled }: ListBoxControlButtonProps<T>) => {
  const { formatMessage } = useIntl();

  return (
    <>
      {selectedOption ? (
        <Text as="span" size="lg" {...(isDisabled && { color: "grey300" })}>
          <FlexContainer as="span" alignItems="center">
            {selectedOption.icon &&
              (typeof selectedOption.icon === "string" ? (
                <Icon type={selectedOption.icon as IconType} size="sm" />
              ) : (
                selectedOption.icon
              ))}
            {selectedOption.label}
          </FlexContainer>
        </Text>
      ) : (
        <Text as="span" size="lg" color="grey">
          {placeholder ?? formatMessage({ id: "form.selectValue" })}
        </Text>
      )}
    </>
  );
};

export interface ListBoxProps<T> {
  // Core data props
  /** Array of options to display in the listbox */
  options: Array<Option<T>>;
  /** Currently selected value */
  selectedValue?: T;
  /** Callback function called when an option is selected */
  onSelect: (selectedValue: T) => void;
  // Button props
  /**
   * Custom button content for the ListboxButton.
   * This prop allows you to provide custom content to be used inside the control button for the ListBox.
   */
  controlButtonContent?: React.ComponentType<ListBoxControlButtonProps<T>>;
  /**
   * Custom element type for the ListboxButton.
   * This prop allows you to replace the original ListBox control button with a custom element type.
   */
  controlButtonAs?: ComponentPropsWithoutRef<typeof ListboxButton>["as"];
  /** CSS class name for the control button */
  buttonClassName?: string;
  /** Whether the control button should display an error state */
  hasError?: boolean;
  /** Whether the listbox is disabled */
  isDisabled?: boolean;
  /** Placeholder text to display when no option is selected */
  placeholder?: string;
  /** Callback function called when the control button receives focus */
  onFocus?: () => void;
  // Option props
  /** CSS class name for individual option elements */
  optionClassName?: string;
  /** HTML element type for option text content */
  optionTextAs?: ComponentPropsWithoutRef<typeof Text>["as"];
  // Layout props
  /**
   * Floating menu placement
   */
  placement?: FloatProps["placement"];
  /**
   * Floating menu flip strategy
   * @default 15px (flip to the opposite side if there is no space)
   */
  flip?: FloatProps["flip"];
  /**
   * If true, the width of the ListBox menu will be the same as the width of the control button. Default is true.
   * If false, the width of the ListBox menu will have max-width of 200px.
   */
  adaptiveWidth?: FloatProps["adaptiveWidth"];
  // HTML attributes
  /** HTML id attribute for the control button */
  id?: string;
  /** Test identifier for automated testing */
  "data-testid"?: string;
}

export const MIN_OPTIONS_FOR_VIRTUALIZATION = 30;

export const ListBox = <T,>({ controlButtonContent, ...restProps }: ListBoxProps<T>) => {
  const isVirtualized = useMemo(
    () => restProps.options.length > MIN_OPTIONS_FOR_VIRTUALIZATION,
    [restProps.options.length]
  );

  return isVirtualized ? (
    <VirtualListBox<T> controlButtonContent={controlButtonContent ?? DefaultControlButtonContent} {...restProps} />
  ) : (
    <BaseListBox<T> controlButtonContent={controlButtonContent ?? DefaultControlButtonContent} {...restProps} />
  );
};
