import { ListboxButton as OriginalListboxButton } from "@headlessui/react";
import { FloatProps } from "@headlessui-float/react";
import classNames from "classnames";
import React, { ComponentPropsWithoutRef, useMemo } from "react";
import { useIntl } from "react-intl";

import { Text } from "components/ui/Text";

import { BaseListBox } from "./BaseListBox";
import styles from "./ListBox.module.scss";
import { Option } from "./Option";
import { VirtualListBox } from "./VirtualListbox";
import { FlexContainer, FlexItem } from "../Flex";
import { Icon } from "../Icon";

export interface ListBoxControlButtonProps<T> {
  selectedOption?: Option<T>;
  isDisabled?: boolean;
  placeholder?: string;
}

const DefaultControlButton = <T,>({ placeholder, selectedOption, isDisabled }: ListBoxControlButtonProps<T>) => {
  const { formatMessage } = useIntl();

  return (
    <>
      {selectedOption ? (
        <Text
          as="span"
          size="lg"
          className={classNames(styles.defaultControlButton, { [styles.disabledText]: isDisabled })}
        >
          <FlexContainer as="span" alignItems="center">
            {selectedOption.icon && <FlexItem className={styles.icon}>{selectedOption.icon}</FlexItem>}
            {selectedOption.label}
          </FlexContainer>
        </Text>
      ) : (
        <Text as="span" size="lg" color="grey" className={styles.defaultControlButton}>
          {placeholder ?? formatMessage({ id: "form.selectValue" })}
        </Text>
      )}

      <Icon type="chevronDown" color="action" />
    </>
  );
};

export interface ListBoxProps<T> {
  optionClassName?: string;
  optionTextAs?: ComponentPropsWithoutRef<typeof Text>["as"];
  selectedOptionClassName?: string;
  options: Array<Option<T>>;
  selectedValue?: T;
  onSelect: (selectedValue: T) => void;
  buttonClassName?: string;
  id?: string;
  isDisabled?: boolean;
  /**
   * Custom button content for the OriginalListboxButton.
   * This prop allows you to provide custom content to be used inside the control button for the ListBox.
   */
  controlButton?: React.ComponentType<ListBoxControlButtonProps<T>>;
  /**
   * Custom element type for the OriginalListboxButton.
   * This prop allows you to replace the original ListBox control button with a custom element type.
   */
  controlButtonAs?: ComponentPropsWithoutRef<typeof OriginalListboxButton>["as"];
  hasError?: boolean;
  placeholder?: string;
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
  onFocus?: () => void;
  "data-testid"?: string;
}

export const MIN_OPTIONS_FOR_VIRTUALIZATION = 30;

export const ListBox = <T,>({ controlButton, ...restProps }: ListBoxProps<T>) => {
  const isVirtualized = useMemo(
    () => restProps.options.length > MIN_OPTIONS_FOR_VIRTUALIZATION,
    [restProps.options.length]
  );

  return isVirtualized ? (
    <VirtualListBox<T> controlButton={controlButton ?? DefaultControlButton} {...restProps} />
  ) : (
    <BaseListBox<T> controlButton={controlButton ?? DefaultControlButton} {...restProps} />
  );
};
