import {
  Listbox,
  ListboxOption as OriginalListboxOption,
  ListboxButton as OriginalListboxButton,
  ListboxOptions as OriginalListboxOptions,
} from "@headlessui/react";
import { Float, FloatProps } from "@headlessui-float/react";
import classNames from "classnames";
import debounce from "lodash/debounce";
import isEqual from "lodash/isEqual";
import React, { ComponentPropsWithoutRef, useCallback, useMemo, useRef } from "react";
import { useIntl } from "react-intl";
import { IndexLocationWithAlign, Virtuoso, VirtuosoHandle } from "react-virtuoso";

import { Text } from "components/ui/Text";

import styles from "./ListBox.module.scss";
import { FlexContainer, FlexItem } from "../Flex";
import { Icon } from "../Icon";

export interface ListBoxControlButtonProps<T> {
  selectedOption?: Option<T>;
  isDisabled?: boolean;
}

const DefaultControlButton = <T,>({ selectedOption, isDisabled }: ListBoxControlButtonProps<T>) => {
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
          {formatMessage({ id: "form.selectValue" })}
        </Text>
      )}

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

export interface ListBoxProps<T> {
  className?: string;
  optionsMenuClassName?: string;
  optionClassName?: string;
  optionTextAs?: ComponentPropsWithoutRef<typeof Text>["as"];
  selectedOptionClassName?: string;
  options: Array<Option<T>>;
  selectedValue?: T;
  onSelect: (selectedValue: T) => void;
  buttonClassName?: string;
  id?: string;
  isDisabled?: boolean;
  controlButton?: React.ComponentType<ListBoxControlButtonProps<T>>;
  "data-testid"?: string;
  hasError?: boolean;
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
  /**
   * DEPRECATED. This is a way to hack in a custom button at the bottom of the ListBox, but this is not the right way to do this.
   * We should be using a headlessui Menu for this instead of a ListBox: https://github.com/airbytehq/airbyte/issues/24394
   * @deprecated
   */
  footerOption?: React.ReactNode;
  onFocus?: () => void;
}

export const MIN_OPTIONS_FOR_VIRTUALIZATION = 30;

export const ListBox = <T,>({
  className,
  options,
  selectedValue,
  onSelect,
  buttonClassName,
  controlButton: ControlButton = DefaultControlButton,
  optionsMenuClassName,
  optionClassName,
  optionTextAs,
  selectedOptionClassName,
  "data-testid": testId,
  hasError,
  id,
  isDisabled,
  placement = "bottom",
  flip = 15,
  adaptiveWidth = true,
  footerOption,
  onFocus,
}: ListBoxProps<T>) => {
  const virtuosoRef = useRef<VirtuosoHandle | null>(null);
  const selectedOption = options.find((option) => isEqual(option.value, selectedValue));
  const searchTerm = useRef("");

  const onOnSelect = (value: T) => {
    onSelect(value);
  };

  const ListBoxOption: React.FC<Option<T>> = ({ label, value, icon, disabled, ...restOptionProps }, index) => (
    <OriginalListboxOption
      as="li"
      key={typeof label === "string" ? label : index}
      value={value}
      disabled={disabled}
      className={classNames(styles.option, optionClassName, {
        [styles.disabled]: disabled,
      })}
      onClick={(e) => e.stopPropagation()}
      {...(restOptionProps["data-testid"] && {
        "data-testid": `${restOptionProps["data-testid"]}-option`,
      })}
    >
      {({ focus, selected }) => (
        <FlexContainer
          alignItems="center"
          className={classNames(styles.optionValue, selected && selectedOptionClassName, {
            [styles.focus]: focus,
            [styles.selected]: selected,
          })}
        >
          {icon && <FlexItem className={styles.icon}>{icon}</FlexItem>}
          <Text className={styles.label} as={optionTextAs}>
            {label}
          </Text>
        </FlexContainer>
      )}
    </OriginalListboxOption>
  );

  const isVirtualized = useMemo(() => options.length > MIN_OPTIONS_FOR_VIRTUALIZATION, [options.length]);
  const initialTopMostItemIndex: IndexLocationWithAlign | undefined = useMemo(() => {
    if (!isVirtualized || !selectedValue) {
      return;
    }

    const index = options.findIndex((option) => isEqual(option.value, selectedValue));
    return index === -1 ? undefined : { index, align: "center" };
  }, [isVirtualized, options, selectedValue]);

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
    <div
      className={className}
      {...(testId && {
        "data-testid": testId,
      })}
    >
      <Listbox value={selectedValue} onChange={onOnSelect} disabled={isDisabled} by={isEqual}>
        <Float
          adaptiveWidth={adaptiveWidth}
          placement={placement}
          flip={flip}
          offset={5} // $spacing-sm
          autoUpdate={{
            elementResize: false, // this will prevent render in wrong place after multiple open/close actions
          }}
        >
          <OriginalListboxButton
            className={classNames(buttonClassName, styles.button, { [styles["button--error"]]: hasError })}
            onClick={(e) => e.stopPropagation()}
            {...(testId && {
              "data-testid": `${testId}-listbox-button`,
            })}
            id={id}
            onFocus={onFocus}
          >
            <ControlButton selectedOption={selectedOption} isDisabled={isDisabled} />
          </OriginalListboxButton>
          <OriginalListboxOptions
            as="ul"
            modal={false}
            onKeyDown={isVirtualized ? handleKeydownForVirtualizedList : undefined}
            className={classNames(styles.optionsMenu, { [styles.nonAdaptive]: !adaptiveWidth }, optionsMenuClassName)}
          >
            {options.length && isVirtualized ? (
              <Virtuoso<Option<T>>
                style={{ height: "300px" }} // $height-long-listbox-options-list
                data={options}
                ref={virtuosoRef}
                increaseViewportBy={{ top: 100, bottom: 100 }}
                itemContent={(index, option) => <ListBoxOption key={index} {...option} />}
                {...(initialTopMostItemIndex && { initialTopMostItemIndex })} // scroll to selected value
              />
            ) : (
              options.map(ListBoxOption)
            )}
            {footerOption && (
              <OriginalListboxOption value={undefined} className={classNames(styles.option, optionClassName)}>
                {footerOption}
              </OriginalListboxOption>
            )}
          </OriginalListboxOptions>
        </Float>
      </Listbox>
    </div>
  );
};
