import { Combobox, ComboboxInput, ComboboxButton, ComboboxOption, ComboboxOptions } from "@headlessui/react";
import { Float } from "@headlessui-float/react";
import { FloatProps } from "@headlessui-float/react/dist/float";
import classnames from "classnames";
import difference from "lodash/difference";
import isArray from "lodash/isArray";
import React, { ChangeEvent, useState } from "react";
import { useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { CheckBox } from "components/ui/CheckBox";
import { Option } from "components/ui/ComboBox";
import { FlexContainer } from "components/ui/Flex";
import { IconProps } from "components/ui/Icon";
import { Input } from "components/ui/Input";
import { RadioButton } from "components/ui/RadioButton";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import styles from "./CatalogComboBox.module.scss";
import { TextHighlighter } from "../TextHighlighter";

interface BaseProps {
  options: Option[];
  disabled?: boolean;
  buttonPlaceholder?: React.ReactNode;
  inputPlaceholder?: React.ReactNode;
  buttonAddText?: React.ReactNode;
  buttonEditText?: React.ReactNode;
  /**
   * if value is true, the button will have red color and buttonErrorText will be shown
   */
  error?: boolean;
  buttonErrorText?: React.ReactNode;
  /**
   * shared styles for control Button and Input (i.e width, height) to prevent jumping because of different content
   */
  controlClassName?: string;
  /**
   * Icon for the control button
   */
  controlBtnIcon: IconProps["type"];
  /**
   * Content for the tooltip
   */
  controlBtnTooltipContent?: React.ReactNode;
}

export interface CatalogComboBoxProps extends BaseProps {
  value: string | undefined;
  onChange: (newValue: string) => void;
}

export interface MultiCatalogComboBoxProps extends BaseProps {
  value: string[] | undefined;
  onChange: (newValue: string[]) => void;
  maxSelectedLabels?: number;
}

const transformStringToArray = (value: string, limit?: number): string[] =>
  value.split(",", limit).map((word) => word.trim());

// layout for options menu
const FloatLayout: React.FC<FloatProps> = ({ children, ...restProps }) => (
  <Float
    placement="bottom-start"
    flip={15}
    offset={5} // $spacing-sm
    autoUpdate={{
      elementResize: false, // this will prevent render in wrong place after multiple open/close actions
    }}
    {...restProps}
  >
    {children}
  </Float>
);

type ControlButtonProps = Omit<BaseProps, "options"> & {
  open: boolean;
  value: string | string[] | undefined;
  setFilterQuery: (value: string) => void;
  maxSelectedLabels?: number;
};
const ControlButton = React.forwardRef<HTMLButtonElement, ControlButtonProps>((props, ref) => {
  const { formatMessage } = useIntl();
  const {
    open,
    value,
    disabled,
    setFilterQuery,
    error,
    maxSelectedLabels = 3,
    buttonPlaceholder = formatMessage({ id: "ui.combobox.buttonPlaceholder" }),
    inputPlaceholder = formatMessage({ id: "ui.combobox.inputPlaceholder" }),
    buttonErrorText = formatMessage({ id: "ui.combobox.buttonErrorText" }),
    buttonAddText = formatMessage({ id: "ui.combobox.buttonAddText" }),
    buttonEditText = formatMessage({ id: "ui.combobox.buttonEditText" }),
    controlClassName,
    controlBtnIcon,
    controlBtnTooltipContent,
  } = props;

  const [isButtonHovered, setIsButtonHovered] = useState(false);

  const onInputChange = (event: ChangeEvent<HTMLInputElement>) => {
    // get the string user entered to filter options by ignoring the selected ones
    const [distinctValue] = difference(transformStringToArray(event.target.value), isArray(value) ? value : [value]);
    setFilterQuery(distinctValue ? distinctValue : "");
  };

  // show coma separated items + set the focus to the end of the input
  const displayValue = (items: string | string[]) =>
    items.length ? (isArray(items) ? `${items.join(", ")}, ` : `${items}`) : "";

  const getButtonText = () => {
    if (isButtonHovered) {
      return !value?.length ? buttonAddText : buttonEditText;
    }

    if (value?.length) {
      if (isArray(value)) {
        return maxSelectedLabels < value.length
          ? formatMessage({ id: "ui.combobox.amountOfSelectedOptions" }, { count: value.length })
          : value.join(", ");
      }
      return value;
    }

    return error ? buttonErrorText : buttonPlaceholder;
  };

  const onButtonHover = (value: boolean) => {
    if (!disabled) {
      setIsButtonHovered(value);
    }
  };

  const comboboxButton = (
    <ComboboxButton
      ref={ref}
      as={Button}
      type="button"
      variant="clear"
      full
      disabled={disabled}
      icon={isButtonHovered && !disabled ? (value?.length ? "pencil" : "plus") : controlBtnIcon}
      iconSize="sm"
      onMouseEnter={() => onButtonHover(true)}
      onMouseLeave={() => onButtonHover(false)}
      className={classnames(styles.buttonClear, { [styles.error]: error }, controlClassName)}
    >
      {getButtonText()}
    </ComboboxButton>
  );

  return (
    <>
      {open ? (
        <ComboboxInput
          as={Input}
          /* eslint-disable-next-line jsx-a11y/no-autofocus */
          autoFocus
          className={classnames(styles.input, controlClassName)}
          containerClassName={styles.inputContainer}
          onChange={onInputChange}
          displayValue={displayValue}
          placeholder={`${inputPlaceholder}`}
          onFocus={() => setIsButtonHovered(false)}
        />
      ) : controlBtnTooltipContent ? (
        <Tooltip placement="bottom" control={comboboxButton}>
          {controlBtnTooltipContent}
        </Tooltip>
      ) : (
        comboboxButton
      )}
    </>
  );
});
ControlButton.displayName = "ControlButton";

interface OptionsProps {
  options: Array<Option & { disabled?: boolean; disabledReason?: React.ReactNode }>;
  filterQuery: string;
  multiple?: boolean;
}
const Options = React.forwardRef(
  ({ options, filterQuery, multiple }: OptionsProps, ref: React.Ref<HTMLUListElement>) => {
    const { formatMessage } = useIntl();
    const filteredOptions =
      filterQuery === ""
        ? options
        : options.filter((option) => option.value.toLowerCase().includes(filterQuery.toLowerCase()));

    return (
      <ComboboxOptions ref={ref} className={styles.optionsMenu}>
        {filteredOptions.length === 0 ? (
          <FlexContainer className={styles.optionValue} alignItems="center">
            <Text size="md">{formatMessage({ id: "ui.combobox.noOptions" })}</Text>
          </FlexContainer>
        ) : (
          filteredOptions.map(({ disabled, disabledReason, value }) => (
            <ComboboxOption disabled={disabled} className={styles.option} key={value} value={value}>
              {({ focus, selected }) => {
                const control = (
                  <FlexContainer
                    className={classnames(styles.optionValue, {
                      [styles.focus]: focus,
                      [styles.selected]: selected,
                    })}
                    alignItems="center"
                  >
                    {multiple ? (
                      <CheckBox checkboxSize="sm" checked={selected} onClick={(e) => e.preventDefault()} readOnly />
                    ) : (
                      <RadioButton checked={selected} onClick={(e) => e.preventDefault()} readOnly />
                    )}
                    <Text size="md" color={disabled ? "grey" : "darkBlue"}>
                      <TextHighlighter searchWords={[filterQuery]} textToHighlight={value} />
                    </Text>
                  </FlexContainer>
                );
                return (
                  <Tooltip
                    placement="top"
                    disabled={!(disabled && disabledReason)}
                    containerClassName={styles.block}
                    control={control}
                  >
                    {disabledReason}
                  </Tooltip>
                );
              }}
            </ComboboxOption>
          ))
        )}
      </ComboboxOptions>
    );
  }
);
Options.displayName = "Options";

export const CatalogComboBox: React.FC<CatalogComboBoxProps> = ({ value, options, onChange, ...restControlProps }) => {
  const [filterQuery, setFilterQuery] = useState("");

  const onCloseOptionsMenu = () => {
    // reset filter value after closing the options menu
    if (filterQuery.length) {
      setFilterQuery("");
    }
  };

  return (
    <Combobox
      value={value}
      onChange={onChange}
      disabled={restControlProps.disabled}
      onClose={onCloseOptionsMenu}
      immediate
    >
      {({ open }) => (
        <FloatLayout>
          <ControlButton value={value} open={open} setFilterQuery={setFilterQuery} {...restControlProps} />
          <Options options={options} filterQuery={filterQuery} />
        </FloatLayout>
      )}
    </Combobox>
  );
};

export const MultiCatalogComboBox: React.FC<MultiCatalogComboBoxProps> = ({
  value,
  options,
  onChange,
  ...restControlProps
}) => {
  const [filterQuery, setFilterQuery] = useState("");
  const [selectedOptions, setSelectedOptions] = useState(value ?? []);

  const onSelectedOptionsChange = (selectedOptions: string[]) => {
    setSelectedOptions(selectedOptions);
    setFilterQuery("");
  };

  const onCloseOptionsMenu = () => {
    onChange(selectedOptions);
    // reset filter value after closing the options menu
    if (filterQuery.length) {
      setFilterQuery("");
    }
  };

  return (
    <Combobox
      value={selectedOptions}
      onChange={onSelectedOptionsChange}
      multiple
      disabled={restControlProps.disabled}
      onClose={onCloseOptionsMenu}
      immediate
    >
      {({ open }) => (
        <FloatLayout>
          <ControlButton open={open} value={selectedOptions} setFilterQuery={setFilterQuery} {...restControlProps} />
          <Options options={options} filterQuery={filterQuery} multiple />
        </FloatLayout>
      )}
    </Combobox>
  );
};
