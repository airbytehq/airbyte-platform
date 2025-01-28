import { Combobox, ComboboxButton, ComboboxInput, ComboboxOption, ComboboxOptions } from "@headlessui/react";
import classNames from "classnames";
import React, { ReactNode, useMemo, useState } from "react";
import { ControllerRenderProps, FieldValues } from "react-hook-form";
import { useIntl } from "react-intl";

import { Icon } from "components/ui/Icon";

import styles from "./ComboBox.module.scss";
import { Box } from "../Box";
import { FlexContainer } from "../Flex";
import { Input } from "../Input";
import { TagInput } from "../TagInput";
import { Text } from "../Text";

export interface Option {
  value: string;
  label?: string;
  iconLeft?: React.ReactNode;
  iconRight?: React.ReactNode;
  description?: string;
}

export interface OptionSection {
  sectionTitle?: string;
  innerOptions: Option[];
}

interface BaseProps {
  options: Option[] | OptionSection[];
  error?: boolean;
  fieldInputProps?: ControllerRenderProps<FieldValues, string>;
}

export interface OptionsConfig {
  loading?: boolean;
  loadingMessage?: ReactNode;
  instructionMessage?: ReactNode;
}

export interface OptionsProps extends OptionsConfig {
  optionSections: OptionSection[];
}

export interface ComboBoxProps extends BaseProps {
  value: string | undefined;
  onChange: (newValue: string) => void;
  /** overrides the caret down button */
  adornment?: ReactNode;
  onBlur?: React.FocusEventHandler<HTMLInputElement>;
  filterOptions?: boolean;
  disabled?: boolean;
  allowCustomValue?: boolean;
  optionsConfig?: OptionsConfig;
  "data-testid"?: string;
  placeholder?: string;
  className?: string;
}

export interface MultiComboBoxProps extends BaseProps {
  name: string;
  value: string[] | undefined;
  onChange: (newValue: string[]) => void;
  disabled?: boolean;
}

const ComboBoxOption = ({ option }: { option: Option }) => (
  <ComboboxOption as="li" value={option.value}>
    {({ focus, selected }) => (
      <FlexContainer
        gap="sm"
        className={classNames(styles.optionValue, { [styles.focus]: focus, [styles.selected]: selected })}
        alignItems="center"
      >
        {option.iconLeft}
        <FlexContainer alignItems="baseline">
          <Text size="md">{getLabel(option)}</Text>
          {option.description && (
            <Text size="sm" className={styles.description}>
              {option.description}
            </Text>
          )}
        </FlexContainer>
        {option.iconRight}
      </FlexContainer>
    )}
  </ComboboxOption>
);

const OptionsLoading = ({ message }: { message: ReactNode }) => {
  return (
    <FlexContainer
      gap="sm"
      className={classNames(styles.optionValue, styles.optionInstructions, styles.optionLoading)}
      alignItems="center"
    >
      <Icon type="loading" size="xs" color="disabled" />
      {message}
    </FlexContainer>
  );
};

const OptionsInstruction = ({ message }: { message: ReactNode }) => {
  return (
    <FlexContainer className={classNames(styles.optionValue, styles.optionInstructions)} alignItems="center">
      {message}
    </FlexContainer>
  );
};

const Options: React.FC<OptionsProps> = ({ optionSections, loadingMessage, instructionMessage, loading = false }) => {
  const { formatMessage } = useIntl();
  const defaultLoadingMessage = formatMessage({ id: "ui.loading" });
  const optionsList = optionSections.map(({ sectionTitle, innerOptions }, index) => (
    <FlexContainer direction="column" key={`${sectionTitle}_${index}`} gap="none">
      {sectionTitle && (
        <Box p="md">
          <Text size="sm" color="grey">
            {sectionTitle}
          </Text>
        </Box>
      )}
      {innerOptions.map((option) => (
        <ComboBoxOption key={option.value} option={option} />
      ))}
    </FlexContainer>
  ));

  if (optionSections.length === 0 && !loading) {
    return null;
  }

  if (loading) {
    optionsList.unshift(<OptionsLoading message={loadingMessage || defaultLoadingMessage} />);
  }

  if (instructionMessage) {
    optionsList.unshift(<OptionsInstruction message={instructionMessage} />);
  }

  return <>{optionsList}</>;
};

const normalizeOptionsAsSections = (options: Option[] | OptionSection[]): OptionSection[] => {
  if (options.length === 0) {
    return [];
  }

  if ("innerOptions" in options[0]) {
    return options as OptionSection[];
  }

  return [{ innerOptions: options as Option[] }];
};

function getLabel(option: Option): string {
  return option.label ?? option.value;
}

const findMatchingOption = (
  stringToMatch: string,
  matchType: "value" | "label",
  optionsSections: OptionSection[]
): Option | undefined => {
  for (const section of optionsSections) {
    const foundOption = section.innerOptions.find((option) =>
      matchType === "value" ? option.value === stringToMatch : getLabel(option) === stringToMatch
    );
    if (foundOption) {
      return foundOption;
    }
  }

  return undefined;
};

const filterOptionSectionsByQuery = (optionSections: OptionSection[], query: string): OptionSection[] => {
  return optionSections
    .map(({ sectionTitle, innerOptions }) => ({
      sectionTitle,
      innerOptions: innerOptions.filter((option) => getLabel(option).toLowerCase().includes(query.toLowerCase())),
    }))
    .filter(({ innerOptions }) => innerOptions.length > 0);
};

const isCustomValue = (value: string, optionSections: OptionSection[]) => {
  return !optionSections.some((optionSection) =>
    optionSection.innerOptions.some((option) => getLabel(option) === value)
  );
};

// the values and labels across all options should be unique!
export const ComboBox = ({
  options,
  value,
  onChange,
  error,
  adornment,
  onBlur,
  fieldInputProps,
  disabled,
  optionsConfig,
  filterOptions = true,
  allowCustomValue,
  "data-testid": testId,
  placeholder,
  className,
}: ComboBoxProps) => {
  // Stores the value that the user types in to filter the options
  const [query, setQuery] = useState("");

  const inputOptionSections = useMemo(() => normalizeOptionsAsSections(options), [options]);

  const currentInputValue = useMemo(() => {
    if (query) {
      return query;
    }

    const selectedOption = value ? findMatchingOption(value, "value", inputOptionSections) : undefined;
    if (selectedOption) {
      return getLabel(selectedOption);
    }

    if (allowCustomValue) {
      return value;
    }

    return "";
  }, [allowCustomValue, inputOptionSections, query, value]);

  const displayOptionSections = useMemo(() => {
    const nonEmptyOptionSections = inputOptionSections.filter((section) => section.innerOptions.length > 0);

    const filteredOptionSections =
      filterOptions && query ? filterOptionSectionsByQuery(nonEmptyOptionSections, query) : nonEmptyOptionSections;

    const shouldAddCustomValue =
      allowCustomValue && currentInputValue && isCustomValue(currentInputValue, filteredOptionSections);
    const customValueOption = shouldAddCustomValue ? [{ innerOptions: [{ value: currentInputValue }] }] : [];

    return [...customValueOption, ...filteredOptionSections];
  }, [filterOptions, query, inputOptionSections, allowCustomValue, currentInputValue]);

  return (
    <Combobox
      value={value ?? ""}
      onChange={(newValue) => onChange(newValue ?? "")}
      onClose={() => {
        setQuery("");
      }}
      immediate
      as="div"
      data-testid={testId}
      className={className}
    >
      <ComboboxInput as={React.Fragment}>
        <Input
          {...fieldInputProps}
          spellCheck={false}
          value={currentInputValue}
          error={error}
          adornment={
            adornment ?? (
              <ComboboxButton className={styles.caretButton} data-testid={testId ? `${testId}--button` : undefined}>
                <Icon type="caretDown" />
              </ComboboxButton>
            )
          }
          autoComplete="off"
          onChange={(event) => {
            const newQuery = event.target.value;
            setQuery(newQuery);

            const selectedOption = findMatchingOption(newQuery, "label", inputOptionSections);
            if (allowCustomValue) {
              onChange(selectedOption?.value ?? newQuery);
            } else if (selectedOption) {
              onChange(selectedOption.value);
            } else {
              onChange("");
            }
          }}
          onBlur={onBlur ? (e) => onBlur?.(e) : fieldInputProps?.onBlur}
          disabled={disabled}
          data-testid={testId ? `${testId}--input` : undefined}
          placeholder={placeholder}
        />
      </ComboboxInput>
      <ComboboxOptions as="ul" className={styles.optionsMenu} modal={false} anchor="bottom start">
        <Options optionSections={displayOptionSections} {...optionsConfig} />
      </ComboboxOptions>
    </Combobox>
  );
};

export const MultiComboBox = ({
  name,
  options,
  value,
  onChange,
  error,
  fieldInputProps,
  disabled,
}: MultiComboBoxProps) => (
  <Combobox value={value} onChange={onChange} multiple immediate>
    <ComboboxInput as={React.Fragment}>
      <TagInput
        name={name}
        fieldValue={value ?? []}
        onChange={onChange}
        onBlur={fieldInputProps?.onBlur}
        error={error}
        disabled={disabled}
      />
    </ComboboxInput>
    <ComboboxOptions as="ul" className={styles.optionsMenu} modal={false}>
      <Options optionSections={normalizeOptionsAsSections(options)} />
    </ComboboxOptions>
  </Combobox>
);
