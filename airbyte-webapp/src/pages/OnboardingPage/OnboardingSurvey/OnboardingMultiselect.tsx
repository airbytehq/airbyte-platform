import { Listbox } from "@headlessui/react";
import React, { useState } from "react";
import { useIntl } from "react-intl";

import { Badge } from "components/ui/Badge";
import { Box } from "components/ui/Box";
import { CheckBox } from "components/ui/CheckBox";
import { FlexContainer } from "components/ui/Flex";
import { Input } from "components/ui/Input";
import { FloatLayout } from "components/ui/ListBox/FloatLayout";
import { ListboxButton } from "components/ui/ListBox/ListboxButton";
import { ListboxOption } from "components/ui/ListBox/ListboxOption";
import { ListboxOptions } from "components/ui/ListBox/ListboxOptions";
import { Text } from "components/ui/Text";

import styles from "./OnboardingMultiselect.module.scss";

interface OptionWithInput {
  value: string;
  label: string;
  disabled?: boolean;
  showInput?: boolean;
}

export interface OnboardingMultiSelectProps {
  label: string;
  options: OptionWithInput[];
  selectedValues: string[];
  onSelectValues: (selectedValues: string[]) => void;
  showSelected?: boolean;
}

interface ShowSelectedValuesProps {
  selectedValues: string[];
  options: OptionWithInput[];
  label: string;
}

export const ShowSelectedValues = ({ selectedValues, options, label }: ShowSelectedValuesProps) => {
  const getDisplayLabel = (value: string) => {
    if (value.startsWith("other:")) {
      const customValue = value.replace("other:", "");
      const otherOption = options.find((opt) => opt.showInput);
      return customValue || otherOption?.label || "Other";
    }
    return options.find((option) => option.value === value)?.label || value;
  };

  if (selectedValues.length === 1) {
    return <>{getDisplayLabel(selectedValues[0])}</>;
  }
  if (selectedValues.length > 1) {
    const rest = selectedValues.length - 2;
    return (
      <span>
        {selectedValues.slice(0, 2).map((value, index) => (
          <Badge key={index} variant="blue">
            {getDisplayLabel(value)}
          </Badge>
        ))}
        {rest > 0 && <Badge variant="blue">+ {rest}</Badge>}
      </span>
    );
  }
  return <Text>{label}</Text>;
};

export const OnboardingMultiselect = ({
  options,
  selectedValues,
  onSelectValues,
  label,
  showSelected,
}: OnboardingMultiSelectProps) => {
  const otherOption = options.find((opt) => opt.showInput);
  const otherOptionValue = otherOption?.value;

  const existingOtherValue = selectedValues.find((v) => String(v).startsWith("other:"));
  const initialCustomInput = existingOtherValue ? existingOtherValue.replace("other:", "") : "";

  const [customInput, setCustomInput] = useState(initialCustomInput);
  const { formatMessage } = useIntl();

  const handleSelectChange = (values: string[]) => {
    if (!otherOptionValue) {
      onSelectValues(values);
      return;
    }

    const cleanedValues = values.filter((v) => v !== otherOptionValue && !String(v).startsWith("other:"));

    const otherSelected = values.includes(otherOptionValue);

    if (otherSelected) {
      if (customInput.trim()) {
        cleanedValues.push(`other:${customInput}`);
      } else {
        // Otherwise just use "other"
        cleanedValues.push(otherOptionValue);
      }
    }

    onSelectValues(cleanedValues);
  };

  const handleOtherInputChange = (value: string) => {
    setCustomInput(value);
    if (!otherOptionValue) {
      return;
    }

    const newValues = selectedValues.filter((v) => v !== otherOptionValue && !String(v).startsWith("other:"));

    if (value.trim()) {
      newValues.push(`other:${value}`);
    } else {
      newValues.push(otherOptionValue);
    }
    onSelectValues(newValues);
  };

  const isOtherSelected = otherOptionValue
    ? selectedValues.some((v) => v === otherOptionValue || String(v).startsWith("other:"))
    : false;
  const hasOtherOption = !!otherOption;

  return (
    <>
      <Listbox multiple value={selectedValues} onChange={handleSelectChange}>
        <FloatLayout shift={5}>
          <ListboxButton className={styles.multiSelect__button}>
            <FlexContainer as="span" wrap="wrap" gap="sm">
              {showSelected ? (
                <ShowSelectedValues selectedValues={selectedValues} options={options} label={label} />
              ) : (
                <>
                  <Text>{label}</Text>
                  {selectedValues.length > 0 && <Badge variant="blue">{selectedValues.length}</Badge>}
                </>
              )}
            </FlexContainer>
          </ListboxButton>
          <ListboxOptions className={styles.multiSelect__options}>
            {options.map(({ label, value, disabled }, index) => (
              <OnboardingMultiSelectOption label={label} value={value} key={index} disabled={disabled} />
            ))}
          </ListboxOptions>
        </FloatLayout>
      </Listbox>
      {hasOtherOption && isOtherSelected && (
        <Box mt="sm">
          <Input
            placeholder={formatMessage({ id: "onboarding.survey.pleaseSpecify" })}
            value={customInput}
            onChange={(e) => handleOtherInputChange(e.target.value)}
          />
        </Box>
      )}
    </>
  );
};

interface OnboardingMultiSelectOptionProps {
  label: React.ReactNode;
  value: string;
  disabled?: boolean;
}

const OnboardingMultiSelectOption = ({ label, value, disabled }: OnboardingMultiSelectOptionProps) => (
  <ListboxOption value={value} disabled={disabled}>
    {({ selected }) => (
      <Box p="md">
        <FlexContainer alignItems="center" as="span" gap="sm">
          <CheckBox checked={selected} readOnly />
          <Text>{label}</Text>
        </FlexContainer>
      </Box>
    )}
  </ListboxOption>
);
