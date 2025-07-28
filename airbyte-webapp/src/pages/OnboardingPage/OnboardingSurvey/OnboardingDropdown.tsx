import { Listbox } from "@headlessui/react";
import { useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Input } from "components/ui/Input";
import { FloatLayout } from "components/ui/ListBox/FloatLayout";
import { ListboxButton } from "components/ui/ListBox/ListboxButton";
import { ListboxOption } from "components/ui/ListBox/ListboxOption";
import { ListboxOptions } from "components/ui/ListBox/ListboxOptions";
import { Text } from "components/ui/Text";

import { SurveyOption } from "./onboarding_survey_definition";
import styles from "./OnboardingDropdown.module.scss";

export interface OnboardingDropdownProps {
  options: SurveyOption[];
  selectedValue: string;
  onSelect: (value: string) => void;
}

export const OnboardingDropdown = ({ options, selectedValue, onSelect }: OnboardingDropdownProps) => {
  const [otherInputValue, setOtherInputValue] = useState("");
  const { formatMessage } = useIntl();

  const otherOption = options.find((opt) => opt.showInput);
  const otherOptionValue = otherOption?.value;

  const isOtherValue = selectedValue.startsWith("other:");
  const hasOtherOption = !!otherOption;

  const displayInputValue = isOtherValue ? selectedValue.replace("other:", "") : otherInputValue;

  const displayValue = isOtherValue ? otherOptionValue : selectedValue;

  const handleSelect = (value: string) => {
    if (value === otherOptionValue) {
      onSelect(displayInputValue.trim() ? `other:${displayInputValue}` : otherOptionValue);
    } else {
      onSelect(value);
      setOtherInputValue("");
    }
  };

  const handleOtherInputChange = (value: string) => {
    setOtherInputValue(value);
    if (value.trim()) {
      onSelect(`other:${value}`);
    } else {
      onSelect(otherOptionValue || "other");
    }
  };

  // Get the label for the selected value
  const getSelectedLabel = () => {
    if (!selectedValue) {
      return <FormattedMessage id="onboarding.survey.selectOption" />;
    }

    if (isOtherValue) {
      const customValue = selectedValue.replace("other:", "");
      return `${otherOption?.label}: ${customValue}`;
    }

    const selectedOption = options.find((opt) => opt.value === selectedValue);
    return selectedOption?.label || selectedValue;
  };

  return (
    <>
      <Listbox value={displayValue} onChange={handleSelect}>
        <FloatLayout shift={5}>
          <ListboxButton className={styles.dropdown__button}>
            <FlexContainer justifyContent="space-between" alignItems="center">
              <Text>{getSelectedLabel()}</Text>
            </FlexContainer>
          </ListboxButton>
          <ListboxOptions>
            {options.map(({ label, value }) => (
              <ListboxOption key={value} value={value}>
                {({ selected }) => (
                  <Box p="md" className={selected ? styles.selectedOption : ""}>
                    <Text>{label}</Text>
                  </Box>
                )}
              </ListboxOption>
            ))}
          </ListboxOptions>
        </FloatLayout>
      </Listbox>
      {hasOtherOption && (selectedValue === otherOptionValue || isOtherValue) && (
        <Box mt="sm">
          <Input
            placeholder={formatMessage({ id: "onboarding.survey.pleaseSpecify" })}
            value={displayInputValue}
            onChange={(e) => handleOtherInputChange(e.target.value)}
          />
        </Box>
      )}
    </>
  );
};
