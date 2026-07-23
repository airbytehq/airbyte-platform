import { Listbox } from "@headlessui/react";
import isEqual from "lodash/isEqual";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { FormControlErrorMessage } from "components/ui/forms/FormControl";
import { Icon, IconType } from "components/ui/Icon";
import { FloatLayout } from "components/ui/ListBox/FloatLayout";
import { ListboxButton } from "components/ui/ListBox/ListboxButton";
import { ListboxOption } from "components/ui/ListBox/ListboxOption";
import { ListboxOptions } from "components/ui/ListBox/ListboxOptions";
import { Text } from "components/ui/Text";

import styles from "./LabeledListbox.module.scss";

interface LabeledListboxProps<T> {
  value: T;
  onChange: (value: T) => void;
  options: Array<{ label: string; value: T; disabled?: boolean }>;
  label: string;
  hasError: boolean;
  iconType?: IconType;
  fieldName: string;
  disabled?: boolean;
}

export const LabeledListbox = <T,>({
  fieldName,
  hasError,
  iconType,
  onChange,
  options,
  label,
  value,
  disabled,
}: LabeledListboxProps<T>) => {
  const iconColor = disabled ? "disabled" : hasError ? "error" : undefined;
  const textColor = disabled ? "grey300" : hasError ? "red" : undefined;
  const labelTextColor = disabled ? "grey300" : hasError ? "red" : "grey";

  return (
    <FlexContainer direction="column" gap="xs" className={styles.labeledListbox}>
      <Listbox value={value} onChange={onChange} disabled={disabled} by={isEqual}>
        <FloatLayout adaptiveWidth>
          <ListboxButton hasError={hasError} className={styles.labeledListbox__button} disabled={disabled}>
            {iconType && <Icon type={iconType} className={styles.labeledListbox__icon} color={iconColor} />}
            {!!value && (
              <Text className={styles.labeledListbox__value} color={textColor}>
                {options.find((option) => isEqual(option.value, value))?.label}
              </Text>
            )}

            <Text className={styles.labeledListbox__label} color={labelTextColor}>
              {label}
            </Text>
          </ListboxButton>
          <ListboxOptions>
            {options.map((option) => (
              <ListboxOption
                key={typeof option.value === "string" ? option.value : JSON.stringify(option.value)}
                value={option.value}
                disabled={option.disabled}
              >
                <Box p="md" pr="none" as="span">
                  <Text className={styles.labeledListbox__optionLabel}>{option.label}</Text>
                </Box>
              </ListboxOption>
            ))}
          </ListboxOptions>
        </FloatLayout>
      </Listbox>
      {hasError && <FormControlErrorMessage name={fieldName} />}
    </FlexContainer>
  );
};
