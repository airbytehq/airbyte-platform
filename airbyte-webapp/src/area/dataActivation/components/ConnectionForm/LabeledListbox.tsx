import { Listbox } from "@headlessui/react";
import isEqual from "lodash/isEqual";

import { FormControlErrorMessage } from "components/forms/FormControl";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
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
}

export const LabeledListbox = <T,>({
  fieldName,
  hasError,
  iconType,
  onChange,
  options,
  label,
  value,
}: LabeledListboxProps<T>) => {
  return (
    <FlexContainer direction="column" gap="xs" className={styles.labeledListbox}>
      <Listbox value={value} onChange={onChange}>
        <FloatLayout adaptiveWidth>
          <ListboxButton hasError={hasError} className={styles.labeledListbox__button}>
            {iconType && (
              <Icon type={iconType} className={styles.labeledListbox__icon} color={hasError ? "error" : undefined} />
            )}
            {!!value && (
              <Text className={styles.labeledListbox__value} color={hasError ? "red" : undefined}>
                {options.find((option) => isEqual(option.value, value))?.label}
              </Text>
            )}

            <Text className={styles.labeledListbox__label} color={hasError ? "red" : "grey"}>
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
