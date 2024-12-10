import { Combobox, ComboboxButton, ComboboxInput, ComboboxOption, ComboboxOptions } from "@headlessui/react";
import { Float } from "@headlessui-float/react";
import classNames from "classnames";
import { Fragment, useState } from "react";
import { Controller, FieldValues, Path, useFormContext } from "react-hook-form";
import { useIntl } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Input } from "components/ui/Input";
import { Text } from "components/ui/Text";

import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";

import styles from "./SelectTargetField.module.scss";

export interface SelectFieldOption {
  fieldName: string;
  fieldType: string;
  airbyteType?: string;
}

interface SelectTargetFieldProps<TFormValues> {
  targetFieldOptions: SelectFieldOption[];
  name: Path<TFormValues>;
}

export const SelectTargetField = <TFormValues extends FieldValues>({
  targetFieldOptions,
  name,
}: SelectTargetFieldProps<TFormValues>) => {
  const { control } = useFormContext<TFormValues>();

  return (
    <Controller
      name={name}
      control={control}
      render={({ field }) => (
        <ComboBox
          options={targetFieldOptions}
          selectedFieldName={field.value}
          onSelectField={(value) => {
            field.onChange(value);
            // We're using onBlur mode, so we need to manually trigger the validation
            field.onBlur();
          }}
        />
      )}
    />
  );
};

export interface TargetFieldOption {
  fieldName: string;
  fieldType: string;
  airbyteType?: string;
}

interface FieldComboBoxProps {
  onSelectField: (fieldName: string) => void;
  options: TargetFieldOption[];
  selectedFieldName?: string;
}

const ComboBox: React.FC<FieldComboBoxProps> = ({ onSelectField, options, selectedFieldName }) => {
  const { mode } = useConnectionFormService();
  const [query, setQuery] = useState<string>("");
  const { formatMessage } = useIntl();

  const filteredOptions = query === "" ? options : options.filter((option) => option.fieldName.includes(query));

  const handleFieldNameSelect = (fieldName: string) => {
    onSelectField(fieldName);
  };

  return (
    <Combobox
      as="div"
      disabled={mode === "readonly"}
      immediate
      value={selectedFieldName ?? ""}
      onChange={handleFieldNameSelect}
      onClose={() => setQuery("")}
    >
      <Float adaptiveWidth placement="bottom-start" as={Fragment}>
        <ComboboxInput as={Fragment}>
          <Input
            spellCheck={false}
            autoComplete="off"
            aria-label={formatMessage({ id: "connections.mappings.fieldName" })}
            placeholder={formatMessage({ id: "connections.mappings.selectField" })}
            value={query || selectedFieldName || ""}
            className={styles.comboboxInput}
            onChange={(e) => setQuery(e.target.value)}
            adornment={
              <ComboboxButton className={styles.caretButton}>
                <Icon type="caretDown" />
              </ComboboxButton>
            }
          />
        </ComboboxInput>
        <ComboboxOptions className={styles.comboboxOptions} as="ul">
          {filteredOptions.map(({ fieldName, fieldType, airbyteType }) => (
            <ComboboxOption as="li" key={fieldName} value={fieldName}>
              {({ selected }) => (
                <FlexContainer
                  direction="row"
                  className={classNames(styles.comboboxOption, { [styles.selected]: selected })}
                >
                  <Text>{fieldName}</Text>
                  <Text color="grey500" italicized>
                    {airbyteType || fieldType}
                  </Text>
                </FlexContainer>
              )}
            </ComboboxOption>
          ))}
        </ComboboxOptions>
      </Float>
    </Combobox>
  );
};
