import { Combobox, ComboboxButton, ComboboxInput, ComboboxOption, ComboboxOptions } from "@headlessui/react";
import classNames from "classnames";
import { Fragment, useState } from "react";
import { Controller, FieldValues, Path, get, useFormContext, useFormState } from "react-hook-form";
import { useIntl } from "react-intl";

import { FormControlErrorMessage } from "components/forms/FormControl";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Input } from "components/ui/Input";
import { FloatLayout } from "components/ui/ListBox/FloatLayout";
import { Text } from "components/ui/Text";

import { useCurrentWorkspace } from "core/api";
import { FieldSpec } from "core/api/types/AirbyteClient";
import { useFormMode } from "core/services/ui/FormModeContext";
import { useIntent } from "core/utils/rbac";

import { MappingRowItem } from "./MappingRow";
import styles from "./SelectTargetField.module.scss";
import { useGetFieldsForMapping } from "./useGetFieldsInStream";

interface SelectTargetFieldProps<TFormValues> {
  mappingId: string;
  streamDescriptorKey: string;
  shouldLimitTypes?: boolean;
  name: Path<TFormValues>;
  disabled: boolean;
}

export const SelectTargetField = <TFormValues extends FieldValues>({
  ...props
}: SelectTargetFieldProps<TFormValues>) => {
  const { workspaceId } = useCurrentWorkspace();
  const canEditConnection = useIntent("EditConnection", { workspaceId });
  const { control } = useFormContext<TFormValues>();

  return (
    <>
      {canEditConnection ? (
        <SelectTargetFieldContent {...props} />
      ) : (
        <MappingRowItem>
          <Controller
            name={props.name}
            control={control}
            render={({ field }) => (
              <FieldComboBox
                disabled
                options={[]}
                selectedFieldName={field.value}
                onSelectField={() => {
                  return null;
                }}
              />
            )}
          />
        </MappingRowItem>
      )}
    </>
  );
};

export const SelectTargetFieldContent = <TFormValues extends FieldValues>({
  mappingId,
  streamDescriptorKey,
  name,
  shouldLimitTypes,
  disabled,
}: SelectTargetFieldProps<TFormValues>) => {
  const fieldsInStream = useGetFieldsForMapping(streamDescriptorKey, mappingId);
  const { control } = useFormContext<TFormValues>();
  const { errors } = useFormState<TFormValues>({ name });
  const error = get(errors, name);

  return (
    <MappingRowItem>
      <Controller
        name={name}
        control={control}
        render={({ field }) => (
          <FieldComboBox
            hasError={!!error}
            disabled={disabled}
            options={fieldsInStream}
            selectedFieldName={field.value}
            shouldLimitTypes={shouldLimitTypes}
            onSelectField={(value) => {
              field.onChange(value ?? "");
              // We're using onBlur mode, so we need to manually trigger the validation
              field.onBlur();
            }}
          />
        )}
      />
      <FormControlErrorMessage name={name} />
    </MappingRowItem>
  );
};

export interface TargetFieldOption {
  fieldName: string;
  fieldType: string;
  airbyteType?: string;
}

interface FieldComboBoxProps {
  onSelectField: (fieldName: string) => void;
  options: FieldSpec[];
  selectedFieldName?: string;
  shouldLimitTypes?: boolean;
  hasError?: boolean;
  disabled: boolean;
}

const FieldComboBox: React.FC<FieldComboBoxProps> = ({
  onSelectField,
  options,
  selectedFieldName,
  shouldLimitTypes,
  hasError = false,
  disabled,
}) => {
  const { mode } = useFormMode();
  const [query, setQuery] = useState<string>("");
  const { formatMessage } = useIntl();

  const filteredOptions = query === "" ? options : options.filter((option) => option.name.includes(query));

  const handleFieldNameSelect = (name: string) => {
    onSelectField(name);
  };

  return (
    <Combobox
      as="div"
      disabled={mode === "readonly" || disabled}
      value={selectedFieldName ?? ""}
      onChange={handleFieldNameSelect}
      onClose={() => setQuery("")}
      immediate
    >
      <FloatLayout adaptiveWidth flip={5} offset={-10}>
        <ComboboxInput as={Fragment}>
          <Input
            disabled={disabled || mode === "readonly"}
            error={hasError}
            spellCheck={false}
            autoComplete="off"
            aria-label={formatMessage({ id: "connections.mappings.fieldName" })}
            placeholder={formatMessage({ id: "connections.mappings.selectField" })}
            value={query || selectedFieldName || ""}
            containerClassName={classNames(styles.comboboxInput, {
              [styles.disabled]: disabled || mode === "readonly",
            })}
            onChange={(e) => setQuery(e.target.value)}
            adornment={
              <ComboboxButton
                className={styles.caretButton}
                aria-label={formatMessage({ id: "connections.mappings.selectField" })}
              >
                <Icon type="caretDown" />
              </ComboboxButton>
            }
          />
        </ComboboxInput>
        <ComboboxOptions className={styles.comboboxOptions} as="ul">
          {filteredOptions.map(({ name, type }) => (
            <ComboboxOption
              as="li"
              key={name}
              value={name}
              disabled={shouldLimitTypes && type !== "STRING" && type !== "NUMBER"}
            >
              {({ selected, disabled }) => (
                <FlexContainer
                  direction="row"
                  className={classNames(styles.comboboxOption, {
                    [styles.selected]: selected,
                    [styles.disabled]: disabled,
                  })}
                >
                  <Text>{name}</Text>
                  <Text color="grey500" italicized>
                    {type}
                  </Text>
                </FlexContainer>
              )}
            </ComboboxOption>
          ))}
        </ComboboxOptions>
      </FloatLayout>
    </Combobox>
  );
};
