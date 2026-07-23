import { Controller, useFormContext, useWatch } from "react-hook-form";

import { MultiComboBox } from "components/ui/ComboBox";

import { FormValues } from "./Form";
import { OmittableProperties, MultiComboboxControlProps } from "./FormControl";

export const MultiComboBoxWrapper = <T extends FormValues>({
  controlId,
  hasError,
  name,
  disabled = false,
  options,
  ...rest
}: Omit<MultiComboboxControlProps<T>, OmittableProperties>) => {
  const { control } = useFormContext();
  // If we don't watch the name explicitly, the MultiCombobox will not update
  // when its value is changed as a result of setting a parent object value.
  const value = useWatch({ name });

  return (
    <Controller
      name={name}
      control={control}
      render={({ field }) => (
        <MultiComboBox
          name={name.toString()}
          disabled={disabled}
          options={options}
          error={hasError}
          value={value}
          {...rest}
          id={controlId}
          onChange={field.onChange}
        />
      )}
    />
  );
};
