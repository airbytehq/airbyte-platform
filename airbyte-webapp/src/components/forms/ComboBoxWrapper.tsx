import { Controller, useFormContext, useWatch } from "react-hook-form";

import { ComboBox } from "components/ui/ComboBox";

import { FormValues } from "./Form";
import { OmittableProperties, ComboboxControlProps } from "./FormControl";

export const ComboBoxWrapper = <T extends FormValues>({
  controlId,
  hasError,
  name,
  disabled = false,
  options,
  ...rest
}: Omit<ComboboxControlProps<T>, OmittableProperties>) => {
  const { control } = useFormContext();
  // If we don't watch the name explicitly, the combobox will not update
  // when its value is changed as a result of setting a parent object value.
  const value = useWatch({ name });

  return (
    <Controller
      name={name}
      control={control}
      render={({ field }) => (
        <ComboBox
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
