import type { OmittableProperties, SwitchControlProps } from "./FormControl";

import { Controller, useFormContext, useWatch } from "react-hook-form";

import { Switch } from "components/ui/Switch/Switch";

import { FormValues } from "./Form";

export const SwitchWrapper = <T extends FormValues>({
  controlId,
  name,
  disabled = false,
  onChange,
}: Omit<SwitchControlProps<T>, OmittableProperties>) => {
  const { control } = useFormContext();
  // If we don't watch the name explicitly, the switch will not update
  // when its value is changed as a result of setting a parent object value.
  const value = useWatch({ name });

  return (
    <Controller
      name={name}
      control={control}
      render={({ field }) => (
        <Switch
          disabled={disabled}
          name={name}
          id={controlId}
          checked={value}
          onChange={(value) => {
            field.onChange(value);
            onChange?.(value);
          }}
        />
      )}
    />
  );
};
