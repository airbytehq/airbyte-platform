import type { OmittableProperties, SwitchControlProps } from "./FormControl";

import { Controller, useFormContext } from "react-hook-form";

import { Switch } from "components/ui/Switch/Switch";

import { FormValues } from "./Form";

export const SwitchWrapper = <T extends FormValues>({
  controlId,
  name,
  disabled = false,
}: Omit<SwitchControlProps<T>, OmittableProperties>) => {
  const { control } = useFormContext();

  return (
    <Controller
      name={name}
      control={control}
      render={({ field }) => (
        <Switch
          disabled={disabled}
          name={name}
          id={controlId}
          checked={field.value}
          onChange={(value) => field.onChange(value)}
        />
      )}
    />
  );
};
