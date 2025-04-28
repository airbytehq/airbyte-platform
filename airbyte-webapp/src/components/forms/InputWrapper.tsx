import type { OmittableProperties, InputControlProps } from "./FormControl";

import { useCallback } from "react";
import { useFormContext, useWatch } from "react-hook-form";

import { Input } from "components/ui/Input";

import { FormValues } from "./Form";

export const InputWrapper = <T extends FormValues>({
  controlId,
  name,
  type,
  hasError,
  onChange,
  ...rest
}: Omit<InputControlProps<T>, OmittableProperties>) => {
  const { setValue } = useFormContext<T>();
  // If we don't watch the name explicitly, the input will not update
  // when its value is changed as a result of setting a parent object value.
  const value = useWatch({ name });

  const handleChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      onChange?.(e);

      const value = e.target.value;
      if (type !== "number") {
        setValue(name, value as T[typeof name], { shouldValidate: true, shouldDirty: true, shouldTouch: true });
        return;
      }

      if (value === "") {
        // If empty, set the value to null, to distinguish it from an empty string.
        // The type cast is needed because React Hook Form expects a value of the field's type
        // but we're intentionally setting it to null to remove it.
        setValue(name, null as T[typeof name], { shouldValidate: true, shouldDirty: true, shouldTouch: true });
      } else {
        // Otherwise parse it as a number
        const numberValue = Number(value);
        setValue(name, numberValue as T[typeof name], { shouldValidate: true, shouldDirty: true, shouldTouch: true });
      }
    },
    [name, onChange, setValue, type]
  );

  // Don't use register here, since that causes incomplete numeric values like "12e" to be cleared out
  // before the user has a chance to complete the input, like "12e3"
  return (
    <Input
      {...rest}
      value={value ?? ""}
      onChange={handleChange}
      name={name}
      type={type}
      error={hasError}
      id={controlId}
    />
  );
};
