import type { OmittableProperties, InputControlProps } from "./FormControl";

import { useCallback } from "react";
import { useFormContext } from "react-hook-form";

import { Input } from "components/ui/Input";

import { FormValues } from "./Form";

export const InputWrapper = <T extends FormValues>({
  controlId,
  name,
  type,
  hasError,
  ...rest
}: Omit<InputControlProps<T>, OmittableProperties>) => {
  const { register, setValue } = useFormContext<T>();
  const { onChange, ...registerRest } = register(name);

  const handleChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const value = e.target.value;
      if (type !== "number") {
        onChange(e);
        return;
      }

      if (value === "") {
        // If empty, remove the value by setting it to undefined.
        // The type cast is needed because React Hook Form expects a value of the field's type
        // but we're intentionally setting it to undefined to remove it.
        setValue(name, undefined as T[typeof name], { shouldValidate: true, shouldDirty: true, shouldTouch: true });
      } else {
        // Otherwise parse it as a number
        const numberValue = parseFloat(value);
        setValue(name, numberValue as T[typeof name], { shouldValidate: true, shouldDirty: true, shouldTouch: true });
      }
    },
    [name, onChange, setValue, type]
  );

  return (
    <Input
      {...rest}
      {...registerRest}
      onChange={handleChange}
      name={name}
      type={type}
      error={hasError}
      id={controlId}
    />
  );
};
