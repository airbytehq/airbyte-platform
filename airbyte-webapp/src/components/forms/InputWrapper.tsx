import type { OmittableProperties, InputControlProps } from "./FormControl";

import { useState } from "react";
import { useFormContext, useWatch } from "react-hook-form";
import { useUpdateEffect } from "react-use";

import { Input } from "components/ui/Input";

import { FormValues } from "./Form";

export const InputWrapper = <T extends FormValues>({
  controlId,
  name,
  type,
  hasError,
  onBlur,
  ...rest
}: Omit<InputControlProps<T>, OmittableProperties>) => {
  const { register } = useFormContext<T>();
  const {
    onChange,
    onBlur: onBlurRegister,
    ...restRegister
  } = register(name, {
    setValueAs: (v) => {
      if (type !== "number") {
        return v;
      }
      if (v === null) {
        return null;
      }
      if (typeof v === "string" && v.trim() === "") {
        return null;
      }
      return isNaN(Number(v)) ? v : Number(v);
    },
  });

  // If we don't watch the name explicitly, the input will not update
  // when its value is changed as a result of setting a parent object value.
  const formValue = useWatch({ name });
  const [inputValue, setInputValue] = useState<string | undefined>(formValue ?? "");

  useUpdateEffect(() => {
    if (type === "number") {
      const formValueNumber = Number(formValue);
      const inputValueNumber = Number(inputValue);
      // Prevents typing "12e1" causing the input to change to "120" immediately
      if (!isNaN(formValueNumber) && !isNaN(inputValueNumber) && formValueNumber === inputValueNumber) {
        return;
      }
      setInputValue(formValue ?? "");
    } else {
      setInputValue(formValue ?? "");
    }
  }, [formValue]);

  return (
    <Input
      {...rest}
      {...restRegister}
      onChange={(e) => {
        setInputValue(e.target.value);
        onChange(e);
      }}
      onBlur={(e) => {
        onBlurRegister(e);
        onBlur?.(e);
      }}
      value={inputValue}
      name={name}
      type={type === "number" ? "text" : type}
      error={hasError}
      id={controlId}
    />
  );
};
