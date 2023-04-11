import type { OmittableProperties, InputControlProps } from "./FormControl";

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
  const { register } = useFormContext();

  return <Input {...rest} {...register(name)} name={name} type={type} error={hasError} id={controlId} />;
};
