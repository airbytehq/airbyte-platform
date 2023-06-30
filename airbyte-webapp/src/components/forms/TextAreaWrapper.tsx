import type { OmittableProperties, TextAreaControlProps } from "./FormControl";

import { useFormContext } from "react-hook-form";

import { TextArea } from "components/ui/TextArea";

import { FormValues } from "./Form";

export const TextAreaWrapper = <T extends FormValues>({
  controlId,
  name,
  hasError,
  ...rest
}: Omit<TextAreaControlProps<T>, OmittableProperties>) => {
  const { register } = useFormContext();

  return <TextArea {...rest} {...register(name)} name={name} error={hasError} id={controlId} />;
};
