import type { OmittableProperties, TextAreaControlProps } from "./FormControl";

import { useFormContext, useWatch } from "react-hook-form";

import { TextArea } from "components/ui/TextArea";

import { FormValues } from "./Form";

export const TextAreaWrapper = <T extends FormValues>({
  controlId,
  name,
  hasError,
  ...rest
}: Omit<TextAreaControlProps<T>, OmittableProperties>) => {
  const { register } = useFormContext();
  // If we don't watch the name explicitly, the textarea will not update
  // when its value is changed as a result of setting a parent object value.
  const value = useWatch({ name });

  return <TextArea {...rest} {...register(name)} value={value ?? ""} name={name} error={hasError} id={controlId} />;
};
