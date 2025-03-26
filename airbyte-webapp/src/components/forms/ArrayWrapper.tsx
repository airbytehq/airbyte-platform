import { Controller, useFormContext } from "react-hook-form";

import { TagInput } from "components/ui/TagInput";

import { FormValues } from "./Form";
import { ArrayControlProps, OmittableProperties } from "./FormControl";

export const ArrayWrapper = <T extends FormValues>({
  controlId,
  name,
  hasError,
  ...rest
}: Omit<ArrayControlProps<T>, OmittableProperties>) => {
  const { control } = useFormContext();

  return (
    <Controller
      name={name}
      control={control}
      render={({ field }) => (
        <TagInput
          {...rest}
          name={name}
          fieldValue={field.value ?? []}
          onChange={field.onChange}
          onBlur={field.onBlur}
          error={hasError}
        />
      )}
    />
  );
};
