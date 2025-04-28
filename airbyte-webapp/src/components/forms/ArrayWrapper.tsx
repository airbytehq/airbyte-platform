import { Controller, useFormContext, useWatch } from "react-hook-form";

import { TagInput } from "components/ui/TagInput";

import { FormValues } from "./Form";
import { ArrayControlProps, OmittableProperties } from "./FormControl";

export const ArrayWrapper = <T extends FormValues>({
  controlId,
  name,
  hasError,
  itemType,
  ...rest
}: Omit<ArrayControlProps<T>, OmittableProperties>) => {
  const { control } = useFormContext();
  // If we don't watch the name explicitly, the array input will not update
  // when its value is changed as a result of setting a parent object value.
  const value = useWatch({ name });

  return (
    <Controller
      name={name}
      control={control}
      render={({ field }) => (
        <TagInput
          {...rest}
          itemType={itemType}
          name={name}
          fieldValue={value ?? []}
          onChange={field.onChange}
          onBlur={field.onBlur}
          error={hasError}
        />
      )}
    />
  );
};
