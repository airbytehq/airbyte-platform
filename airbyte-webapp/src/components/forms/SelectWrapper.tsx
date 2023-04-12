import { Controller, useFormContext } from "react-hook-form";

import { ListBox } from "components/ui/ListBox";

import { FormValues } from "./Form";
import { SelectControlProps, OmittableProperties } from "./FormControl";

export const SelectWrapper = <T extends FormValues>({
  hasError,
  name,
  ...rest
}: Omit<SelectControlProps<T>, OmittableProperties>) => {
  const { control } = useFormContext();

  return (
    <Controller
      name={name}
      control={control}
      render={({ field }) => (
        <ListBox
          options={rest.options}
          hasError={hasError}
          onSelect={(value) => field.onChange(value)}
          selectedValue={field.value}
        />
      )}
    />
  );
};
