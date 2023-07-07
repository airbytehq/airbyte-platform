import type { OmittableProperties, DatepickerControlProps } from "./FormControl";

import { Controller, useFormContext } from "react-hook-form";

import DatePicker from "components/ui/DatePicker";

import { FormValues } from "./Form";

export const DatepickerWrapper = <T extends FormValues>({
  name,
  format = "date",
  hasError,
  ...rest
}: Omit<DatepickerControlProps<T>, OmittableProperties>) => {
  const { control } = useFormContext();

  return (
    <Controller
      name={name}
      control={control}
      render={({ field }) => (
        <DatePicker
          {...rest}
          value={field.value}
          onChange={field.onChange}
          withTime={format === "date-time"}
          error={hasError}
        />
      )}
    />
  );
};
