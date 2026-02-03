import type { OmittableProperties, DatepickerControlProps } from "./FormControl";

import { Controller, useFormContext, useWatch } from "react-hook-form";

import DatePicker from "components/ui/DatePicker";

import { FormValues } from "./Form";

export const DatepickerWrapper = <T extends FormValues>({
  name,
  format = "date",
  hasError,
  controlId,
  ...rest
}: Omit<DatepickerControlProps<T>, OmittableProperties>) => {
  const { control } = useFormContext();
  // If we don't watch the name explicitly, the datepicker will not update
  // when its value is changed as a result of setting a parent object value.
  const value = useWatch({ name });

  return (
    <Controller
      name={name}
      control={control}
      render={({ field }) => (
        <DatePicker
          {...rest}
          id={controlId}
          value={value ?? ""}
          onChange={field.onChange}
          withTime={format === "date-time"}
          error={hasError}
        />
      )}
    />
  );
};
