import { Controller, useFormContext, useWatch } from "react-hook-form";

import { ListBox } from "components/ui/ListBox";

import { FormValues } from "./Form";
import { SelectControlProps, OmittableProperties } from "./FormControl";
import styles from "./SelectWrapper.module.scss";

export const SelectWrapper = <T extends FormValues>({
  controlId,
  hasError,
  name,
  disabled = false,
  options,
  ...rest
}: Omit<SelectControlProps<T>, OmittableProperties>) => {
  const { control } = useFormContext();
  // If we don't watch the name explicitly, the listbox will not update
  // when its value is changed as a result of setting a parent object value.
  const value = useWatch({ name });

  return (
    <Controller
      name={name}
      control={control}
      render={({ field }) => (
        <ListBox
          isDisabled={disabled}
          options={options}
          hasError={hasError}
          onSelect={(value) => field.onChange(value)}
          selectedValue={value}
          buttonClassName={styles.select}
          {...rest}
          id={controlId}
        />
      )}
    />
  );
};
