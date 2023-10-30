import { Controller, useFormContext } from "react-hook-form";

import { ListBox } from "components/ui/ListBox";

import { FormValues } from "./Form";
import { SelectControlProps, OmittableProperties } from "./FormControl";
import styles from "./SelectWrapper.module.scss";

export const SelectWrapper = <T extends FormValues>({
  hasError,
  name,
  disabled = false,
  options,
  ...rest
}: Omit<SelectControlProps<T>, OmittableProperties>) => {
  const { control } = useFormContext();

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
          selectedValue={field.value}
          className={styles.select}
          {...rest}
        />
      )}
    />
  );
};
