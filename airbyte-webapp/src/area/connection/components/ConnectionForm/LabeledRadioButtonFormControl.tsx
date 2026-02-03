import React from "react";
import { Controller, useFormContext } from "react-hook-form";

import { LabeledRadioButton, LabeledRadioButtonProps } from "components";

interface LabeledRadioButtonFormControlProps extends LabeledRadioButtonProps {
  controlId: string;
  name: string; // redeclare name to make it required
}

export const LabeledRadioButtonFormControl: React.FC<LabeledRadioButtonFormControlProps> = ({
  controlId,
  name,
  label,
  value,
  message,
  ...restProps
}) => {
  const { control } = useFormContext();

  return (
    <Controller
      name={name}
      control={control}
      render={({ field }) => (
        <LabeledRadioButton
          {...restProps}
          {...field}
          id={controlId}
          label={label}
          name={name}
          value={value}
          checked={field.value === value}
          message={message}
        />
      )}
    />
  );
};
