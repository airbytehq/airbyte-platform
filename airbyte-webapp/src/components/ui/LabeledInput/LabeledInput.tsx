import React from "react";

import { Input, InputProps } from "components/ui/Input";
import { ControlLabels, ControlLabelsProps } from "components/ui/LabeledControl";

type LabeledInputProps = Pick<ControlLabelsProps, "success" | "message" | "label"> &
  InputProps & { className?: string };

const LabeledInput: React.FC<LabeledInputProps> = ({ error, success, message, label, className, ...inputProps }) => (
  <ControlLabels error={error} success={success} message={message} label={label} className={className}>
    <Input {...inputProps} error={error} />
  </ControlLabels>
);

export default LabeledInput;
