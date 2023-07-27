import { useFormContext, useWatch } from "react-hook-form";

import { BuilderField } from "./BuilderField";
import { BuilderFieldWithInputs } from "./BuilderFieldWithInputs";
import { InjectIntoValue, injectIntoOptions } from "../types";

interface InjectIntoFieldsProps {
  path: string;
  descriptor: string;
  label?: string;
  tooltip?: string;
  excludeValues?: InjectIntoValue[];
}

export const InjectIntoFields: React.FC<InjectIntoFieldsProps> = ({
  path,
  descriptor,
  label,
  tooltip,
  excludeValues,
}) => {
  const value = useWatch({ name: `${path}.inject_into` });
  const { setValue } = useFormContext();

  return (
    <>
      <BuilderField
        type="enum"
        path={`${path}.inject_into`}
        options={
          excludeValues
            ? injectIntoOptions.filter((target) => !excludeValues.includes(target.value))
            : injectIntoOptions
        }
        onChange={(newValue) => {
          if (newValue === "path") {
            setValue(path, { inject_into: newValue });
          }
        }}
        label={label || "Inject Into"}
        tooltip={tooltip || `Configures where the ${descriptor} should be set on the HTTP requests`}
      />
      {value !== "path" && (
        <BuilderFieldWithInputs
          type="string"
          path={`${path}.field_name`}
          label={injectIntoOptions.find((option) => option.value === value)?.fieldLabel ?? "Field Name"}
          tooltip={`Configures which key should be used in the location that the ${descriptor} is being injected into`}
        />
      )}
    </>
  );
};
