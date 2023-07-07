import { useFormContext, useWatch } from "react-hook-form";

import { BuilderField } from "./BuilderField";
import { BuilderFieldWithInputs } from "./BuilderFieldWithInputs";
import { injectIntoOptions } from "../types";

interface RequestOptionFieldsProps {
  path: string;
  descriptor: string;
  excludePathInjection?: boolean;
}

export const RequestOptionFields: React.FC<RequestOptionFieldsProps> = ({ path, descriptor, excludePathInjection }) => {
  const value = useWatch({ name: `${path}.inject_into` });
  const { setValue } = useFormContext();

  return (
    <>
      <BuilderField
        type="enum"
        path={`${path}.inject_into`}
        options={
          excludePathInjection ? injectIntoOptions.filter((target) => target.value !== "path") : injectIntoOptions
        }
        onChange={(newValue) => {
          if (newValue === "path") {
            setValue(path, { inject_into: newValue });
          }
        }}
        label="Inject Into"
        tooltip={`Configures where the ${descriptor} should be set on the HTTP requests`}
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
