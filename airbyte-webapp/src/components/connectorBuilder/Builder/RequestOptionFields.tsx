import { useFormContext, useWatch } from "react-hook-form";

import { BuilderField } from "./BuilderField";
import { BuilderFieldWithInputs } from "./BuilderFieldWithInputs";
import { injectIntoValues } from "../types";

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
        options={excludePathInjection ? injectIntoValues.filter((target) => target !== "path") : injectIntoValues}
        onChange={(newValue) => {
          if (newValue === "path") {
            setValue(path, { inject_into: newValue });
          }
        }}
        label="Inject into"
        tooltip={`Configures where the ${descriptor} should be set on the HTTP requests`}
      />
      {value !== "path" && (
        <BuilderFieldWithInputs
          type="string"
          path={`${path}.field_name`}
          label="Field name"
          tooltip={`Configures which key should be used in the location that the ${descriptor} is being injected into`}
        />
      )}
    </>
  );
};
