import { useEffect } from "react";
import { useFormContext } from "react-hook-form";
import { useIntl } from "react-intl";

import { RequestOptionInjectInto } from "core/api/types/ConnectorManifest";

import { BuilderField } from "./BuilderField";
import { InjectIntoValue, injectIntoOptions } from "../useBuilderValidationSchema";
import { useWatchWithPreview } from "../useBuilderWatch";

interface BuilderRequestInjectionProps {
  path: string;
  descriptor: string;
  label?: string;
  tooltip?: string;
  excludeValues?: InjectIntoValue[];
}

export const BuilderRequestInjection: React.FC<BuilderRequestInjectionProps> = ({
  path,
  descriptor,
  label,
  tooltip,
  excludeValues,
}) => {
  const { formatMessage } = useIntl();
  const { fieldValue: injectInto } = useWatchWithPreview({ name: `${path}.inject_into` });
  const { setValue } = useFormContext();

  useEffect(() => {
    if (injectInto === RequestOptionInjectInto.body_json) {
      setValue(`${path}.field_name`, undefined);
    } else {
      setValue(`${path}.field_path`, undefined);
    }
  }, [injectInto, path, setValue]);

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
        label={label || formatMessage({ id: "connectorBuilder.injectInto.label" })}
        tooltip={tooltip || formatMessage({ id: "connectorBuilder.injectInto.tooltip" }, { descriptor })}
      />
      {injectInto === RequestOptionInjectInto.body_json ? (
        <BuilderField
          type="array"
          path={`${path}.field_path`}
          itemType="string"
          label={formatMessage({ id: "connectorBuilder.injectInto.fieldPath.label" })}
          tooltip={formatMessage({ id: "connectorBuilder.injectInto.fieldPath.tooltip" }, { descriptor })}
        />
      ) : (
        injectInto !== "path" && (
          <BuilderField
            type="jinja"
            path={`${path}.field_name`}
            label={
              injectIntoOptions.find((option) => option.value === injectInto)?.fieldLabel ??
              formatMessage({ id: "connectorBuilder.injectInto.fieldName.label" })
            }
            tooltip={formatMessage({ id: "connectorBuilder.injectInto.fieldName.tooltip" }, { descriptor })}
          />
        )
      )}
    </>
  );
};
