import { ReactElement } from "react";

import { SchemaFormControl } from "./Controls/SchemaFormControl";
import { useSchemaForm } from "./SchemaForm";
import { getDeclarativeSchemaTypeValue } from "./utils";

type OverrideByPath = Record<string, ReactElement | null>;

/**
 * Component that renders form controls that haven't been rendered by other SchemaFormControl components
 */
export interface SchemaFormRemainingFieldsProps {
  /**
   * Path to the property in the schema. Empty string or undefined for root level properties.
   */
  path?: string;

  /**
   * Map of property paths to custom renderers, allowing override of specific fields.
   */
  overrideByPath?: OverrideByPath;
}

export const SchemaFormRemainingFields = ({ path = "", overrideByPath = {} }: SchemaFormRemainingFieldsProps) => {
  const { getSchemaAtPath, isPathRendered, registerRenderedPath } = useSchemaForm();

  registerRenderedPath(path);

  // Get the property at the specified path
  const targetProperty = getSchemaAtPath(path, true);

  // If object has no properties, nothing to render
  if (!targetProperty.properties) {
    return null;
  }

  // Render only properties that haven't been rendered yet
  return (
    <>
      {Object.entries(targetProperty.properties).map(([propertyName, property]) => {
        const fullPath = path ? `${path}.${propertyName}` : propertyName;

        // Skip if this path or any parent path has already been rendered
        if (isPathRendered(fullPath)) {
          return null;
        }

        // ~ declarative_component_schema type handling ~
        if (getDeclarativeSchemaTypeValue(propertyName, property)) {
          return null;
        }

        // Use skipRenderedPathRegistration=true to prevent double registration
        return (
          <SchemaFormControl
            key={fullPath}
            path={fullPath}
            overrideByPath={overrideByPath}
            skipRenderedPathRegistration
            isRequired={targetProperty.required?.includes(propertyName) ?? false}
          />
        );
      })}
    </>
  );
};
