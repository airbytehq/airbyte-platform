import { ReactElement } from "react";
import { ZodSchema } from "zod";

import { AirbyteJsonSchema } from "../utils";

export interface BaseControlProps {
  name: string;
  label?: string;
  labelTooltip?: ReactElement;
  optional: boolean;
  header?: ReactElement;
  containerControlClassName?: string;
  onlyShowErrorIfTouched?: boolean;
  placeholder?: string;
  "data-field-path"?: string;
  disabled?: boolean;
  interpolationContext?: string[];
}

export type OverrideByPath = Record<string, (path: string) => ReactElement | null>;

type FieldName = string;
type ObjectType = string;
export type OverrideByObjectField = Record<
  ObjectType,
  {
    fieldOverrides: Record<FieldName, (path: string) => ReactElement | null>;
    validate?: ZodSchema;
  }
>;

export type OverrideByFieldSchema = Array<{
  shouldOverride: (schema: AirbyteJsonSchema) => boolean;
  renderOverride: (props: BaseControlProps) => ReactElement | null;
}>;

export interface BaseControlComponentProps {
  fieldSchema: AirbyteJsonSchema;
  baseProps: BaseControlProps;
  overrideByPath?: OverrideByPath;
  skipRenderedPathRegistration?: boolean;
  hideBorder?: boolean;
  nonAdvancedFields?: string[];
}
