import { ReactElement } from "react";

import { AirbyteJsonSchema } from "../utils";

export interface BaseControlProps {
  name: string;
  label?: string;
  labelTooltip?: ReactElement;
  optional: boolean;
  header?: ReactElement;
}

export type OverrideByPath = Record<string, ReactElement | null>;

export interface BaseControlComponentProps {
  fieldSchema: AirbyteJsonSchema;
  baseProps: BaseControlProps;
  overrideByPath?: OverrideByPath;
  skipRenderedPathRegistration?: boolean;
  hideBorder?: boolean;
}
