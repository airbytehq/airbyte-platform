import React from "react";

import { IconType } from "../Icon";

export interface Option<T> {
  label: React.ReactNode;
  value: T;
  icon?: React.ReactNode | IconType;
  disabled?: boolean;
  "data-testid"?: string;
}
