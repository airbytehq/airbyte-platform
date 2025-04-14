import React from "react";

export interface Option<T> {
  label: React.ReactNode;
  value: T;
  icon?: React.ReactNode;
  disabled?: boolean;
  "data-testid"?: string;
}
