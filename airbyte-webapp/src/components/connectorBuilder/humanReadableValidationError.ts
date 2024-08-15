import type { ValidationError } from "json-schema";

export const humanReadableError = (error: ValidationError): string => {
  const pathWithoutInstance = error.property.replace(/^instance\./, "");
  return `${pathWithoutInstance} ${error.message}`;
};
