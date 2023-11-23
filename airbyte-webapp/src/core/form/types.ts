import { JSONSchema7Type, JSONSchema7TypeName } from "json-schema";

import { AirbyteJSONSchema } from "../jsonSchema/types";

export interface GroupDetails {
  id: string;
  title?: string;
}

/**
 * When turning the JSON schema into `FormBlock`s,
 * some often used props are copied over for easy access.
 */
type FormRelevantJSONSchema = Pick<
  AirbyteJSONSchema,
  | "default"
  | "examples"
  | "description"
  | "pattern"
  | "order"
  | "const"
  | "title"
  | "airbyte_hidden"
  | "enum"
  | "format"
  | "always_show"
  | "pattern_descriptor"
  | "group"
  | "readOnly"
  | "display_type"
>;

interface FormItem extends FormRelevantJSONSchema {
  fieldKey: string;
  path: string;
  isRequired: boolean;
}

export interface FormBaseItem extends FormItem {
  _type: "formItem";
  type: JSONSchema7TypeName;
  /**
   * In case type is array, itemType specifies the type of the items
   * */
  itemType?: JSONSchema7TypeName;
  isSecret?: boolean;
  multiline?: boolean;
}

export interface FormGroupItem extends FormItem {
  _type: "formGroup";
  properties: FormBlock[];
}

export interface FormConditionItem extends FormItem {
  _type: "formCondition";
  conditions: FormGroupItem[];
  /**
   * The key of the const property describing which condition is selected (e.g. type)
   */
  selectionKey: string;
  /**
   * The possible values of the selectionKey property ordered in the same way as the conditions
   */
  selectionConstValues: JSONSchema7Type[];
}

export interface FormObjectArrayItem extends FormItem {
  _type: "objectArray";
  properties: FormGroupItem;
}

export type FormBlock = FormGroupItem | FormBaseItem | FormConditionItem | FormObjectArrayItem;

export const FORM_PATTERN_ERROR = "form.pattern.error";
