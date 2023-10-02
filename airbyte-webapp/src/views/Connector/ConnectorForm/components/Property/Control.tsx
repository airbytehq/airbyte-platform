import get from "lodash/get";
import React, { useCallback } from "react";
import { useController, useFormContext } from "react-hook-form";

import { DropDown } from "components/ui/DropDown";
import { Input } from "components/ui/Input";
import { Multiselect } from "components/ui/Multiselect";
import { TagInput } from "components/ui/TagInput/TagInput";
import { TextArea } from "components/ui/TextArea";

import { FormBaseItem } from "core/form/types";
import { isDefined } from "core/utils/common";
import { useOptionalDocumentationPanelContext } from "views/Connector/ConnectorDocumentationLayout/DocumentationPanelContext";

import SecretConfirmationControl from "./SecretConfirmationControl";

const DatePicker = React.lazy(() => import("components/ui/DatePicker"));

interface ControlProps {
  property: FormBaseItem;
  name: string;
  disabled?: boolean;
  error?: boolean;
}

export const Control: React.FC<ControlProps> = ({ property, name, disabled, error }) => {
  const {
    formState: { defaultValues: initialValues },
  } = useFormContext();
  const { field } = useController({ name });
  const setFocusedField = useOptionalDocumentationPanelContext()?.setFocusedField;

  const onChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => {
      field.onChange(e.target.value);
    },
    [field]
  );

  if (property.type === "array" && !property.enum) {
    return (
      <TagInput
        name={name}
        itemType={property.itemType}
        fieldValue={field.value === undefined ? [] : Array.isArray(field.value) ? field.value : [field.value]}
        onChange={(tagLabels) => {
          field.onChange(tagLabels);
        }}
        error={!!error}
        disabled={disabled || property.readOnly}
        onBlur={() => field.onBlur()}
        onFocus={() => setFocusedField?.(name)}
      />
    );
  }

  if (property.type === "array" && property.enum) {
    const data =
      property.enum?.length && typeof property.enum[0] !== "object"
        ? (property.enum as string[] | number[])
        : undefined;
    return (
      <Multiselect
        name={name}
        data={data}
        onChange={(dataItems) => field.onChange(dataItems)}
        value={field.value}
        disabled={disabled}
        readOnly={property.readOnly}
      />
    );
  }

  if (property.type === "string" && (property.format === "date-time" || property.format === "date")) {
    return (
      <DatePicker
        error={error}
        withTime={property.format === "date-time"}
        withPrecision={
          property.pattern?.endsWith(".[0-9]{3}Z$")
            ? "milliseconds"
            : property.pattern?.endsWith(".[0-9]{6}Z$")
            ? "microseconds"
            : undefined
        }
        onChange={field.onChange}
        value={field.value}
        disabled={disabled}
        readOnly={property.readOnly}
        onFocus={() => setFocusedField?.(name)}
        onBlur={() => field.onBlur()}
      />
    );
  }

  if (property.enum) {
    return (
      <DropDown
        {...field}
        options={property.enum.map((dataItem) => ({
          label: dataItem?.toString() ?? "",
          value: dataItem?.toString() ?? "",
        }))}
        onChange={(selectedItem) => selectedItem && field.onChange(selectedItem.value)}
        value={field.value}
        isDisabled={disabled || property.readOnly}
        onFocus={() => setFocusedField?.(name)}
        error={error}
      />
    );
  } else if (property.multiline && !property.isSecret) {
    return (
      <TextArea
        {...field}
        onChange={onChange}
        autoComplete="off"
        value={field.value ?? ""}
        rows={3}
        disabled={disabled}
        error={error}
        onUpload={(val) => field.onChange(val)}
        onFocus={() => setFocusedField?.(name)}
        readOnly={property.readOnly}
      />
    );
  } else if (property.isSecret) {
    const isFormInEditMode = isDefined(get(initialValues, name));
    return (
      <SecretConfirmationControl
        name={name}
        multiline={Boolean(property.multiline)}
        showButtons={isFormInEditMode}
        disabled={disabled || property.readOnly}
        onFocus={() => setFocusedField?.(name)}
        error={error}
        onChange={onChange}
      />
    );
  }
  const inputType = property.type === "integer" || property.type === "number" ? "number" : "text";

  return (
    <Input
      {...field}
      onChange={onChange}
      placeholder={inputType === "number" ? property.default?.toString() : undefined}
      autoComplete="off"
      type={inputType}
      value={field.value ?? ""}
      disabled={disabled}
      readOnly={property.readOnly}
      onFocus={() => setFocusedField?.(name)}
      error={error}
    />
  );
};
