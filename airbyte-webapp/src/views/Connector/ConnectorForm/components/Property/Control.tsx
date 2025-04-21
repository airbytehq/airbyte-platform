import get from "lodash/get";
import React, { useCallback } from "react";
import { useController, useFormContext, useWatch } from "react-hook-form";

import { Input } from "components/ui/Input";
import { ListBox } from "components/ui/ListBox";
import { MultiSelectTags } from "components/ui/MultiSelectTags";
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
  // use value from useWatch since this will respect updates made to parent paths in the form
  const fieldValue = useWatch({ name });
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
        itemType={property.itemType === "number" ? "number" : property.itemType === "integer" ? "integer" : "string"}
        fieldValue={fieldValue === undefined ? [] : Array.isArray(fieldValue) ? fieldValue : [fieldValue]}
        onChange={(tagLabels: string[] | number[]) => {
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
      <MultiSelectTags
        options={data?.map((item) => ({ label: String(item), value: item })) ?? []}
        onSelectValues={(selectedValues) => field.onChange(selectedValues)}
        selectedValues={fieldValue}
        testId={name}
        disabled={disabled || property.readOnly}
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
        value={fieldValue}
        disabled={disabled}
        readOnly={property.readOnly}
        onFocus={() => setFocusedField?.(name)}
        onBlur={() => field.onBlur()}
        yearMonth={property.pattern === "^[0-9]{4}-[0-9]{2}$"}
      />
    );
  }

  if (property.enum) {
    return (
      <ListBox
        options={property.enum.map((dataItem) => ({
          label: dataItem?.toString() ?? "",
          value: dataItem?.toString() ?? "",
        }))}
        onSelect={(selectedItem) => selectedItem && field.onChange(selectedItem)}
        selectedValue={fieldValue}
        isDisabled={disabled || property.readOnly}
        onFocus={() => setFocusedField?.(name)}
        hasError={error}
        data-testid={field.name}
      />
    );
  } else if (property.multiline && !property.isSecret) {
    return (
      <TextArea
        {...field}
        onChange={onChange}
        autoComplete="off"
        value={fieldValue ?? ""}
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
      value={fieldValue ?? ""}
      disabled={disabled}
      readOnly={property.readOnly}
      onFocus={() => setFocusedField?.(name)}
      error={error}
    />
  );
};
