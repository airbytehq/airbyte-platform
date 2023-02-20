import { FieldProps, useField } from "formik";
import React from "react";
import { FormattedMessage } from "react-intl";

import { ControlLabels } from "components/LabeledControl";
import { DropDown } from "components/ui/DropDown";

import { NamespaceDefinitionType } from "core/request/AirbyteClient";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";

import { FormFieldWrapper } from "./FormFieldWrapper";

export const StreamOptions = [
  {
    value: NamespaceDefinitionType.destination,
    label: <FormattedMessage id="connectionForm.destinationFormat" />,
    testId: "namespaceDefinition-destination",
  },
  {
    value: NamespaceDefinitionType.source,
    label: <FormattedMessage id="connectionForm.sourceFormat" />,
    testId: "namespaceDefinition-source",
  },
  {
    value: NamespaceDefinitionType.customformat,
    label: <FormattedMessage id="connectionForm.customFormat" />,
    testId: "namespaceDefinition-customformat",
  },
];

export const NamespaceDefinitionField: React.FC<FieldProps<string>> = ({ field, form }) => {
  const [, meta] = useField(field.name);
  const { mode } = useConnectionFormService();

  return (
    <FormFieldWrapper>
      <ControlLabels
        nextLine
        error={!!meta.error && meta.touched}
        label={<FormattedMessage id="connectionForm.namespaceDefinition.title" />}
        infoTooltipContent={<FormattedMessage id="connectionForm.namespaceDefinition.subtitle" />}
      />
      <DropDown
        name="namespaceDefinition"
        error={!!meta.error && meta.touched}
        options={StreamOptions}
        value={field.value}
        isDisabled={form.isSubmitting || mode === "readonly"}
        onChange={({ value }) => form.setFieldValue(field.name, value)}
      />
    </FormFieldWrapper>
  );
};
