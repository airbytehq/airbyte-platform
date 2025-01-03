import React, { useCallback } from "react";
import { AnyObjectSchema } from "yup";

import { Form } from "components/forms";

import {
  ConnectorDefinition,
  ConnectorDefinitionSpecificationRead,
  SourceDefinitionSpecificationDraft,
} from "core/domain/connector";
import { removeEmptyProperties } from "core/utils/form";
import { useFormChangeTrackerService, useUniqueFormId } from "hooks/services/FormChangeTracker";

import { ConnectorFormContextProvider } from "./connectorFormContext";
import { FormRootProps, FormRoot } from "./FormRoot";
import { ConnectorFormValues } from "./types";
import { useBuildForm } from "./useBuildForm";

export interface ConnectorFormProps extends Omit<FormRootProps, "formFields" | "castValues" | "groupStructure"> {
  formType: "source" | "destination";
  formId?: string;
  /**
   * Definition of the connector might not be available if it's not released but only exists in frontend heap
   */
  selectedConnectorDefinition?: ConnectorDefinition;
  selectedConnectorDefinitionSpecification?: ConnectorDefinitionSpecificationRead | SourceDefinitionSpecificationDraft;
  onSubmit: (values: ConnectorFormValues) => Promise<void>;
  isEditMode?: boolean;
  formValues?: Partial<ConnectorFormValues>;
  connectorId?: string;
  trackDirtyChanges?: boolean;
  canEdit: boolean;
}

export const ConnectorForm: React.FC<ConnectorFormProps> = (props) => {
  const formId = useUniqueFormId(props.formId);
  const { clearFormChange } = useFormChangeTrackerService();

  const {
    formType,
    formValues,
    onSubmit,
    isEditMode,
    selectedConnectorDefinition,
    selectedConnectorDefinitionSpecification,
    connectorId,
    canEdit,
  } = props;

  const { formFields, initialValues, validationSchema, groups } = useBuildForm(
    Boolean(isEditMode),
    formType,
    selectedConnectorDefinitionSpecification,
    formValues
  );

  const castValues = useCallback(
    (values: ConnectorFormValues) =>
      validationSchema.cast(removeEmptyProperties(values), {
        stripUnknown: true,
      }),
    [validationSchema]
  );

  const onFormSubmit = useCallback(
    async (values: ConnectorFormValues) => {
      const valuesToSend = castValues(values);
      await onSubmit(valuesToSend);
      clearFormChange(formId);
      // do not reset form values to avoid casting oddities
      return {
        resetValues: valuesToSend,
        keepStateOptions: {
          keepValues: true,
        },
      };
    },
    [castValues, onSubmit, clearFormChange, formId]
  );

  return (
    <Form
      trackDirtyChanges={props.trackDirtyChanges}
      defaultValues={initialValues as ConnectorFormValues<object>}
      schema={validationSchema as AnyObjectSchema}
      onSubmit={onFormSubmit}
      disabled={!canEdit}
    >
      <ConnectorFormContextProvider
        formType={formType}
        getValues={castValues}
        selectedConnectorDefinition={selectedConnectorDefinition}
        selectedConnectorDefinitionSpecification={selectedConnectorDefinitionSpecification}
        isEditMode={isEditMode}
        validationSchema={validationSchema}
        connectorId={connectorId}
      >
        <FormRoot {...props} formFields={formFields} castValues={castValues} groupStructure={groups} />
      </ConnectorFormContextProvider>
    </Form>
  );
};
