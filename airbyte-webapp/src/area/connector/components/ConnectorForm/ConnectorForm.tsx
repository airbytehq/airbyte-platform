import cloneDeep from "lodash/cloneDeep";
import React, { useCallback, useId } from "react";
import { UseFormReturn } from "react-hook-form";
import { AnyObjectSchema } from "yup";

import { Form } from "components/ui/forms";

import {
  ConnectorDefinition,
  ConnectorDefinitionSpecificationRead,
  SourceDefinitionSpecificationDraft,
} from "core/domain/connector";
import { useExperiment } from "core/services/Experiment";
import { FeatureItem, useFeature } from "core/services/features";
import { useFormChangeTrackerService, useUniqueFormId } from "core/services/FormChangeTracker";
import { removeEmptyProperties } from "core/utils/form";

import { ConnectorFormContextProvider } from "./connectorFormContext";
import { FormRootProps, FormRoot } from "./FormRoot";
import { ConnectorFormValues } from "./types";
import { useBuildForm } from "./useBuildForm";

export interface ConnectorFormProps
  extends Omit<FormRootProps, "formFields" | "castValues" | "groupStructure" | "formElementId" | "sections"> {
  formType: "source" | "destination";
  formId?: string;
  /**
   * Definition of the connector might not be available if it's not released but only exists in frontend heap
   */
  selectedConnectorDefinition: ConnectorDefinition;
  selectedConnectorDefinitionSpecification?: ConnectorDefinitionSpecificationRead | SourceDefinitionSpecificationDraft;
  onSubmit: (values: ConnectorFormValues) => Promise<void>;
  isEditMode?: boolean;
  formValues?: Partial<ConnectorFormValues>;
  connectorId?: string;
  trackDirtyChanges?: boolean;
  /**
   * Components that read or fill the form from code, such as the connector setup agent's tools.
   * They render inside the form's providers and take no place in its layout.
   */
  formAutomation?: React.ReactNode;
}

export const ConnectorForm: React.FC<ConnectorFormProps> = (props) => {
  const formId = useUniqueFormId(props.formId);
  const { clearFormChange } = useFormChangeTrackerService();
  const isResourceAllocationEnabled = useFeature(FeatureItem.ConnectorResourceAllocation);
  const isSectionedSetupFormEnabled = useExperiment("connector.sectionedSetupForm");
  const formElementId = useId();

  const {
    formType,
    formValues,
    onSubmit,
    isEditMode,
    selectedConnectorDefinition,
    selectedConnectorDefinitionSpecification,
    connectorId,
    canEdit,
    formAutomation,
  } = props;

  const { formFields, initialValues, validationSchema, groups, sections } = useBuildForm(
    Boolean(isEditMode),
    formType,
    selectedConnectorDefinitionSpecification,
    isResourceAllocationEnabled,
    isSectionedSetupFormEnabled,
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
    async (values: ConnectorFormValues, methods: UseFormReturn<ConnectorFormValues>) => {
      const valuesToSend = castValues(values);
      // Take the new default values before the submission runs, so an edit made during it keeps the form dirty. They
      // are the form values, not the cast ones, because react-hook-form compares the two again after the reset. The
      // deep copy matters: react-hook-form writes nested field changes into the objects that getValues() returns.
      const submittedFormValues = cloneDeep(methods.getValues());
      await onSubmit(valuesToSend);
      clearFormChange(formId);
      return {
        resetValues: submittedFormValues,
        keepStateOptions: {
          keepValues: true,
        },
      };
    },
    [castValues, onSubmit, clearFormChange, formId]
  );

  return (
    <Form
      id={formElementId}
      trackDirtyChanges={props.trackDirtyChanges}
      defaultValues={initialValues}
      schema={validationSchema as AnyObjectSchema}
      onSubmit={onFormSubmit}
      disabled={!canEdit}
    >
      <ConnectorFormContextProvider
        formType={formType}
        castValues={castValues}
        selectedConnectorDefinition={selectedConnectorDefinition}
        selectedConnectorDefinitionSpecification={selectedConnectorDefinitionSpecification}
        isEditMode={isEditMode}
        validationSchema={validationSchema}
        connectorId={connectorId}
      >
        <FormRoot
          {...props}
          formElementId={formElementId}
          formFields={formFields}
          castValues={castValues}
          groupStructure={groups}
          sections={sections}
        />
        {formAutomation}
      </ConnectorFormContextProvider>
    </Form>
  );
};
