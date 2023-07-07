import React, { ReactNode } from "react";
import { useFormContext } from "react-hook-form";

import { FlexContainer } from "components/ui/Flex";

import { FormBlock, GroupDetails } from "core/form/types";

import { FormSection } from "./components/Sections/FormSection";
import { useConnectorForm } from "./connectorFormContext";
import { ConnectorFormValues } from "./types";

export interface FormRootProps {
  formFields: FormBlock[];
  groupStructure?: GroupDetails[];
  connectionTestSuccess?: boolean;
  isTestConnectionInProgress?: boolean;
  bodyClassName?: string;
  headerBlock?: ReactNode;
  title?: React.ReactNode;
  description?: React.ReactNode;
  full?: boolean;
  castValues: (values: ConnectorFormValues) => ConnectorFormValues;
  renderFooter?: (formProps: {
    dirty: boolean;
    isSubmitting: boolean;
    isValid: boolean;
    resetConnectorForm: () => void;
    isEditMode?: boolean;
    formType: "source" | "destination";
    getValues: () => ConnectorFormValues;
  }) => ReactNode;
}

export const FormRoot: React.FC<FormRootProps> = ({
  isTestConnectionInProgress = false,
  formFields,
  groupStructure,
  bodyClassName,
  headerBlock,
  renderFooter,
  castValues,
  ...props
}) => {
  const form = useFormContext<ConnectorFormValues>();
  const isSubmitting = form.formState.isSubmitting;
  const dirty = form.formState.isDirty;
  const isValid = form.formState.isValid;
  const { resetConnectorForm, isEditMode, formType } = useConnectorForm();

  const formBody = (
    <FormSection
      headerBlock={
        headerBlock || props.title || props.description
          ? {
              elements: headerBlock,
              title: props.title,
              description: props.description,
            }
          : undefined
      }
      rootLevel
      blocks={formFields}
      groupStructure={groupStructure}
      disabled={isSubmitting || isTestConnectionInProgress}
    />
  );
  return (
    <FlexContainer direction="column" gap="xl">
      <div className={bodyClassName}>{formBody}</div>
      {renderFooter &&
        renderFooter({
          dirty,
          isSubmitting,
          isValid,
          resetConnectorForm,
          isEditMode,
          formType,
          getValues: () => castValues(form.getValues()),
        })}
    </FlexContainer>
  );
};
