import { Form, useFormikContext } from "formik";
import React, { ReactNode } from "react";

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
  const { dirty, isSubmitting, isValid, values } = useFormikContext<ConnectorFormValues>();
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
    <Form>
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
            getValues: () => castValues(values),
          })}
      </FlexContainer>
    </Form>
  );
};
