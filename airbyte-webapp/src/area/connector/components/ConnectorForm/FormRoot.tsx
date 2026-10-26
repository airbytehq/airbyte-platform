import uniq from "lodash/uniq";
import React, { ReactNode, useMemo } from "react";
import { useFormContext, useWatch } from "react-hook-form";
import { useIntl } from "react-intl";
import { AnySchema, ValidationError } from "yup";

import { FlexContainer } from "components/ui/Flex";

import { FormBlock, GroupDetails } from "core/form/types";

import { FormSection } from "./components/Sections/FormSection";
import { useConnectorForm } from "./connectorFormContext";
import { ConnectorFormValues } from "./types";
import { useAuthentication } from "./useAuthentication";

interface RenderFooterArgs {
  dirty: boolean;
  isSubmitting: boolean;
  isValid: boolean;
  /**
   * Labels of the required fields that are empty. Only `required` errors count: pattern, range and type errors are
   * ignored. Empty fields that OAuth fills show as one entry that asks the user to authenticate.
   */
  emptyRequiredFields: string[];
  resetConnectorForm: () => void;
  isEditMode?: boolean;
  formType: "source" | "destination";
  getValues: () => ConnectorFormValues;
}

export interface FormRootProps {
  formFields: FormBlock[];
  groupStructure?: GroupDetails[];
  connectionTestSuccess?: boolean;
  isTestConnectionInProgress?: boolean;
  bodyClassName?: string;
  headerBlock?: ReactNode;
  title?: string;
  description?: React.ReactNode;
  full?: boolean;
  castValues: (values: ConnectorFormValues) => ConnectorFormValues;
  formHeader?: ReactNode;
  renderFooter?: (formProps: RenderFooterArgs) => ReactNode;
}

interface EmptyRequiredField {
  path: string;
  label: string;
}

/** The label comes from the field's schema, which `buildYupFormForJsonSchema` sets to the title the user sees. */
const getEmptyRequiredFields = (validationSchema: AnySchema, values: unknown): EmptyRequiredField[] => {
  try {
    validationSchema.validateSync(values, { abortEarly: false });
    return [];
  } catch (error) {
    if (!ValidationError.isError(error)) {
      throw error;
    }
    const errors = error.inner.length > 0 ? error.inner : [error];
    return errors.flatMap(({ type, path, params }) =>
      type === "required" && path ? [{ path, label: typeof params?.label === "string" ? params.label : path }] : []
    );
  }
};

// A separate component, so that watching every value re-renders only the footer and not the form body.
const FormFooter: React.FC<{
  renderFooter: NonNullable<FormRootProps["renderFooter"]>;
  footerProps: Omit<RenderFooterArgs, "emptyRequiredFields">;
}> = ({ renderFooter, footerProps }) => {
  const values = useWatch<ConnectorFormValues>();
  const { validationSchema } = useConnectorForm();
  const { isHiddenAuthField } = useAuthentication();
  const { formatMessage } = useIntl();

  const emptyRequiredFields = useMemo(() => {
    const emptyFields = getEmptyRequiredFields(validationSchema, values);
    const authenticationEntry = emptyFields.some(({ path }) => isHiddenAuthField(path))
      ? [formatMessage({ id: "connectorForm.fillRequiredFields.authenticate" })]
      : [];
    const fieldLabels = emptyFields.filter(({ path }) => !isHiddenAuthField(path)).map(({ label }) => label);
    return uniq([...authenticationEntry, ...fieldLabels]);
  }, [formatMessage, isHiddenAuthField, validationSchema, values]);

  return <>{renderFooter({ ...footerProps, emptyRequiredFields })}</>;
};

export const FormRoot: React.FC<FormRootProps> = ({
  isTestConnectionInProgress = false,
  formFields,
  groupStructure,
  bodyClassName,
  headerBlock,
  formHeader,
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
      {formHeader}
      <div className={bodyClassName}>{formBody}</div>
      {renderFooter && (
        <FormFooter
          renderFooter={renderFooter}
          footerProps={{
            dirty,
            isSubmitting,
            isValid,
            resetConnectorForm,
            isEditMode,
            formType,
            getValues: () => castValues(form.getValues()),
          }}
        />
      )}
    </FlexContainer>
  );
};
