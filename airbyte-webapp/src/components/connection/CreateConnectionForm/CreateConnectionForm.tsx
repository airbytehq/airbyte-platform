import { Form, Formik, FormikHelpers } from "formik";
import React, { Suspense, useCallback, useState } from "react";
import { useNavigate } from "react-router-dom";

import { ConnectionFormFields } from "components/connection/ConnectionForm/ConnectionFormFields";
import { CreateControls } from "components/connection/ConnectionForm/CreateControls";
import {
  FormikConnectionFormValues,
  useConnectionValidationSchema,
} from "components/connection/ConnectionForm/formConfig";
import { OperationsSection } from "components/connection/ConnectionForm/OperationsSection";
import LoadingSchema from "components/LoadingSchema";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useCreateConnection } from "core/api";
import { FeatureItem, useFeature } from "core/services/features";
import { useGetDestinationFromSearchParams } from "hooks/domain/connector/useGetDestinationFromParams";
import { useGetSourceFromSearchParams } from "hooks/domain/connector/useGetSourceFromParams";
import {
  ConnectionFormServiceProvider,
  tidyConnectionFormValues,
  useConnectionFormService,
} from "hooks/services/ConnectionForm/ConnectionFormService";
import { useExperimentContext } from "hooks/services/Experiment";
import { useFormChangeTrackerService } from "hooks/services/FormChangeTracker";
import { SchemaError as SchemaErrorType, useDiscoverSchema } from "hooks/services/useSourceHook";

import styles from "./CreateConnectionForm.module.scss";
import { CreateConnectionNameField } from "./CreateConnectionNameField";
import { DataResidency } from "./DataResidency";
import { SchemaError } from "./SchemaError";

interface CreateConnectionPropsInner {
  schemaError: SchemaErrorType;
}

const CreateConnectionFormInner: React.FC<CreateConnectionPropsInner> = ({ schemaError }) => {
  const navigate = useNavigate();
  const canEditDataGeographies = useFeature(FeatureItem.AllowChangeDataGeographies);
  const { mutateAsync: createConnection } = useCreateConnection();
  const { clearFormChange } = useFormChangeTrackerService();

  const workspaceId = useCurrentWorkspaceId();

  const { connection, initialValues, mode, formId, getErrorMessage, setSubmitError } = useConnectionFormService();
  const [editingTransformation, setEditingTransformation] = useState(false);
  const validationSchema = useConnectionValidationSchema({ mode });
  useExperimentContext("source-definition", connection.source?.sourceDefinitionId);

  const onFormSubmit = useCallback(
    async (formValues: FormikConnectionFormValues, formikHelpers: FormikHelpers<FormikConnectionFormValues>) => {
      const values = tidyConnectionFormValues(formValues, workspaceId, validationSchema);

      try {
        const createdConnection = await createConnection({
          values,
          source: connection.source,
          destination: connection.destination,
          sourceDefinition: {
            sourceDefinitionId: connection.source?.sourceDefinitionId ?? "",
          },
          destinationDefinition: {
            name: connection.destination?.name ?? "",
            destinationDefinitionId: connection.destination?.destinationDefinitionId ?? "",
          },
          sourceCatalogId: connection.catalogId,
        });

        formikHelpers.resetForm();
        // We need to clear the form changes otherwise the dirty form intercept service will prevent navigation
        clearFormChange(formId);

        navigate(`../../connections/${createdConnection.connectionId}`);
      } catch (e) {
        setSubmitError(e);
      }
    },
    [
      workspaceId,
      validationSchema,
      createConnection,
      connection.source,
      connection.destination,
      connection.catalogId,
      clearFormChange,
      formId,
      navigate,
      setSubmitError,
    ]
  );

  if (schemaError) {
    return <SchemaError schemaError={schemaError} />;
  }

  return (
    <Suspense fallback={<LoadingSchema />}>
      <div className={styles.connectionFormContainer}>
        <Formik initialValues={initialValues} validationSchema={validationSchema} onSubmit={onFormSubmit}>
          {({ isSubmitting, isValid, dirty, errors, validateForm }) => (
            <Form>
              <CreateConnectionNameField />
              {canEditDataGeographies && <DataResidency />}
              <ConnectionFormFields isSubmitting={isSubmitting} dirty={dirty} validateForm={validateForm} />
              <OperationsSection
                onStartEditTransformation={() => setEditingTransformation(true)}
                onEndEditTransformation={() => setEditingTransformation(false)}
              />
              <CreateControls
                isSubmitting={isSubmitting}
                isValid={isValid && !editingTransformation}
                errorMessage={getErrorMessage(isValid, errors)}
              />
            </Form>
          )}
        </Formik>
      </div>
    </Suspense>
  );
};

export const CreateConnectionForm: React.FC = () => {
  const source = useGetSourceFromSearchParams();
  const destination = useGetDestinationFromSearchParams();

  const { schema, isLoading, schemaErrorStatus, catalogId, onDiscoverSchema } = useDiscoverSchema(
    source.sourceId,
    true
  );

  const partialConnection = {
    syncCatalog: schema,
    destination,
    source,
    catalogId,
  };

  return (
    <ConnectionFormServiceProvider
      connection={partialConnection}
      mode="create"
      refreshSchema={onDiscoverSchema}
      schemaError={schemaErrorStatus}
    >
      {isLoading ? <LoadingSchema /> : <CreateConnectionFormInner schemaError={schemaErrorStatus} />}
    </ConnectionFormServiceProvider>
  );
};
