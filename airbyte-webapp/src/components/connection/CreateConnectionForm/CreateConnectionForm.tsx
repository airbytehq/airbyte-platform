import { Form, Formik, FormikHelpers } from "formik";
import React, { Suspense, useCallback, useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";

import { ConnectionFormFields } from "components/connection/ConnectionForm/ConnectionFormFields";
import { CreateControls } from "components/connection/ConnectionForm/CreateControls";
import {
  FormikConnectionFormValues,
  useConnectionValidationSchema,
} from "components/connection/ConnectionForm/formConfig";
import { OperationsSection } from "components/connection/ConnectionForm/OperationsSection";
import LoadingSchema from "components/LoadingSchema";

import { useGetDestinationFromSearchParams, useGetSourceFromSearchParams } from "area/connector/utils";
import { ConnectionValues, useCreateConnection } from "core/api";
import { FeatureItem, useFeature } from "core/services/features";
import {
  ConnectionFormServiceProvider,
  useConnectionFormService,
} from "hooks/services/ConnectionForm/ConnectionFormService";
import { useExperimentContext } from "hooks/services/Experiment";
import { SchemaError as SchemaErrorType, useDiscoverSchema } from "hooks/services/useSourceHook";

import styles from "./CreateConnectionForm.module.scss";
import { CreateConnectionNameField } from "./CreateConnectionNameField";
import { DataResidency } from "./DataResidency";
import { SchemaError } from "./SchemaError";
import { useAnalyticsTrackFunctions } from "./useAnalyticsTrackFunctions";

interface CreateConnectionPropsInner {
  schemaError: SchemaErrorType;
}

/**
 * @deprecated the file will be removed in 3rd PR of the cleanup
 */
const CreateConnectionFormInner: React.FC<CreateConnectionPropsInner> = ({ schemaError }) => {
  const navigate = useNavigate();
  const canEditDataGeographies = useFeature(FeatureItem.AllowChangeDataGeographies);
  const { mutateAsync: createConnection } = useCreateConnection();

  const { connection, initialValues, mode, setSubmitError } = useConnectionFormService();
  const [editingTransformation, setEditingTransformation] = useState(false);
  const validationSchema = useConnectionValidationSchema({ mode });
  useExperimentContext("source-definition", connection.source?.sourceDefinitionId);

  const onFormSubmit = useCallback(
    async (formValues: FormikConnectionFormValues, formikHelpers: FormikHelpers<FormikConnectionFormValues>) => {
      try {
        const createdConnection = await createConnection({
          // just a stub to fix the type errors
          values: formValues as ConnectionValues,
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

        navigate(`../../connections/${createdConnection.connectionId}`);
      } catch (e) {
        setSubmitError(e);
      }
    },
    [createConnection, connection.source, connection.destination, connection.catalogId, navigate, setSubmitError]
  );

  if (schemaError) {
    return <SchemaError schemaError={schemaError} />;
  }

  return (
    <Suspense fallback={<LoadingSchema />}>
      <div className={styles.connectionFormContainer}>
        <Formik
          // just a stub to fix the type errors
          initialValues={initialValues as FormikConnectionFormValues}
          validationSchema={validationSchema}
          onSubmit={onFormSubmit}
        >
          {({ isSubmitting, isValid, dirty, validateForm }) => (
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
                // errorMessage={getErrorMessage(isValid, errors)}
              />
            </Form>
          )}
        </Formik>
      </div>
    </Suspense>
  );
};

/**
 * @deprecated the file will be removed in 3rd PR of the cleanup
 */
export const CreateConnectionForm: React.FC = () => {
  const source = useGetSourceFromSearchParams();
  const destination = useGetDestinationFromSearchParams();
  const { trackFailure } = useAnalyticsTrackFunctions();

  const { schema, isLoading, schemaErrorStatus, catalogId, onDiscoverSchema } = useDiscoverSchema(
    source.sourceId,
    true
  );

  useEffect(() => {
    if (schemaErrorStatus) {
      trackFailure(source, destination, schemaErrorStatus);
    }
    // we need to track the schemaErrorStatus changes only
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [schemaErrorStatus]);

  if (!schema) {
    return <LoadingSchema />;
  }

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
