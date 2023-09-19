import React, { Suspense, useCallback } from "react";

import { Form } from "components/forms";
import LoadingSchema from "components/LoadingSchema";
import { FlexContainer } from "components/ui/Flex";

import { FeatureItem, useFeature } from "core/services/features";
import { useGetDestinationFromSearchParams } from "hooks/domain/connector/useGetDestinationFromParams";
import { useGetSourceFromSearchParams } from "hooks/domain/connector/useGetSourceFromParams";
import {
  ConnectionHookFormServiceProvider,
  useConnectionHookFormService,
} from "hooks/services/ConnectionForm/ConnectionHookFormService";
import { useExperimentContext } from "hooks/services/Experiment";
import { SchemaError as SchemaErrorType, useDiscoverSchema } from "hooks/services/useSourceHook";

import { ConnectionNameHookFormCard } from "./ConnectionNameHookFormCard";
import styles from "./CreateConnectionHookForm.module.scss";
import { ConnectionConfigurationHookFormCard } from "../ConnectionForm/ConnectionConfigurationHookFormCard";
import { HookFormConnectionFormValues, useConnectionHookFormValidationSchema } from "../ConnectionForm/hookFormConfig";
import { OperationsSectionHookForm } from "../ConnectionForm/OperationsSectionHookForm";
import { DataResidencyHookFormCard } from "../CreateConnectionForm/DataResidencyHookFormCard";
import { SchemaError } from "../CreateConnectionForm/SchemaError";

interface CreateConnectionPropsInner {
  schemaError: SchemaErrorType;
}

const CreateConnectionFormInner: React.FC<CreateConnectionPropsInner> = ({ schemaError }) => {
  // const navigate = useNavigate();
  const canEditDataGeographies = useFeature(FeatureItem.AllowChangeDataGeographies);
  // const { mutateAsync: createConnection } = useCreateConnection();
  // const { clearFormChange } = useFormChangeTrackerService();

  // const workspaceId = useCurrentWorkspaceId();

  const {
    connection,
    initialValues,
    // mode,
    // formId
    // , getErrorMessage,
    // setSubmitError,
  } = useConnectionHookFormService();

  const validationSchema = useConnectionHookFormValidationSchema();
  useExperimentContext("source-definition", connection.source?.sourceDefinitionId);

  const onSubmit = useCallback(async (formValues: HookFormConnectionFormValues) => {
    /**
     *there is some magic , need to try get rid of tidyConnectionHookFormValues, or at least split it
     */
    // const values = tidyConnectionHookFormValues(formValues, workspaceId, validationSchema);

    console.log(formValues);
    // try {
    //   const createdConnection = await createConnection({
    //     formValues,
    //     source: connection.source,
    //     destination: connection.destination,
    //     sourceDefinition: {
    //       sourceDefinitionId: connection.source?.sourceDefinitionId ?? "",
    //     },
    //     destinationDefinition: {
    //       name: connection.destination?.name ?? "",
    //       destinationDefinitionId: connection.destination?.destinationDefinitionId ?? "",
    //     },
    //     sourceCatalogId: connection.catalogId,
    //   });

    // formikHelpers.resetForm();
    // We need to clear the form changes otherwise the dirty form intercept service will prevent navigation
    // clearFormChange(formId);

    /**
     * can't move to onSuccess since we get connectionId in the createdConnection response
     */
    //   navigate(`../../connections/${createdConnection.connectionId}`);
    // } catch (e) {
    //   // setSubmitError(e);
    //   console.log(e);
    // }
  }, []);

  if (schemaError) {
    return <SchemaError schemaError={schemaError} />;
  }

  return (
    <Suspense fallback={<LoadingSchema />}>
      <Form<HookFormConnectionFormValues> defaultValues={initialValues} schema={validationSchema} onSubmit={onSubmit}>
        <FlexContainer direction="column" className={styles.formContainer}>
          <ConnectionNameHookFormCard />
          {canEditDataGeographies && <DataResidencyHookFormCard />}
          <ConnectionConfigurationHookFormCard />
          {/* SyncCatalog will be here */}
          <OperationsSectionHookForm />
          {/* <Submit button */}
        </FlexContainer>
      </Form>
    </Suspense>
  );
};

/**
 * react-hook-form version of the CreateConnectionForm
 */
export const CreateConnectionHookForm: React.FC = () => {
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
    <ConnectionHookFormServiceProvider
      connection={partialConnection}
      mode="create"
      refreshSchema={onDiscoverSchema}
      schemaError={schemaErrorStatus} // never consumed from useConnectionFormService() hook
    >
      {isLoading ? <LoadingSchema /> : <CreateConnectionFormInner schemaError={schemaErrorStatus} />}
    </ConnectionHookFormServiceProvider>
  );
};
