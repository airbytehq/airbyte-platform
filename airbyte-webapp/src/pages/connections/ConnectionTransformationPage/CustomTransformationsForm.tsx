import React, { useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import * as yup from "yup";
import { SchemaOf } from "yup";

import { CustomTransformationsFormField } from "components/connection/ConnectionForm/CustomTransformationsFormField";
import { getInitialTransformations } from "components/connection/ConnectionForm/formConfig";
import { DbtOperationReadOrCreate, dbtOperationReadOrCreateSchema } from "components/connection/TransformationForm";
import { Form } from "components/forms";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";
import { CollapsibleCard } from "components/ui/CollapsibleCard";

import { isDbtTransformation } from "area/connection/utils";
import { useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useNotificationService } from "hooks/services/Notification";

export interface CustomTransformationsFormValues {
  transformations: DbtOperationReadOrCreate[];
}

const customTransformationsValidationSchema: SchemaOf<CustomTransformationsFormValues> = yup.object({
  transformations: yup.array().of(dbtOperationReadOrCreateSchema),
});

export const CustomTransformationsForm: React.FC = () => {
  const { formatMessage } = useIntl();
  const { trackError } = useAppMonitoringService();
  const { registerNotification } = useNotificationService();
  const { mode } = useConnectionFormService();
  const {
    connection: { operations = [], connectionId },
    updateConnection,
  } = useConnectionEditService();

  const initialValues = useMemo(
    () => ({
      transformations: getInitialTransformations(operations),
    }),
    [operations]
  );

  const onSubmit = async ({ transformations }: CustomTransformationsFormValues) => {
    await updateConnection({
      connectionId,
      operations: [...operations.filter((op) => !isDbtTransformation(op)), ...transformations],
    });
  };

  const onSuccess = () => {
    registerNotification({
      id: "customTransformations_settings_change_success",
      text: formatMessage({ id: "connection.customTransformations.successMessage" }),
      type: "success",
    });
  };

  const onError = (e: Error, { transformations }: CustomTransformationsFormValues) => {
    trackError(e, { transformations });
    registerNotification({
      id: "customTransformations_settings_change_error",
      text: formatMessage({ id: "connection.customTransformations.errorMessage" }),
      type: "error",
    });
  };

  return (
    <Form<CustomTransformationsFormValues>
      defaultValues={initialValues}
      schema={customTransformationsValidationSchema}
      onSubmit={onSubmit}
      onSuccess={onSuccess}
      onError={onError}
      disabled={mode === "readonly"}
      trackDirtyChanges
      dataTestId="custom-transformation-form"
    >
      <CollapsibleCard title={<FormattedMessage id="connection.customTransformations" />} collapsible>
        <CustomTransformationsFormField />
        <FormSubmissionButtons submitKey="form.saveChanges" />
      </CollapsibleCard>
    </Form>
  );
};
