import React, { useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import * as yup from "yup";
import { SchemaOf } from "yup";

import { getInitialNormalization } from "components/connection/ConnectionForm/formConfig";
import { NormalizationFormField } from "components/connection/ConnectionForm/NormalizationFormField";
import { Form } from "components/forms";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";
import { CollapsibleCard } from "components/ui/CollapsibleCard";

import { NormalizationType } from "area/connection/types";
import { isNormalizationTransformation } from "area/connection/utils";
import { useCurrentWorkspace } from "core/api";
import { OperatorType } from "core/api/types/AirbyteClient";
import { useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useNotificationService } from "hooks/services/Notification";

interface NormalizationFormValues {
  normalization: NormalizationType;
}

const normalizationFormSchema: SchemaOf<NormalizationFormValues> = yup.object().shape({
  normalization: yup.mixed().oneOf([NormalizationType.raw, NormalizationType.basic]),
});

export const NormalizationForm: React.FC = () => {
  const { formatMessage } = useIntl();

  const { workspaceId } = useCurrentWorkspace();
  const { mode } = useConnectionFormService();
  const { registerNotification } = useNotificationService();
  const { trackError } = useAppMonitoringService();
  const {
    connection: { operations, connectionId },
    updateConnection,
  } = useConnectionEditService();

  const initialValues = useMemo(
    () => ({
      normalization: getInitialNormalization(operations, true),
    }),
    [operations]
  );

  const onSubmit = async ({ normalization }: NormalizationFormValues) => {
    const operationsWithoutNormalization = (operations ?? [])?.filter((op) => !isNormalizationTransformation(op));

    await updateConnection({
      connectionId,
      operations: [
        // if normalization is "basic", add normalization operation
        ...(normalization === NormalizationType.basic
          ? [
              {
                name: "Normalization",
                workspaceId,
                operatorConfiguration: {
                  operatorType: OperatorType.normalization,
                  normalization: {
                    option: normalization,
                  },
                },
              },
            ]
          : // if normalization is "raw", remove normalization operation
            []),
        ...operationsWithoutNormalization,
      ],
    });
  };

  const onSuccess = () => {
    registerNotification({
      id: "normalization_settings_change_success",
      text: formatMessage({ id: "connection.normalization.successMessage" }),
      type: "success",
    });
  };

  const onError = (e: Error, { normalization }: NormalizationFormValues) => {
    trackError(e, { normalization });
    registerNotification({
      id: "normalization_settings_change_error",
      text: formatMessage({ id: "connection.normalization.errorMessage" }),
      type: "error",
    });
  };

  return (
    <Form<NormalizationFormValues>
      defaultValues={initialValues}
      schema={normalizationFormSchema}
      onSubmit={onSubmit}
      onSuccess={onSuccess}
      onError={onError}
      disabled={mode === "readonly"}
      trackDirtyChanges
      dataTestId="normalization-form"
    >
      <CollapsibleCard title={<FormattedMessage id="connection.normalization" />} collapsible>
        <NormalizationFormField />
        <FormSubmissionButtons submitKey="form.saveChanges" />
      </CollapsibleCard>
    </Form>
  );
};
