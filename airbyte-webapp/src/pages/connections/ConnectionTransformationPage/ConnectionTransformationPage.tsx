import React from "react";
import { FormattedMessage } from "react-intl";

import {
  getInitialNormalization,
  getInitialTransformationsOld,
  mapFormPropsToOperation,
} from "components/connection/ConnectionForm/formConfig";
import { PageContainer } from "components/PageContainer";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { useCurrentWorkspace } from "core/api";
import { OperationCreate, OperationRead, OperatorType } from "core/api/types/AirbyteClient";
import { NormalizationType } from "core/domain/connection";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { FeatureItem, useFeature } from "core/services/features";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useExperiment } from "hooks/services/Experiment";
import { FormikOnSubmit } from "types/formik";

import styles from "./ConnectionTransformationPage.module.scss";
import { CustomTransformationsCard } from "./CustomTransformationsCard";
import { CustomTransformationsHookForm } from "./CustomTransformationsHookForm";
import { DbtCloudTransformationsCard } from "./DbtCloudTransformationsCard";
import { NormalizationCard } from "./NormalizationCard";
import { NormalizationHookForm } from "./NormalizationHookForm";

export const ConnectionTransformationPage: React.FC = () => {
  const { connection, updateConnection } = useConnectionEditService();
  const { mode, destDefinition } = useConnectionFormService();

  useTrackPage(PageTrackingCodes.CONNECTIONS_ITEM_TRANSFORMATION);
  const supportsNormalization = destDefinition.normalizationConfig.supported;
  const supportsDbt = useFeature(FeatureItem.AllowCustomDBT) && destDefinition.supportsDbt;
  const supportsCloudDbtIntegration = useFeature(FeatureItem.AllowDBTCloudIntegration) && destDefinition.supportsDbt;
  const noSupportedTransformations = !supportsNormalization && !supportsDbt && !supportsCloudDbtIntegration;

  // TODO: remove 40-66 lines after NormalizationHookFormCard and CustomTransformationsHookFormCard form migration
  const doUseReactHookForm = useExperiment("form.reactHookForm", false);
  const workspace = useCurrentWorkspace();
  const onSubmit: FormikOnSubmit<{ transformations?: OperationRead[]; normalization?: NormalizationType }> = async (
    values,
    { resetForm }
  ) => {
    const newOp = mapFormPropsToOperation(values, connection.operations, workspace.workspaceId);

    const operations = values.transformations
      ? (connection.operations as OperationCreate[]) // There's an issue meshing the OperationRead here with OperationCreate that we want, in the types
          ?.filter((op) => op.operatorConfiguration.operatorType === OperatorType.normalization)
          .concat(newOp)
      : newOp.concat(
          (connection.operations ?? [])?.filter((op) => op.operatorConfiguration.operatorType === OperatorType.dbt)
        );

    await updateConnection({ connectionId: connection.connectionId, operations });

    const nextFormValues: typeof values = {};
    if (values.transformations) {
      nextFormValues.transformations = getInitialTransformationsOld(operations);
    }
    nextFormValues.normalization = getInitialNormalization(operations, true);

    resetForm({ values: nextFormValues });
  };

  return (
    <PageContainer centered>
      <FlexContainer direction="column" gap="lg">
        {doUseReactHookForm ? (
          <>
            {supportsNormalization && <NormalizationHookForm />}
            {supportsDbt && <CustomTransformationsHookForm />}
          </>
        ) : (
          <>
            <fieldset
              disabled={mode === "readonly"}
              style={{
                all: "unset",
                pointerEvents: `${mode === "readonly" ? "none" : "auto"}`,
              }}
            >
              {supportsNormalization && <NormalizationCard operations={connection.operations} onSubmit={onSubmit} />}
            </fieldset>
            <fieldset
              disabled={mode === "readonly"}
              style={{
                all: "unset",
                pointerEvents: `${mode === "readonly" ? "none" : "auto"}`,
              }}
            >
              {supportsDbt && <CustomTransformationsCard operations={connection.operations} onSubmit={onSubmit} />}
            </fieldset>
          </>
        )}

        {/* TODO: remove fieldset after DbtCloudTransformationsCard form migration*/}
        <fieldset
          disabled={mode === "readonly"}
          style={{ all: "unset", pointerEvents: `${mode === "readonly" ? "none" : "auto"}` }}
        >
          {supportsCloudDbtIntegration && <DbtCloudTransformationsCard connection={connection} />}
        </fieldset>

        {noSupportedTransformations && (
          <Card className={styles.customCard}>
            <Text size="lg" align="center">
              <FormattedMessage id="connectionForm.operations.notSupported" />
            </Text>
          </Card>
        )}
      </FlexContainer>
    </PageContainer>
  );
};
