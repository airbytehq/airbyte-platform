import { Field, FieldArray } from "formik";
import React from "react";
import { useIntl } from "react-intl";

import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { FeatureItem, useFeature } from "core/services/features";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";

import { NormalizationField } from "./NormalizationField";
import { TransformationField } from "./TransformationField";

interface OperationsSectionProps {
  onStartEditTransformation?: () => void;
  onEndEditTransformation?: () => void;
}

export const OperationsSection: React.FC<OperationsSectionProps> = ({
  onStartEditTransformation,
  onEndEditTransformation,
}) => {
  const { formatMessage } = useIntl();

  const {
    destDefinitionVersion: { normalizationConfig, supportsDbt },
  } = useConnectionFormService();

  const supportsNormalization = normalizationConfig.supported;
  const supportsTransformations = useFeature(FeatureItem.AllowCustomDBT) && supportsDbt;

  if (!supportsNormalization && !supportsTransformations) {
    return null;
  }

  return (
    <Card withPadding>
      <FlexContainer direction="column" gap="lg">
        {supportsNormalization || supportsTransformations ? (
          <Heading as="h2" size="sm">
            {[
              supportsNormalization && formatMessage({ id: "connectionForm.normalization.title" }),
              supportsTransformations && formatMessage({ id: "connectionForm.transformation.title" }),
            ]
              .filter(Boolean)
              .join(" & ")}
          </Heading>
        ) : null}
        {supportsNormalization && <Field name="normalization" component={NormalizationField} />}
        {supportsTransformations && (
          <FieldArray name="transformations">
            {(formProps) => (
              <TransformationField
                onStartEdit={onStartEditTransformation}
                onEndEdit={onEndEditTransformation}
                {...formProps}
              />
            )}
          </FieldArray>
        )}
      </FlexContainer>
    </Card>
  );
};
