import React, { useMemo } from "react";
import { useIntl } from "react-intl";

import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { FeatureItem, useFeature } from "core/services/features";
import { useConnectionHookFormService } from "hooks/services/ConnectionForm/ConnectionHookFormService";

import { NormalizationHookFormField } from "./NormalizationHookFormField";
import { TransformationFieldHookForm } from "./TransformationFieldHookForm";

export const OperationsSectionHookForm: React.FC = () => {
  const { formatMessage } = useIntl();
  const {
    destDefinitionVersion: { normalizationConfig, supportsDbt },
  } = useConnectionHookFormService();

  const supportsNormalization = normalizationConfig.supported;
  const supportsTransformations = useFeature(FeatureItem.AllowCustomDBT) && supportsDbt;

  const titleKey = useMemo(() => {
    if (supportsNormalization && supportsTransformations) {
      return "connectionForm.normalizationAndTransformation.title";
    } else if (supportsNormalization) {
      return "connectionForm.normalization.title";
    }
    return "connectionForm.transformation.title";
  }, [supportsNormalization, supportsTransformations]);

  if (!supportsNormalization && !supportsTransformations) {
    return null;
  }

  return (
    <Card withPadding>
      <FlexContainer direction="column" gap="lg">
        {supportsNormalization || supportsTransformations ? (
          <Heading as="h2" size="sm">
            {formatMessage({ id: titleKey })}
          </Heading>
        ) : null}
        {supportsNormalization && <NormalizationHookFormField />}
        {supportsTransformations && <TransformationFieldHookForm />}
      </FlexContainer>
    </Card>
  );
};
