import React from "react";
import { FormattedMessage } from "react-intl";

import { PageContainer } from "components/PageContainer";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { FeatureItem, useFeature } from "core/services/features";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";

import styles from "./ConnectionTransformationPage.module.scss";
import { DbtCloudTransformations } from "./DbtCloudTransformations";
import { NormalizationForm } from "./NormalizationForm";

export const ConnectionTransformationPage: React.FC = () => {
  const { destDefinitionVersion } = useConnectionFormService();

  useTrackPage(PageTrackingCodes.CONNECTIONS_ITEM_TRANSFORMATION);
  const supportsNormalization = destDefinitionVersion.normalizationConfig.supported;
  const supportsCloudDbtIntegration =
    useFeature(FeatureItem.AllowDBTCloudIntegration) && destDefinitionVersion.supportsDbt;
  const noSupportedTransformations = !supportsNormalization && !supportsCloudDbtIntegration;

  return (
    <PageContainer centered>
      <FlexContainer direction="column" gap="lg">
        {supportsNormalization && <NormalizationForm />}
        {supportsCloudDbtIntegration && <DbtCloudTransformations />}
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
