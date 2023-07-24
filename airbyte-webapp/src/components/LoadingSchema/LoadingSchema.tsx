import React from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { ProgressBar } from "components/ui/ProgressBar";
import { Text } from "components/ui/Text";

import styles from "./LoadingSchema.module.scss";

// Progress Bar runs 4min for discoveries schema
const PROGRESS_BAR_TIME = 60 * 4;

const LoadingSchema: React.FC = () => (
  <Box p="2xl" className={styles.container}>
    <FlexContainer direction="column" gap="lg" alignItems="center">
      <ProgressBar runTime={PROGRESS_BAR_TIME} />
      <Text size="md" className={styles.message} align="center">
        <FormattedMessage id="onboarding.fetchingSchema" />
      </Text>
    </FlexContainer>
  </Box>
);

export default LoadingSchema;
