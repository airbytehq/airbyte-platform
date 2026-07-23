import React from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { LoadingSkeleton } from "components/ui/LoadingSkeleton";
import { Text } from "components/ui/Text";

import styles from "./AssistWaiting.module.scss";

interface AssistWaitingProps {
  onSkip: () => void;
}

const FloatingLoadingSkeleton = () => (
  <div className={styles.floatingAnimation}>
    <LoadingSkeleton variant="magic" className={styles.loadingSkeleton} />
  </div>
);

const AssistLoadingAnimation = () => {
  return (
    <FlexContainer direction="column" gap="lg" className={styles.assistLoadingAnimation}>
      <FloatingLoadingSkeleton />
      <FloatingLoadingSkeleton />
      <FloatingLoadingSkeleton />
    </FlexContainer>
  );
};

export const AssistWaiting: React.FC<AssistWaitingProps> = ({ onSkip }) => {
  return (
    <FlexContainer alignItems="center" direction="column" gap="md" className={styles.container}>
      <AssistLoadingAnimation />
      <Heading as="h3" color="darkBlue">
        <FormattedMessage id="connectorBuilder.assist.waiting.message" />
      </Heading>
      <Button onClick={onSkip} variant="link">
        <Text size="sm" color="grey">
          <FormattedMessage id="connectorBuilder.assist.waiting.skip" />
        </Text>
      </Button>
    </FlexContainer>
  );
};
