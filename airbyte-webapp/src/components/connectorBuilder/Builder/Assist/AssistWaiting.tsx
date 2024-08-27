import React from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import aiProcessing from "./assistwaiting.gif";
import styles from "./AssistWaiting.module.scss";

interface AssistWaitingProps {
  onSkip: () => void;
}

export const AssistWaiting: React.FC<AssistWaitingProps> = ({ onSkip }) => {
  return (
    <FlexContainer alignItems="center" direction="column" gap="md" className={styles.container}>
      <img src={aiProcessing} alt="" className={styles.aiProcessingImage} />
      <Text size="xl" color="darkBlue">
        <FormattedMessage id="connectorBuilder.assist.waiting.message" />
      </Text>
      <Button onClick={onSkip} variant="link">
        <Text size="sm" color="grey">
          <FormattedMessage id="connectorBuilder.assist.waiting.skip" />
        </Text>
      </Button>
    </FlexContainer>
  );
};
