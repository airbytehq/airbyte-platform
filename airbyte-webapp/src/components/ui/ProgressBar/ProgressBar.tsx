import React from "react";
import { FormattedMessage } from "react-intl";

import styles from "./ProgressBar.module.scss";
import { FlexContainer } from "../Flex";
import { Text } from "../Text";

interface ProgressBarProps {
  runTime?: number;
  text?: React.ReactNode;
}

export const ProgressBar: React.FC<ProgressBarProps> = ({ runTime = 20, text }) => (
  <div className={styles.bar}>
    <div className={styles.progress} style={{ animationDuration: `${runTime}s` }}>
      <FlexContainer
        justifyContent="center"
        alignItems="center"
        className={styles.textContainer}
        style={{ animationDelay: `${runTime + 0.5}s` }}
      >
        <Text size="xs">{text || <FormattedMessage id="form.wait" />}</Text>
      </FlexContainer>
    </div>
  </div>
);
