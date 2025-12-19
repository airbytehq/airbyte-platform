import { useEffect, useState } from "react";

import { Text } from "components/ui/Text";

import styles from "./StreamingIndicator.module.scss";

const LOADING_MESSAGES = [
  "Analyzing workspace...",
  "Checking connections...",
  "Reviewing sync logs...",
  "Investigating...",
  "Processing...",
];

export const StreamingIndicator: React.FC = () => {
  const [messageIndex, setMessageIndex] = useState(0);

  useEffect(() => {
    const interval = setInterval(() => {
      setMessageIndex((i) => (i + 1) % LOADING_MESSAGES.length);
    }, 2000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className={styles.indicator}>
      <div className={styles.pulse} />
      <Text size="sm" color="grey">
        {LOADING_MESSAGES[messageIndex]}
      </Text>
    </div>
  );
};
