import { useEffect, useState } from "react";
import { useIntl } from "react-intl";

import { Text } from "components/ui/Text";

import styles from "./ThinkingIndicator.module.scss";

const THINKING_MESSAGE_KEYS = [
  "chat.thinking.investigating",
  "chat.thinking.processing",
  "chat.thinking.searching",
  "chat.thinking.connecting",
  "chat.thinking.diagnosing",
  "chat.thinking.replicating",
  "chat.thinking.airbytering",
  "chat.thinking.leveraging",
  "chat.thinking.powering",
];

const getRandomMessageKey = () => {
  return THINKING_MESSAGE_KEYS[Math.floor(Math.random() * THINKING_MESSAGE_KEYS.length)];
};

export const ThinkingIndicator: React.FC = () => {
  const { formatMessage } = useIntl();
  const [messageKey, setMessageKey] = useState(getRandomMessageKey());

  useEffect(() => {
    const interval = setInterval(() => {
      setMessageKey(getRandomMessageKey());
    }, 1000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className={styles.indicator}>
      <div className={styles.pulse} />
      <Text size="sm" color="grey">
        {formatMessage({ id: messageKey })}
      </Text>
    </div>
  );
};
