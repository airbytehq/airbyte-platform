import { Fragment } from "react";

import { Text } from "components/ui/Text";

import styles from "./HotkeyLabel.module.scss";

export const HotkeyLabel = ({ keys }: { keys: string[] }) => {
  return (
    <Text className={styles.label} size="sm">
      {keys.map((key, index) => (
        <Fragment key={`${key}-${index}`}>
          <kbd>{key}</kbd>
          {index < keys.length - 1 && " + "}
        </Fragment>
      ))}
    </Text>
  );
};

export const getCtrlOrCmdKey = () => (navigator.userAgent.includes("Mac") ? "⌘ Cmd" : "Ctrl");
