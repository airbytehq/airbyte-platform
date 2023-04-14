import classNames from "classnames";

import styles from "./InfoText.module.scss";
import { Text, TextWithOverflowTooltip } from "../Text";

export type InfoTextVariant = "grey" | "light-grey" | "red" | "green" | "blue" | "light-blue";

const STYLES_BY_VARIANT: Readonly<Record<InfoTextVariant, string>> = {
  grey: styles.grey,
  blue: styles.blue,
  green: styles.green,
  red: styles.red,
  "light-blue": styles.lightBlue,
  "light-grey": styles.lightGrey,
};

interface InfoTextProps {
  variant?: InfoTextVariant;
  className?: string;
  withOverflowTooltip?: boolean;
  ["data-testid"]?: string;
}

export const InfoText: React.FC<InfoTextProps> = ({
  children,
  variant = "grey",
  className,
  "data-testid": testId,
  withOverflowTooltip = false,
}) => {
  const containerClassName = classNames(styles.container, STYLES_BY_VARIANT[variant], className);
  return (
    <div className={containerClassName} data-testid={testId}>
      {withOverflowTooltip ? (
        <TextWithOverflowTooltip size="xs" className={styles.text}>
          {children}
        </TextWithOverflowTooltip>
      ) : (
        <Text size="xs" className={styles.text}>
          {children}
        </Text>
      )}
    </div>
  );
};
