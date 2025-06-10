import classNames from "classnames";
import { useRef, useState } from "react";
import { useIntl } from "react-intl";

import styles from "./CopyButton.module.scss";
import { Button } from "../Button";
import { Icon } from "../Icon";

interface CopyButtonProps {
  className?: string;
  content: string | (() => string);
  title?: string;
  variant?: "secondary" | "clear";
  iconPosition?: "left" | "right";
  full?: boolean;
}

export const CopyButton: React.FC<React.PropsWithChildren<CopyButtonProps>> = ({
  className,
  content,
  title,
  children,
  variant = "secondary",
  iconPosition = "left",
  full = false,
}) => {
  const { formatMessage } = useIntl();
  const [copied, setCopied] = useState(false);

  const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const handleClick = () => {
    if (timeoutRef.current) {
      clearTimeout(timeoutRef.current);
    }

    const text = typeof content === "string" ? content : content();

    navigator.clipboard.writeText(text).then(() => {
      setCopied(true);
      timeoutRef.current = setTimeout(() => setCopied(false), 2500);
    });
  };

  return (
    <Button
      size="xs"
      className={classNames(className, styles.button)}
      variant={variant}
      title={title || formatMessage({ id: "copyButton.title" })}
      onClick={handleClick}
      icon={children ? "copy" : undefined}
      iconPosition={iconPosition}
      full={full}
      type="button"
      data-testid="copy-button"
    >
      {copied && <Icon className={styles.success} type="successFilled" color="success" />}
      {children ? undefined : <Icon type="copy" />}
      {children}
    </Button>
  );
};
