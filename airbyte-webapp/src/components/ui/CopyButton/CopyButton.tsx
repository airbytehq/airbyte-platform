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
  /**
   * Called after the content has been written to the clipboard. Useful for gating other UI
   * (e.g. an exit CTA) on the copy having actually happened, without reaching into the button's
   * internal "copied" state.
   */
  onCopy?: () => void;
  /**
   * Called when the content can't be written to the clipboard - either because the write was
   * rejected (browser permissions, policy) or because the clipboard API isn't available at all
   * (any non-secure context). Lets consumers offer a manual fallback instead of silently doing
   * nothing.
   */
  onCopyError?: () => void;
}

export const CopyButton: React.FC<React.PropsWithChildren<CopyButtonProps>> = ({
  className,
  content,
  title,
  children,
  variant = "secondary",
  iconPosition = "left",
  full = false,
  onCopy,
  onCopyError,
}) => {
  const { formatMessage } = useIntl();
  const [copied, setCopied] = useState(false);

  const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const handleClick = () => {
    if (timeoutRef.current) {
      clearTimeout(timeoutRef.current);
    }

    const text = typeof content === "string" ? content : content();

    // Handling the failure at all - both the guard below and the `.catch()` - stops it surfacing
    // as an uncaught error, so consumers that don't pass `onCopyError` would otherwise lose the
    // signal entirely, including from error monitoring.
    const handleCopyError = (error: unknown) => {
      if (onCopyError) {
        onCopyError();
        return;
      }
      console.error("CopyButton: failed to write to the clipboard", error);
    };

    // `navigator.clipboard` is only exposed in secure contexts, so it's undefined entirely on
    // instances served over plain HTTP. Reading `.writeText` off it would throw synchronously,
    // before there's a promise to reject, so the `.catch()` below would never see it.
    if (!navigator.clipboard?.writeText) {
      handleCopyError(new Error("The clipboard API is unavailable outside a secure context"));
      return;
    }

    navigator.clipboard
      .writeText(text)
      .then(() => {
        setCopied(true);
        onCopy?.();
        timeoutRef.current = setTimeout(() => setCopied(false), 2500);
      })
      .catch(handleCopyError);
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
