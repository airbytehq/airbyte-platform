import { faCopy } from "@fortawesome/free-regular-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import classNames from "classnames";
import { useRef, useState } from "react";
import { useIntl } from "react-intl";

import styles from "./CopyButton.module.scss";
import { Button } from "../Button";
import { Icon } from "../Icon";

interface CopyButtonProps {
  className?: string;
  content: string;
  title?: string;
}

export const CopyButton: React.FC<CopyButtonProps> = ({ className, content, title }) => {
  const { formatMessage } = useIntl();
  const [copied, setCopied] = useState(false);

  const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const handleClick = () => {
    if (timeoutRef.current) {
      clearTimeout(timeoutRef.current);
    }

    navigator.clipboard.writeText(content).then(() => {
      setCopied(true);
      timeoutRef.current = setTimeout(() => setCopied(false), 2500);
    });
  };

  return (
    <Button
      size="xs"
      className={classNames(className, styles.button)}
      variant="secondary"
      title={title || formatMessage({ id: "copyButton.title" })}
      icon={
        <div>
          <FontAwesomeIcon icon={faCopy} />
          {copied && <Icon className={styles.success} type="success" color="success" />}
        </div>
      }
      onClick={handleClick}
    />
  );
};
