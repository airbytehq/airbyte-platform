import React from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { Heading } from "components/ui/Heading";

import octavia from "./biting-nails.png";
import styles from "./ErrorOccurredView.module.scss";

interface ErrorOccurredViewProps {
  message: React.ReactNode;
  ctaButtonText?: React.ReactNode;
  onCtaButtonClick?: React.MouseEventHandler;
}

/**
 * @deprecated Replaced by `ErrorDetails` component. Will be removed once the speakeasy portal forward has been removed.
 */
export const ErrorOccurredView: React.FC<ErrorOccurredViewProps> = ({ message, onCtaButtonClick, ctaButtonText }) => {
  return (
    <div className={styles.errorOccurredView} data-testid="errorView">
      <div className={styles.content}>
        <img src={octavia} alt="" className={styles.octavia} />
        <Heading as="h2" size="lg" centered>
          <FormattedMessage id="errorView.title" />
        </Heading>
        <p className={styles.message}>{message}</p>
        {onCtaButtonClick && ctaButtonText && (
          <Button size="lg" onClick={onCtaButtonClick}>
            {ctaButtonText}
          </Button>
        )}
      </div>
    </div>
  );
};
