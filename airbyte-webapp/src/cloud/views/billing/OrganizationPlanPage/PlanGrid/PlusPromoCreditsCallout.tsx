import dayjs from "dayjs";
import React from "react";
import { FormattedMessage } from "react-intl";

import { Message } from "components/ui/Message";

import styles from "./PlusPromoCreditsCallout.module.scss";

/**
 * Midnight Pacific time at the end of October 21, 2026 (PDT, UTC-7). The callout is visible
 * through October 21 inclusive and hidden from this instant onward.
 */
export const PLUS_PROMO_CREDITS_CALLOUT_END = "2026-10-22T00:00:00-07:00";

export const PlusPromoCreditsCallout: React.FC = () => {
  if (!dayjs().isBefore(PLUS_PROMO_CREDITS_CALLOUT_END)) {
    return null;
  }

  return (
    <Message
      type="info"
      data-testid="plus-promo-credits-callout"
      text={
        <>
          <FormattedMessage id="planGrid.plusPromoCredits.intro" />
          <ul className={styles.tiers}>
            <FormattedMessage
              id="planGrid.plusPromoCredits.tiers"
              values={{ li: (node: React.ReactNode) => <li>{node}</li> }}
            />
          </ul>
        </>
      }
    />
  );
};
