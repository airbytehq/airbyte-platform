import React from "react";
import { FormattedMessage } from "react-intl";

import { Message } from "components/ui/Message";

import styles from "./PlusPromoCreditsCallout.module.scss";

export const PlusPromoCreditsCallout: React.FC = () => (
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
