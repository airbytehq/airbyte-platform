import { useState } from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { ModalBody, ModalFooter } from "components/ui/Modal";

import styles from "./ChangesReviewModal.module.scss";
import { ChangeWarningCard } from "./ChangeWarningCard";
import { ChangesMap, ChangeWarning } from "./connectionUpdateHelpers";

interface ChangesReviewModalProps {
  changes: Partial<ChangesMap>;
  onCancel: () => void;
  onContinue: (decisions: Record<string, "accept" | "reject">) => void;
}

type DecisionsMap = {
  [key in ChangeWarning]: "accept" | "reject";
};

export const ChangesReviewModal: React.FC<ChangesReviewModalProps> = ({ changes, onCancel, onContinue }) => {
  const initialValues = Object.keys(changes).reduce((acc, current) => {
    acc[current as ChangeWarning] = "accept";
    return acc;
  }, {} as DecisionsMap);
  const [decisions, setDecisions] = useState<DecisionsMap>(initialValues);

  const handleDecision = (changeWarningId: string, decision: "accept" | "reject") => {
    setDecisions((prev) => ({
      ...prev,
      [changeWarningId]: decision,
    }));
  };

  return (
    <>
      <ModalBody className={styles.modalBody} data-testid="changes-review-modal-body">
        <FlexContainer direction="column" gap="lg">
          <FlexContainer
            className={styles.changesContainer}
            direction="column"
            gap="lg"
            data-testid="changes-review-modal-changes-container"
          >
            {Object.entries(changes).map(([warning, affectedStreams]) => (
              <ChangeWarningCard
                key={warning}
                warning={warning as ChangeWarning}
                affectedStreams={affectedStreams}
                decision={decisions[warning as ChangeWarning]}
                onDecision={(decision) => handleDecision(warning, decision)}
              />
            ))}
          </FlexContainer>
        </FlexContainer>
      </ModalBody>

      <ModalFooter data-testid="changes-review-modal-footer">
        <Button onClick={onCancel} variant="secondary" data-testid="changes-review-modal-cancel-btn">
          <FormattedMessage id="form.cancel" defaultMessage="Cancel" />
        </Button>
        <Button onClick={() => onContinue(decisions)} variant="primary" data-testid="changes-review-modal-continue-btn">
          <FormattedMessage id="connection.reviewChanges.continue" defaultMessage="Continue with Changes" />
        </Button>
      </ModalFooter>
    </>
  );
};
