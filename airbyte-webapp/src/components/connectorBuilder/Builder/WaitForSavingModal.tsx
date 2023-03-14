import { Transition } from "history";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Modal, ModalFooter } from "components/ui/Modal";
import { Spinner } from "components/ui/Spinner";
import { Text } from "components/ui/Text";

import styles from "./WaitForSavingModal.module.scss";

export const WaitForSavingModal: React.FC<{ pendingTransition: Transition }> = ({ pendingTransition }) => {
  return (
    <Modal title="Waiting for save" testId="waitForSaveModal">
      <FlexContainer direction="column" alignItems="center" className={styles.container}>
        <Spinner />
        <Text>
          <FormattedMessage id="connectorBuilder.waitForSaving" />
        </Text>
      </FlexContainer>
      <ModalFooter>
        <FlexContainer direction="row-reverse">
          <Button
            onClick={() => {
              pendingTransition.retry();
            }}
            variant="danger"
          >
            <FormattedMessage id="connectorBuilder.waitForSaving.cancelButton" />
          </Button>
        </FlexContainer>
      </ModalFooter>
    </Modal>
  );
};
