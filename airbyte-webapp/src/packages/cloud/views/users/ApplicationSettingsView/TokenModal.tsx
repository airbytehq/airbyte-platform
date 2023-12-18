import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { CopyButton } from "components/ui/CopyButton";
import { Icon } from "components/ui/Icon";
import { Message } from "components/ui/Message";
import { ModalBody, ModalFooter } from "components/ui/Modal";

import { FILE_TYPE_DOWNLOAD, downloadFile, fileizeString } from "core/utils/file";

import styles from "./TokenModal.module.scss";

export const TokenModalBody: React.FC<{ token: string }> = ({ token }) => {
  const dateString = new Date().toISOString().split("T")[0];

  const downloadFileWithToken: () => void = () => {
    const file = new Blob([token], {
      type: FILE_TYPE_DOWNLOAD,
    });
    downloadFile(file, fileizeString(`airbyteToken-${dateString}`));
  };

  return (
    <>
      <ModalBody maxHeight={400}>
        <Message type="warning" text={<FormattedMessage id="settings.applications.token.warning" />} />
        <Box my="lg" pl="md" className={styles.tokenModal__scrollboxContainer}>
          <div className={styles.tokenModal__tokenScrollbox}>
            <pre className={styles.tokenModal__pre}>{token}</pre>
          </div>
        </Box>
      </ModalBody>
      <ModalFooter>
        <CopyButton content={token}>
          <FormattedMessage id="copyButton.title" />
        </CopyButton>
        <Button onClick={downloadFileWithToken} icon={<Icon type="download" size="sm" />}>
          <FormattedMessage id="settings.applications.token.download" />
        </Button>
      </ModalFooter>
    </>
  );
};
