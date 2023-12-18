import { useCallback } from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";

import { useGenerateApplicationToken } from "core/api";
import { useModalService } from "hooks/services/Modal";

import styles from "./ActionButton.module.scss";
import { TokenModalBody } from "./TokenModal";

export const GenerateTokenControl: React.FC<{ clientId: string; clientSecret: string }> = ({
  clientId,
  clientSecret,
}) => {
  const { mutateAsync: generateToken, isLoading } = useGenerateApplicationToken();
  const { openModal } = useModalService();

  const onGenerateToken = useCallback(async () => {
    const { access_token } = await generateToken({ client_id: clientId, client_secret: clientSecret });

    return openModal({
      title: <FormattedMessage id="settings.application.token.new" />,
      content: () => <TokenModalBody token={access_token} />,
      size: "md",
    });
  }, [generateToken, clientId, clientSecret, openModal]);

  return (
    <Button className={styles.actionButton} onClick={onGenerateToken} variant="dark" isLoading={isLoading}>
      <FormattedMessage id="settings.applications.table.generateToken" />
    </Button>
  );
};
