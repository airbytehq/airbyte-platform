import React from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { useDeleteConnection } from "core/api";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useDeleteModal } from "hooks/useDeleteModal";

import styles from "./ConnectionDeleteBlock.module.scss";

export const ConnectionDeleteBlock: React.FC = () => {
  const { mode } = useConnectionFormService();
  const { connection } = useConnectionEditService();
  const { mutateAsync: deleteConnection } = useDeleteConnection();
  const onDelete = () => deleteConnection(connection);

  const onDeleteButtonClick = useDeleteModal("connection", onDelete, undefined, connection.name);

  return (
    <Card>
      <FlexContainer direction="row" justifyContent="space-between" alignItems="center" className={styles.text}>
        <FlexContainer direction="column">
          <Text size="lg">
            <FormattedMessage id="tables.connectionDelete.title" />
          </Text>
          <Text size="xs" color="grey">
            <FormattedMessage id="tables.connectionDataDelete" />
          </Text>
        </FlexContainer>
        <Button
          variant="danger"
          onClick={onDeleteButtonClick}
          data-id="open-delete-modal"
          disabled={mode === "readonly"}
        >
          <FormattedMessage id="tables.connectionDelete" />
        </Button>
      </FlexContainer>
    </Card>
  );
};
