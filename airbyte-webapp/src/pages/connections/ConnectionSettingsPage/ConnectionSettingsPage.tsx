import { faChevronDown, faChevronRight } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { Disclosure } from "@headlessui/react";
import React from "react";
import { FormattedMessage } from "react-intl";

import { DeleteBlock } from "components/common/DeleteBlock";
import { UpdateConnectionDataResidency } from "components/connection/UpdateConnectionDataResidency";
import { UpdateConnectionName } from "components/connection/UpdateConnectionName/UpdateConnectionName";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Spinner } from "components/ui/Spinner";

import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useExperiment } from "hooks/services/Experiment";
import { FeatureItem, useFeature } from "hooks/services/Feature";
import { useDeleteConnection } from "hooks/services/useConnectionHook";

import styles from "./ConnectionSettingsPage.module.scss";
import { SchemaUpdateNotifications } from "./SchemaUpdateNotifications";
import { StateBlock } from "./StateBlock";

export const ConnectionSettingsPage: React.FC = () => {
  const { connection } = useConnectionEditService();
  const { mutateAsync: deleteConnection } = useDeleteConnection();
  const canUpdateDataResidency = useFeature(FeatureItem.AllowChangeDataGeographies);
  const canSendSchemaUpdateNotifications = useFeature(FeatureItem.AllowAutoDetectSchema);
  const isUpdatedConnectionFlow = useExperiment("connection.updatedConnectionFlow", false);

  useTrackPage(PageTrackingCodes.CONNECTIONS_ITEM_SETTINGS);
  const onDelete = () => deleteConnection(connection);

  return (
    <div className={styles.container}>
      <FlexContainer direction="column" justifyContent="flex-start">
        {isUpdatedConnectionFlow && <UpdateConnectionName />}
        {canSendSchemaUpdateNotifications && <SchemaUpdateNotifications />}
        {canUpdateDataResidency && <UpdateConnectionDataResidency />}
        {connection.status !== "deprecated" && <DeleteBlock type="connection" onDelete={onDelete} />}
      </FlexContainer>
      <Disclosure>
        {({ open }) => (
          <>
            <Disclosure.Button
              as={Button}
              variant="clear"
              icon={<FontAwesomeIcon icon={open ? faChevronDown : faChevronRight} />}
              className={styles.advancedButton}
            >
              <FormattedMessage id="connectionForm.settings.advancedButton" />
            </Disclosure.Button>
            <Disclosure.Panel className={styles.advancedPanel}>
              <React.Suspense fallback={<Spinner />}>
                <StateBlock connectionId={connection.connectionId} />
              </React.Suspense>
            </Disclosure.Panel>
          </>
        )}
      </Disclosure>
    </div>
  );
};
