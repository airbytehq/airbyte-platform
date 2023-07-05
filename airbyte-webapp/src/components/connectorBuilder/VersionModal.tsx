import classNames from "classnames";
import { useEffect, useState } from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Message } from "components/ui/Message";
import { Modal, ModalBody } from "components/ui/Modal";
import { Spinner } from "components/ui/Spinner";
import { Text } from "components/ui/Text";

import {
  BuilderProject,
  useListVersions,
  useResolvedProjectVersion,
} from "services/connectorBuilder/ConnectorBuilderProjectsService";
import {
  useConnectorBuilderFormState,
  useConnectorBuilderTestRead,
} from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./VersionModal.module.scss";

export const VersionModal: React.FC<{
  project: BuilderProject;
  onClose: () => void;
}> = ({ onClose, project }) => {
  const { displayedVersion, previousManifestDraft, setDisplayedVersion } = useConnectorBuilderFormState();
  const { setTestStreamIndex } = useConnectorBuilderTestRead();
  const { data: versions, isLoading: isLoadingVersionList } = useListVersions(project);
  const [selectedVersion, setSelectedVersion] = useState<number | undefined>(undefined);
  const { data, isLoading } = useResolvedProjectVersion(project.id, selectedVersion);

  async function onSelect(version: number) {
    setSelectedVersion(version);
  }

  useEffect(() => {
    if (isLoading || !data || selectedVersion === undefined) {
      return;
    }

    setDisplayedVersion(selectedVersion, data);
    setTestStreamIndex(0);
    onClose();
  }, [data, isLoading, onClose, selectedVersion, setDisplayedVersion, setTestStreamIndex]);

  return (
    <Modal size="sm" title={<FormattedMessage id="connectorBuilder.versionModal.title" />} onClose={onClose}>
      <ModalBody>
        {isLoadingVersionList ? (
          <FlexContainer justifyContent="center">
            <Spinner size="md" />
          </FlexContainer>
        ) : (
          <FlexContainer direction="column" data-testid="versions-list">
            {(displayedVersion === undefined || previousManifestDraft) && (
              <Message type="warning" text={<FormattedMessage id="connectorBuilder.versionModal.warning" />} />
            )}
            {previousManifestDraft && (
              <button
                type="button"
                onClick={() => {
                  setDisplayedVersion(undefined, previousManifestDraft);
                  onClose();
                }}
                className={classNames(styles.versionItem)}
              >
                <Text size="md" as="span">
                  <FormattedMessage id="connectorBuilder.versionModal.draftLabel" />
                </Text>
              </button>
            )}
            {(versions || []).map((version, index) => (
              <button
                key={index}
                onClick={() => {
                  onSelect(version.version);
                }}
                className={classNames(styles.versionItem)}
              >
                <FlexContainer alignItems="baseline">
                  <Text size="md" as="span">
                    v{version.version}{" "}
                  </Text>
                  <FlexItem grow>
                    <Text size="sm" as="span" color="grey">
                      {version.description}
                    </Text>
                  </FlexItem>
                  {isLoading && version.version === selectedVersion && <Spinner size="xs" />}
                </FlexContainer>
              </button>
            ))}
          </FlexContainer>
        )}
      </ModalBody>
    </Modal>
  );
};
