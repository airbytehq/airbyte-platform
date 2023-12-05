import { useMemo } from "react";
import { useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Message } from "components/ui/Message";
import { Modal, ModalBody } from "components/ui/Modal";
import { NumberBadge } from "components/ui/NumberBadge";
import { Tooltip } from "components/ui/Tooltip";

import { ConnectorConfig } from "core/api/types/ConnectorBuilderClient";
import { SourceDefinitionSpecificationDraft } from "core/domain/connector";
import { useLocalStorage } from "core/utils/useLocalStorage";
import { useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import {
  useConnectorBuilderFormState,
  useConnectorBuilderTestRead,
} from "services/connectorBuilder/ConnectorBuilderStateService";
import { ConnectorForm } from "views/Connector/ConnectorForm";

import styles from "./ConfigMenu.module.scss";
import { ConfigMenuErrorBoundaryComponent } from "./ConfigMenuErrorBoundary";
import { useBuilderWatch } from "../types";

interface ConfigMenuProps {
  testInputJsonErrors: number;
  isOpen: boolean;
  setIsOpen: (open: boolean) => void;
}

export const ConfigMenu: React.FC<ConfigMenuProps> = ({ testInputJsonErrors, isOpen, setIsOpen }) => {
  const { setValue } = useFormContext();
  const mode = useBuilderWatch("mode");
  const { jsonManifest } = useConnectorBuilderFormState();
  const {
    testInputJson,
    setTestInputJson,
    testInputJsonDirty,
    streamRead: { isFetching },
  } = useConnectorBuilderTestRead();
  const { trackError } = useAppMonitoringService();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();

  const [showInputsWarning, setShowInputsWarning] = useLocalStorage("connectorBuilderInputsWarning", true);

  const closeAndSwitchToYaml = () => {
    setValue("mode", "yaml");
    setIsOpen(false);
  };

  const connectorDefinitionSpecification: SourceDefinitionSpecificationDraft | undefined = useMemo(
    () =>
      jsonManifest.spec
        ? {
            documentationUrl: jsonManifest.spec.documentation_url,
            connectionSpecification: jsonManifest.spec.connection_specification,
          }
        : undefined,
    [jsonManifest]
  );

  return (
    <>
      <Tooltip
        control={
          <>
            <Button
              type="button"
              size="sm"
              variant="secondary"
              data-testid="test-inputs"
              onClick={() => setIsOpen(true)}
              disabled={
                isFetching ||
                !jsonManifest.spec ||
                Object.keys(jsonManifest.spec.connection_specification?.properties || {}).length === 0
              }
              icon={<Icon type="user" className={styles.icon} />}
            >
              <FormattedMessage id="connectorBuilder.inputsButton" />
            </Button>
            {testInputJsonErrors > 0 && (
              <NumberBadge className={styles.inputsErrorBadge} value={testInputJsonErrors} color="red" />
            )}
          </>
        }
        placement={mode === "yaml" ? "left" : "top"}
        containerClassName={styles.container}
      >
        {jsonManifest.spec ? (
          <FormattedMessage id="connectorBuilder.inputsTooltip" />
        ) : mode === "ui" ? (
          <FormattedMessage id="connectorBuilder.inputsNoSpecUITooltip" />
        ) : (
          <FormattedMessage id="connectorBuilder.inputsNoSpecYAMLTooltip" />
        )}
      </Tooltip>
      {isOpen && connectorDefinitionSpecification && (
        <Modal
          size="lg"
          onClose={() => setIsOpen(false)}
          title={<FormattedMessage id="connectorBuilder.configMenuTitle" />}
        >
          <ModalBody>
            <ConfigMenuErrorBoundaryComponent
              currentMode={mode}
              closeAndSwitchToYaml={closeAndSwitchToYaml}
              trackError={trackError}
            >
              <FlexContainer direction="column">
                {showInputsWarning && (
                  <Message
                    className={styles.warningBox}
                    type="warning"
                    onClose={() => {
                      setShowInputsWarning(false);
                    }}
                    text={<FormattedMessage id="connectorBuilder.inputsFormWarning" />}
                  />
                )}
                <ConnectorForm
                  // re-mount the form when the form values change from the outside to avoid stale data in the form
                  key={testInputJsonDirty ? "with-testinput" : "without-testinput"}
                  formType="source"
                  bodyClassName={styles.formContent}
                  selectedConnectorDefinitionSpecification={connectorDefinitionSpecification}
                  formValues={{ connectionConfiguration: testInputJson }}
                  onSubmit={async (values) => {
                    setTestInputJson(values.connectionConfiguration as ConnectorConfig);
                    setIsOpen(false);
                  }}
                  isEditMode
                  renderFooter={({ dirty, isSubmitting, resetConnectorForm }) => (
                    <div className={styles.inputFormModalFooter}>
                      <FlexContainer>
                        <FlexItem grow>
                          <Button
                            onClick={() => {
                              openConfirmationModal({
                                title: "connectorBuilder.resetTestingValues.title",
                                text: "connectorBuilder.resetTestingValues.text",
                                submitButtonText: "connectorBuilder.resetTestingValues.submit",
                                onSubmit: () => {
                                  closeConfirmationModal();
                                  setTestInputJson(undefined);
                                  resetConnectorForm();
                                },
                              });
                            }}
                            type="button"
                            variant="danger"
                          >
                            <FormattedMessage id="form.reset" />
                          </Button>
                        </FlexItem>
                        <Button type="button" variant="secondary" onClick={() => setIsOpen(false)}>
                          <FormattedMessage id="form.cancel" />
                        </Button>
                        <Button type="submit" disabled={isSubmitting || !dirty}>
                          <FormattedMessage id="connectorBuilder.saveInputsForm" />
                        </Button>
                      </FlexContainer>
                    </div>
                  )}
                />
              </FlexContainer>
            </ConfigMenuErrorBoundaryComponent>
          </ModalBody>
        </Modal>
      )}
    </>
  );
};
