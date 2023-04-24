import { faUser } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { useLocalStorage } from "react-use";

import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Message } from "components/ui/Message";
import { Modal, ModalBody } from "components/ui/Modal";
import { NumberBadge } from "components/ui/NumberBadge";
import { Tooltip } from "components/ui/Tooltip";

import { SourceDefinitionSpecificationDraft } from "core/domain/connector";
import { StreamReadRequestBodyConfig } from "core/request/ConnectorBuilderClient";
import { useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { useConnectorBuilderTestState } from "services/connectorBuilder/ConnectorBuilderStateService";
import { useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";
import { ConnectorForm } from "views/Connector/ConnectorForm";

import styles from "./ConfigMenu.module.scss";
import { ConfigMenuErrorBoundaryComponent } from "./ConfigMenuErrorBoundary";

interface ConfigMenuProps {
  testInputJsonErrors: number;
  isOpen: boolean;
  setIsOpen: (open: boolean) => void;
}

export const ConfigMenu: React.FC<ConfigMenuProps> = ({ testInputJsonErrors, isOpen, setIsOpen }) => {
  const { jsonManifest, editorView, setEditorView } = useConnectorBuilderFormState();
  const { testInputJson, setTestInputJson } = useConnectorBuilderTestState();
  const { trackError } = useAppMonitoringService();

  const [showInputsWarning, setShowInputsWarning] = useLocalStorage<boolean>("connectorBuilderInputsWarning", true);

  const switchToYaml = () => {
    setEditorView("yaml");
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
              size="sm"
              variant="secondary"
              data-testid="test-inputs"
              onClick={() => setIsOpen(true)}
              disabled={
                !jsonManifest.spec ||
                Object.keys(jsonManifest.spec.connection_specification?.properties || {}).length === 0
              }
              icon={<FontAwesomeIcon className={styles.icon} icon={faUser} />}
            >
              <FormattedMessage id="connectorBuilder.inputsButton" />
            </Button>
            {testInputJsonErrors > 0 && (
              <NumberBadge className={styles.inputsErrorBadge} value={testInputJsonErrors} color="red" />
            )}
          </>
        }
        placement={editorView === "yaml" ? "left" : "top"}
        containerClassName={styles.container}
      >
        {jsonManifest.spec ? (
          <FormattedMessage id="connectorBuilder.inputsTooltip" />
        ) : editorView === "ui" ? (
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
              currentView={editorView}
              closeAndSwitchToYaml={switchToYaml}
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
                  formType="source"
                  bodyClassName={styles.formContent}
                  selectedConnectorDefinitionSpecification={connectorDefinitionSpecification}
                  formValues={{ connectionConfiguration: testInputJson }}
                  onSubmit={async (values) => {
                    setTestInputJson(values.connectionConfiguration as StreamReadRequestBodyConfig);
                    setIsOpen(false);
                  }}
                  isEditMode
                  renderFooter={({ dirty, isSubmitting, resetConnectorForm }) => (
                    <div className={styles.inputFormModalFooter}>
                      <FlexContainer>
                        <FlexItem grow>
                          <Button
                            onClick={() => {
                              resetConnectorForm();
                              setTestInputJson(undefined);
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
