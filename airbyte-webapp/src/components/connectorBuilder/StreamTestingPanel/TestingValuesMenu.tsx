import { useCallback, useMemo, useState } from "react";
import { useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Message } from "components/ui/Message";
import { Modal, ModalBody } from "components/ui/Modal";
import { NumberBadge } from "components/ui/NumberBadge";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { ConnectorBuilderProjectTestingValues } from "core/api/types/AirbyteClient";
import { Spec } from "core/api/types/ConnectorManifest";
import { SourceDefinitionSpecificationDraft } from "core/domain/connector";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
import { useLocalStorage } from "core/utils/useLocalStorage";
import {
  applyTestingValuesDefaults,
  useConnectorBuilderFormState,
  useConnectorBuilderTestRead,
} from "services/connectorBuilder/ConnectorBuilderStateService";
import { ConnectorForm } from "views/Connector/ConnectorForm";

import styles from "./TestingValuesMenu.module.scss";
import { TestingValuesMenuErrorBoundaryComponent } from "./TestingValuesMenuErrorBoundary";
import { useBuilderWatch } from "../types";

interface TestingValuesMenuProps {
  testingValuesErrors: number;
  isOpen: boolean;
  setIsOpen: (open: boolean) => void;
}

export const TestingValuesMenu: React.FC<TestingValuesMenuProps> = ({ testingValuesErrors, isOpen, setIsOpen }) => {
  const { setValue } = useFormContext();
  const mode = useBuilderWatch("mode");
  const {
    jsonManifest: { spec },
    permission,
  } = useConnectorBuilderFormState();
  const {
    streamRead: { isFetching },
  } = useConnectorBuilderTestRead();

  const [showInputsWarning, setShowInputsWarning] = useLocalStorage("connectorBuilderInputsWarning", true);

  const closeAndSwitchToYaml = () => {
    setValue("mode", "yaml");
    setIsOpen(false);
  };

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
                isFetching || !spec || Object.keys(spec.connection_specification?.properties || {}).length === 0
              }
              icon="user"
              iconClassName={styles.icon}
            >
              <FormattedMessage id="connectorBuilder.inputsButton" />
            </Button>
            {testingValuesErrors > 0 && (
              <NumberBadge className={styles.inputsErrorBadge} value={testingValuesErrors} color="red" />
            )}
          </>
        }
        placement="left"
        containerClassName={styles.container}
      >
        {spec ? (
          <FormattedMessage id="connectorBuilder.inputsTooltip" />
        ) : mode === "ui" ? (
          <FormattedMessage id="connectorBuilder.inputsNoSpecUITooltip" />
        ) : (
          <FormattedMessage id="connectorBuilder.inputsNoSpecYAMLTooltip" />
        )}
      </Tooltip>
      {isOpen && spec && (
        <Modal
          size="lg"
          onCancel={() => setIsOpen(false)}
          title={<FormattedMessage id="connectorBuilder.testingValuesMenuTitle" />}
        >
          <ModalBody>
            <TestingValuesMenuErrorBoundaryComponent currentMode={mode} closeAndSwitchToYaml={closeAndSwitchToYaml}>
              <FlexContainer direction="column">
                {showInputsWarning && (
                  <Message
                    type="info"
                    onClose={() => {
                      setShowInputsWarning(false);
                    }}
                    text={<FormattedMessage id="connectorBuilder.inputsFormMessage" />}
                  />
                )}
                {permission === "adminReadOnly" && (
                  <Text color="red">
                    <FormattedMessage id="connectorBuilder.adminTestingValuesWarning" />
                  </Text>
                )}
                <TestingValuesForm spec={spec} setIsOpen={setIsOpen} />
              </FlexContainer>
            </TestingValuesMenuErrorBoundaryComponent>
          </ModalBody>
        </Modal>
      )}
    </>
  );
};

interface TestingValuesFormProps {
  spec?: Spec;
  setIsOpen: TestingValuesMenuProps["setIsOpen"];
}

const TestingValuesForm: React.FC<TestingValuesFormProps> = ({ spec, setIsOpen }) => {
  const canEditConnector = useGeneratedIntent(Intent.CreateOrEditConnector);
  const testingValues = useBuilderWatch("testingValues");
  const { updateTestingValues } = useConnectorBuilderFormState();
  const [connectorFormValues, setConnectorFormValues] = useState(testingValues);
  const [connectorFormKey, setConnectorFormKey] = useState(0);
  const [formWasReset, setFormWasReset] = useState(false);
  const resetConnectorFormValues = useCallback((values?: ConnectorBuilderProjectTestingValues) => {
    setFormWasReset(true);
    setConnectorFormValues(values);
  }, []);
  const incrementConnectorFormKey = useCallback(() => setConnectorFormKey((prev) => prev + 1), []);

  const connectorDefinitionSpecification: SourceDefinitionSpecificationDraft | undefined = useMemo(
    () =>
      spec
        ? {
            documentationUrl: spec.documentation_url,
            connectionSpecification: spec.connection_specification,
          }
        : undefined,
    [spec]
  );

  return (
    <ConnectorForm
      canEdit={canEditConnector}
      key={`testing-values-form-${connectorFormKey}`}
      formType="source"
      bodyClassName={styles.formContent}
      selectedConnectorDefinitionSpecification={connectorDefinitionSpecification}
      formValues={{ connectionConfiguration: connectorFormValues }}
      onSubmit={async (values) => {
        await updateTestingValues({
          spec: spec?.connection_specification ?? {},
          testingValues: values.connectionConfiguration as ConnectorBuilderProjectTestingValues,
        });
        setIsOpen(false);
      }}
      isEditMode
      renderFooter={({ dirty, isSubmitting }) => (
        <div className={styles.inputFormModalFooter}>
          <FlexContainer>
            <FlexItem grow>
              <ResetButton
                spec={spec}
                resetConnectorFormValues={resetConnectorFormValues}
                incrementConnectorFormKey={incrementConnectorFormKey}
              />
            </FlexItem>
            <Button type="button" variant="secondary" onClick={() => setIsOpen(false)}>
              <FormattedMessage id="form.cancel" />
            </Button>
            <Button type="submit" disabled={isSubmitting || (!dirty && !formWasReset)} isLoading={isSubmitting}>
              <FormattedMessage id="connectorBuilder.saveInputsForm" />
            </Button>
          </FlexContainer>
        </div>
      )}
    />
  );
};

const ResetButton: React.FC<{
  spec?: Spec;
  resetConnectorFormValues: (values?: ConnectorBuilderProjectTestingValues) => void;
  incrementConnectorFormKey: () => void;
}> = ({ spec, resetConnectorFormValues, incrementConnectorFormKey }) => {
  return (
    <Button
      onClick={() => {
        const emptyValuesWithDefaults = applyTestingValuesDefaults({}, spec);
        resetConnectorFormValues(emptyValuesWithDefaults);
        incrementConnectorFormKey();
      }}
      type="button"
      variant="danger"
    >
      <FormattedMessage id="form.reset" />
    </Button>
  );
};
