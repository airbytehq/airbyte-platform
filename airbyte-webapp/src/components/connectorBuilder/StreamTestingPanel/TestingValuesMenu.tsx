import { useCallback, useMemo, useState } from "react";
import { useFormContext } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import { ValidationError } from "yup";

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
import { jsonSchemaToFormBlock } from "core/form/schemaToFormBlock";
import { buildYupFormForJsonSchema } from "core/form/schemaToYup";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
import { useLocalStorage } from "core/utils/useLocalStorage";
import {
  useConnectorBuilderFormManagementState,
  useConnectorBuilderFormState,
  useConnectorBuilderPermission,
  useConnectorBuilderTestRead,
} from "services/connectorBuilder/ConnectorBuilderStateService";
import { ConnectorForm } from "views/Connector/ConnectorForm";

import styles from "./TestingValuesMenu.module.scss";
import { TestingValuesMenuErrorBoundaryComponent } from "./TestingValuesMenuErrorBoundary";
import { useBuilderWatch } from "../useBuilderWatch";
import { applyTestingValuesDefaults } from "../useUpdateTestingValuesOnChange";

export const TestingValuesMenu: React.FC = () => {
  const { setValue } = useFormContext();
  const mode = useBuilderWatch("mode");
  const {
    jsonManifest: { spec },
  } = useConnectorBuilderFormState();
  const permission = useConnectorBuilderPermission();
  const { isTestingValuesInputOpen: isOpen, setTestingValuesInputOpen: setIsOpen } =
    useConnectorBuilderFormManagementState();
  const {
    streamRead: { isFetching },
  } = useConnectorBuilderTestRead();

  const testingValuesErrors = useTestingValuesErrors();
  const [showInputsWarning, setShowInputsWarning] = useLocalStorage("connectorBuilderInputsWarning", true);

  const closeAndSwitchToYaml = () => {
    setValue("mode", "yaml");
    setIsOpen(false);
  };

  return (
    <>
      <Tooltip
        control={
          <Button
            type="button"
            size="sm"
            variant="secondary"
            data-testid="test-inputs"
            onClick={() => setIsOpen(true)}
            disabled={isFetching || !spec || Object.keys(spec.connection_specification?.properties || {}).length === 0}
            icon="user"
            iconClassName={styles.icon}
            className={styles.button}
          >
            <FormattedMessage
              id="connectorBuilder.userInputs"
              values={{ number: Object.keys(spec?.connection_specification?.properties ?? {}).length }}
            />
            {testingValuesErrors > 0 && (
              <NumberBadge className={styles.inputsErrorBadge} value={testingValuesErrors} color="red" />
            )}
          </Button>
        }
        placement="bottom"
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
                <TestingValuesForm spec={spec} />
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
}

const TestingValuesForm: React.FC<TestingValuesFormProps> = ({ spec }) => {
  const { setTestingValuesInputOpen: setIsOpen } = useConnectorBuilderFormManagementState();
  const canEditConnector = useGeneratedIntent(Intent.CreateOrEditConnector);
  const testingValues = useBuilderWatch("testingValues");
  const [connectorFormValues, setConnectorFormValues] = useState(testingValues);
  const [connectorFormKey, setConnectorFormKey] = useState(0);
  const [formWasReset, setFormWasReset] = useState(false);
  const { setValue } = useFormContext();
  const resetConnectorFormValues = useCallback((values?: ConnectorBuilderProjectTestingValues) => {
    setFormWasReset(true);
    setConnectorFormValues(values);
  }, []);
  const incrementConnectorFormKey = useCallback(() => setConnectorFormKey((prev) => prev + 1), []);
  const { currentProject } = useConnectorBuilderFormState();

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
      selectedConnectorDefinition={{
        name: currentProject.name,
        dockerImageTag: "none",
        dockerRepository: "none",
        sourceDefinitionId: "none",
      }}
      selectedConnectorDefinitionSpecification={connectorDefinitionSpecification}
      formValues={{ connectionConfiguration: connectorFormValues }}
      onSubmit={async (values) => {
        setValue("testingValues", values.connectionConfiguration);
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

export function useTestingValuesErrors(): number {
  const { formatMessage } = useIntl();
  const testingValues = useBuilderWatch("testingValues");
  const {
    jsonManifest: { spec },
  } = useConnectorBuilderFormState();

  return useMemo(() => {
    try {
      const jsonSchema = spec && spec.connection_specification ? spec.connection_specification : {};
      const formFields = jsonSchemaToFormBlock(jsonSchema);
      const validationSchema = buildYupFormForJsonSchema(jsonSchema, formFields, formatMessage);
      validationSchema.validateSync(testingValues, { abortEarly: false });
      return 0;
    } catch (e) {
      if (ValidationError.isError(e)) {
        return e.errors.length;
      }
      return 1;
    }
  }, [spec, formatMessage, testingValues]);
}
