import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { SvgIcon } from "area/connector/utils";
import { SourceDefinitionRead } from "core/api/types/AirbyteClient";
import { SourceDefinitionSpecificationDraft } from "core/domain/connector";
import { ConnectorForm, ConnectorFormValues } from "views/Connector/ConnectorForm";

import styles from "./PartialUserConfigForm.module.scss";
import { PartialUserConfigFormControls } from "./PartialUserConfigFormControls";
import { useEmbeddedSourceParams } from "../hooks/useEmbeddedSourceParams";

interface PartialUserConfigFormProps {
  connectorName: string;
  icon: string;
  onSubmit: (values: ConnectorFormValues) => void;
  initialValues?: Partial<ConnectorFormValues>;
  sourceDefinitionSpecification: SourceDefinitionSpecificationDraft;
  isEditMode: boolean;
  showSuccessView: boolean;
}

export const PartialUserConfigForm: React.FC<PartialUserConfigFormProps> = ({
  connectorName,
  icon,
  onSubmit,
  initialValues,
  sourceDefinitionSpecification,
  isEditMode,
  showSuccessView,
}) => {
  const { clearSelectedConfig, clearSelectedTemplate } = useEmbeddedSourceParams();

  // The ConnectorForm needs a connector definition to get the connector name. We construct a dummy one here, since
  // Embedded users do not have access to APIs that return actual SourceDefinitionRead objects
  const sourceDefinition: SourceDefinitionRead = {
    sourceDefinitionId: "not-set-for-embedded",
    name: connectorName,
    dockerRepository: "not-set-for-embedded",
    dockerImageTag: "not-set-for-embedded",
  };

  const onClick = () => {
    clearSelectedConfig();
    clearSelectedTemplate();
  };
  return (
    <>
      {!showSuccessView ? (
        <FlexContainer className={styles.content} direction="column">
          <FlexContainer alignItems="center" gap="sm" justifyContent="center">
            <FlexContainer className={styles.iconContainer} aria-hidden="true" alignItems="center">
              <SvgIcon src={icon} />
            </FlexContainer>
            <p>{connectorName}</p>
          </FlexContainer>

          <ConnectorForm
            trackDirtyChanges
            isEditMode={isEditMode}
            formType="source"
            formValues={initialValues}
            selectedConnectorDefinition={sourceDefinition}
            selectedConnectorDefinitionSpecification={sourceDefinitionSpecification}
            onSubmit={async (values: ConnectorFormValues) => {
              onSubmit(values);
            }}
            canEdit
            renderFooter={({ dirty, isSubmitting }) => (
              <PartialUserConfigFormControls isEditMode={isEditMode} isSubmitting={isSubmitting} dirty={dirty} />
            )}
          />
        </FlexContainer>
      ) : (
        <FlexContainer className={styles.content} direction="column" justifyContent="space-between">
          <FlexContainer alignItems="center" gap="sm" justifyContent="center">
            <FlexContainer className={styles.iconContainer} aria-hidden="true" alignItems="center">
              <SvgIcon src={icon} />
            </FlexContainer>
            <p>{connectorName}</p>
          </FlexContainer>
          <FlexContainer direction="column" gap="lg" justifyContent="center" alignItems="center">
            <Icon size="xl" type="checkCircle" color="disabled" />
            <Text size="lg">
              <FormattedMessage id="partialUserConfig.success.title" />
            </Text>
            <Text size="md">
              <FormattedMessage id="partialUserConfig.success.description" />
            </Text>
          </FlexContainer>
          <div className={styles.buttonContainer}>
            <Button full onClick={onClick}>
              <FormattedMessage id="partialUserConfig.backToIntegrations" />
            </Button>
          </div>
        </FlexContainer>
      )}
    </>
  );
};
