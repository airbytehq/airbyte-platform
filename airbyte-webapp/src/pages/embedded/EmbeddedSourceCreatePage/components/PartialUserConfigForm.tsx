import { FlexContainer } from "components/ui/Flex";

import { SourceDefinitionRead } from "core/api/types/AirbyteClient";
import { SourceDefinitionSpecificationDraft } from "core/domain/connector";
import { ConnectorForm, ConnectorFormValues } from "views/Connector/ConnectorForm";

import styles from "./PartialUserConfigForm.module.scss";
import { PartialUserConfigFormControls } from "./PartialUserConfigFormControls";
import { PartialUserConfigHeader } from "./PartialUserConfigHeader";

interface PartialUserConfigFormProps {
  connectorName: string;
  icon: string;
  onSubmit: (values: ConnectorFormValues) => void;
  initialValues?: Partial<ConnectorFormValues>;
  sourceDefinitionSpecification: SourceDefinitionSpecificationDraft;
  isEditMode: boolean;
  onDelete?: () => void;
}

export const PartialUserConfigForm: React.FC<PartialUserConfigFormProps> = ({
  connectorName,
  icon,
  onSubmit,
  initialValues,
  sourceDefinitionSpecification,
  isEditMode,
  onDelete,
}) => {
  // The ConnectorForm needs a connector definition to get the connector name. We construct a dummy one here, since
  // Embedded users do not have access to APIs that return actual SourceDefinitionRead objects
  const sourceDefinition: SourceDefinitionRead = {
    sourceDefinitionId: "not-set-for-embedded",
    name: connectorName,
    dockerRepository: "not-set-for-embedded",
    dockerImageTag: "not-set-for-embedded",
  };

  return (
    <FlexContainer className={styles.content} direction="column">
      <PartialUserConfigHeader icon={icon} connectorName={connectorName} />
      <ConnectorForm
        trackDirtyChanges
        isEditMode={isEditMode}
        formType="source"
        formValues={initialValues}
        selectedConnectorDefinition={sourceDefinition}
        selectedConnectorDefinitionSpecification={sourceDefinitionSpecification}
        onSubmit={async (values: ConnectorFormValues) => {
          await Promise.resolve(onSubmit(values));
        }}
        canEdit
        renderFooter={({ isSubmitting }) => (
          <PartialUserConfigFormControls onDelete={onDelete} isSubmitting={isSubmitting} />
        )}
      />
    </FlexContainer>
  );
};
