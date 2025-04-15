import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { Heading } from "components/ui/Heading";

import { SourceDefinitionSpecificationDraft } from "core/domain/connector";
import { ConnectorForm, ConnectorFormValues } from "views/Connector/ConnectorForm";

import { PartialUserConfigFormControls } from "./PartialUserConfigFormControls";

interface PartialUserConfigFormProps {
  title: string;
  onBack: () => void;
  onSubmit: (values: ConnectorFormValues) => void;
  initialValues?: Partial<ConnectorFormValues>;
  sourceDefinitionSpecification: SourceDefinitionSpecificationDraft;
  isEditMode: boolean;
}

export const PartialUserConfigForm: React.FC<PartialUserConfigFormProps> = ({
  title,
  onBack,
  onSubmit,
  initialValues,
  sourceDefinitionSpecification,
  isEditMode,
}) => {
  return (
    <>
      <Heading as="h1" size="sm">
        {title}
      </Heading>
      <Box py="sm">
        <Button variant="light" onClick={onBack}>
          <FormattedMessage id="partialUserConfig.back" />
        </Button>
      </Box>
      <div>
        <ConnectorForm
          trackDirtyChanges
          formType="source"
          formValues={initialValues}
          selectedConnectorDefinitionSpecification={sourceDefinitionSpecification}
          onSubmit={async (values: ConnectorFormValues) => {
            onSubmit(values);
          }}
          canEdit
          renderFooter={({ dirty, isSubmitting, isValid, resetConnectorForm }) => (
            <PartialUserConfigFormControls
              isEditMode={isEditMode}
              isSubmitting={isSubmitting}
              isValid={isValid}
              dirty={dirty}
              onCancel={resetConnectorForm}
            />
          )}
        />
      </div>
    </>
  );
};
