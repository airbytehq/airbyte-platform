import { FormattedMessage, useIntl } from "react-intl";
import { z } from "zod";

import { Form, FormControl } from "components/forms";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";
import { Box } from "components/ui/Box";
import { Message } from "components/ui/Message";

import { ConnectorDefinition } from "core/domain/connector";

const editDefinitionFormSchema = z.object({
  name: z.string().trim().nonempty("form.empty.error"),
  dockerRepository: z.string().trim().nonempty("form.empty.error"),
  dockerImageTag: z.string().trim().nonempty("form.empty.error"),
});

export type EditDefinitionFormValues = z.infer<typeof editDefinitionFormSchema>;

interface EditConnectorDefinitionModalContentProps<T extends ConnectorDefinition> {
  definition: T;
  isConnectorNameEditable?: boolean;
  latestVersion?: string;
  onUpdateDefinition: (values: EditDefinitionFormValues) => Promise<void>;
}

// Can be used to edit either a source or destination definition, depending on the definition passed in
export const EditConnectorDefinitionModalContent = <T extends ConnectorDefinition>({
  definition,
  isConnectorNameEditable = true,
  latestVersion,
  onUpdateDefinition,
}: EditConnectorDefinitionModalContentProps<T>) => {
  const { formatMessage } = useIntl();

  const updateAvailable = !definition.custom && latestVersion !== definition?.dockerImageTag;

  return (
    <Box p="xl">
      <Form<EditDefinitionFormValues>
        zodSchema={editDefinitionFormSchema}
        defaultValues={{
          name: definition.name,
          dockerRepository: definition.dockerRepository,
          dockerImageTag: definition.dockerImageTag,
        }}
        onSubmit={onUpdateDefinition}
      >
        <FormControl
          label={formatMessage({ id: "settings.connector.editDefinition.displayName" })}
          name="name"
          fieldType="input"
          disabled={!isConnectorNameEditable}
        />
        <FormControl
          label={formatMessage({ id: "settings.connector.editDefinition.dockerImage" })}
          name="dockerRepository"
          fieldType="input"
          disabled
        />
        <FormControl
          label={formatMessage({ id: "settings.connector.editDefinition.dockerImageTag" })}
          name="dockerImageTag"
          fieldType="input"
        />
        {updateAvailable && (
          <Box mb="xl">
            <Message
              text={
                <FormattedMessage
                  id="settings.connector.editDefinition.updateAvailable"
                  values={{ connectorName: definition.name, latestVersion }}
                />
              }
            />
          </Box>
        )}
        <FormSubmissionButtons />
      </Form>
    </Box>
  );
};
