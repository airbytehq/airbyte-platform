import React, { useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { z } from "zod";

import { Form, FormControl } from "components/forms";
import { ModalFormSubmissionButtons } from "components/forms/ModalFormSubmissionButtons";
import { Box } from "components/ui/Box";
import { ExternalLink } from "components/ui/Link";
import { Message } from "components/ui/Message";
import { ModalBody, ModalFooter } from "components/ui/Modal";
import { Text } from "components/ui/Text";

import { useFormatError } from "core/errors";
import { useIsCloudApp } from "core/utils/app";
import { links } from "core/utils/links";

const validationSchema = z.object({
  name: z.string().trim().nonempty("form.empty.error"),
  documentationUrl: z.string().url("form.url.error").or(z.literal("")),
  dockerImageTag: z.string().trim().nonempty("form.empty.error"),
  dockerRepository: z.string().trim().nonempty("form.empty.error"),
});

type ConnectorDefinition = z.infer<typeof validationSchema>;

export interface AddCustomDockerImageConnectorModalProps {
  onCancel: () => void;
  onSubmit: (sourceDefinition: ConnectorDefinition) => Promise<void>;
}

const ConnectorControl = FormControl<ConnectorDefinition>;

export const AddCustomDockerImageConnectorModal: React.FC<AddCustomDockerImageConnectorModalProps> = ({
  onCancel,
  onSubmit,
}) => {
  const { formatMessage } = useIntl();
  const [error, setError] = useState<Error>();
  const formatError = useFormatError();
  const isCloudApp = useIsCloudApp();
  return (
    <Form<ConnectorDefinition>
      defaultValues={{
        name: "",
        documentationUrl: "",
        dockerImageTag: "",
        dockerRepository: "",
      }}
      zodSchema={validationSchema}
      onSubmit={async (values) => {
        setError(undefined);
        await onSubmit(values);
      }}
      onError={(e) => {
        setError(e);
      }}
    >
      <ModalBody>
        <Text>
          <FormattedMessage
            id="admin.learnMore"
            values={{
              lnk: (lnk: React.ReactNode) => <ExternalLink href={links.usingCustomConnectors}>{lnk}</ExternalLink>,
            }}
          />
        </Text>
        <Box mt="xl">
          <ConnectorControl fieldType="input" name="name" label={formatMessage({ id: "admin.connectorName" })} />
          <ConnectorControl
            fieldType="input"
            name="dockerRepository"
            label={formatMessage({ id: isCloudApp ? "admin.dockerFullImageName" : "admin.dockerRepository" })}
          />
          <ConnectorControl
            fieldType="input"
            name="dockerImageTag"
            label={formatMessage({ id: "admin.dockerImageTag" })}
          />
          <ConnectorControl
            fieldType="input"
            name="documentationUrl"
            label={formatMessage({ id: "admin.documentationUrl" })}
            optional
          />
          {error && <Message type="error" text={formatError(error)} />}
        </Box>
      </ModalBody>
      <ModalFooter>
        <ModalFormSubmissionButtons submitKey="form.add" cancelKey="form.cancel" onCancelClickCallback={onCancel} />
      </ModalFooter>
    </Form>
  );
};
