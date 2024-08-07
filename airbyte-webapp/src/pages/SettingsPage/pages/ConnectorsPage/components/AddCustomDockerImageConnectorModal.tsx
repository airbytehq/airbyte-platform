import React, { useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import * as yup from "yup";

import { Form, FormControl } from "components/forms";
import { ModalFormSubmissionButtons } from "components/forms/ModalFormSubmissionButtons";
import { Box } from "components/ui/Box";
import { ExternalLink } from "components/ui/Link";
import { Message } from "components/ui/Message";
import { ModalBody, ModalFooter } from "components/ui/Modal";
import { Text } from "components/ui/Text";

import { useFormatError } from "core/errors";
import { isCloudApp } from "core/utils/app";
import { links } from "core/utils/links";

interface ConnectorDefinition {
  name: string;
  documentationUrl: string;
  dockerImageTag: string;
  dockerRepository: string;
}

export interface AddCustomDockerImageConnectorModalProps {
  onCancel: () => void;
  onSubmit: (sourceDefinition: ConnectorDefinition) => Promise<void>;
}

const validationSchema = yup.object().shape({
  name: yup.string().trim().required("form.empty.error"),
  documentationUrl: yup.string().trim().url("form.url.error").notRequired().default(""),
  dockerImageTag: yup.string().trim().required("form.empty.error"),
  dockerRepository: yup.string().trim().required("form.empty.error"),
});

const ConnectorControl = FormControl<ConnectorDefinition>;

export const AddCustomDockerImageConnectorModal: React.FC<AddCustomDockerImageConnectorModalProps> = ({
  onCancel,
  onSubmit,
}) => {
  const { formatMessage } = useIntl();
  const [error, setError] = useState<Error>();
  const formatError = useFormatError();

  return (
    <Form<ConnectorDefinition>
      defaultValues={{
        name: "",
        documentationUrl: "",
        dockerImageTag: "",
        dockerRepository: "",
      }}
      schema={validationSchema}
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
            label={formatMessage({ id: isCloudApp() ? "admin.dockerFullImageName" : "admin.dockerRepository" })}
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
