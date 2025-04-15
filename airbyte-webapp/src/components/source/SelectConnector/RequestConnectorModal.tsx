import React from "react";
import { useWatch } from "react-hook-form";
import { useIntl } from "react-intl";
import { z } from "zod";

import { Form, FormControl } from "components/forms";
import { ModalFormSubmissionButtons } from "components/forms/ModalFormSubmissionButtons";
import { ModalBody, ModalFooter } from "components/ui/Modal";

import { ConnectorType } from "core/api/types/AirbyteClient";
import { useNotificationService } from "hooks/services/Notification";
import useRequestConnector from "hooks/services/useRequestConnector";

const validationSchema = z.object({
  connectorType: z.nativeEnum(ConnectorType),
  name: z.string().trim().nonempty("form.empty.error"),
  additionalInfo: z.string().optional(),
  email: z.string().email("form.empty.error"),
});

type ConnectorRequest = z.infer<typeof validationSchema>;

const RequestControl = FormControl<ConnectorRequest>;

interface RequestConnectorModalProps {
  onSubmit: () => void;
  onCancel: () => void;
  connectorType: ConnectorRequest["connectorType"];
  workspaceEmail?: string;
  searchedConnectorName?: string;
}

export const RequestConnectorModal: React.FC<RequestConnectorModalProps> = ({
  onSubmit,
  onCancel,
  connectorType,
  searchedConnectorName,
  workspaceEmail,
}) => {
  const { formatMessage } = useIntl();
  const notificationService = useNotificationService();
  const { requestConnector } = useRequestConnector();

  const onSubmitBtnClick = async (values: ConnectorRequest) => {
    requestConnector(values);
    notificationService.registerNotification({
      id: "connector.requestConnector.success",
      text: formatMessage({ id: "connector.request.success" }),
      type: "success",
    });
    onSubmit();
  };

  return (
    <Form<ConnectorRequest>
      defaultValues={{
        connectorType,
        name: searchedConnectorName ?? "",
        additionalInfo: "",
        email: workspaceEmail ?? "",
      }}
      zodSchema={validationSchema}
      onSubmit={onSubmitBtnClick}
      trackDirtyChanges
    >
      <ModalBody>
        <RequestControl
          fieldType="dropdown"
          name="connectorType"
          label={formatMessage({ id: "connector.type" })}
          options={[
            { value: "source", label: formatMessage({ id: "connector.source" }) },
            { value: "destination", label: formatMessage({ id: "connector.destination" }) },
          ]}
        />
        <NameControl />
        <RequestControl
          fieldType="textarea"
          name="additionalInfo"
          label={formatMessage({ id: "connector.additionalInfo.label" })}
          description={formatMessage({ id: "connector.additionalInfo.description" })}
          optional
        />
        {!workspaceEmail && (
          <RequestControl fieldType="input" name="email" label={formatMessage({ id: "connector.email" })} />
        )}
      </ModalBody>

      <ModalFooter>
        <ModalFormSubmissionButtons
          submitKey="connector.request"
          cancelKey="form.cancel"
          onCancelClickCallback={onCancel}
        />
      </ModalFooter>
    </Form>
  );
};

const NameControl = () => {
  const { formatMessage } = useIntl();
  const connectorType = useWatch({ name: "connectorType" });

  return (
    <RequestControl
      fieldType="input"
      name="name"
      label={
        connectorType === "destination"
          ? formatMessage({ id: "connector.requestConnector.destination.name" })
          : formatMessage({ id: "connector.requestConnector.source.name" })
      }
    />
  );
};
