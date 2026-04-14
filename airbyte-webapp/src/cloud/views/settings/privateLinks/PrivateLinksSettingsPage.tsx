import React, { useCallback } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { z } from "zod";

import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Form, FormControl } from "components/ui/forms";
import { FormSubmissionButtons } from "components/ui/forms/FormSubmissionButtons";
import { Heading } from "components/ui/Heading";
import { Text } from "components/ui/Text";

import { HttpError, useCreatePrivateLink, useDeletePrivateLink, useListPrivateLinks } from "core/api";
import { PrivateLinkRead } from "core/api/types/AirbyteClient";
import { useFormatError } from "core/errors";
import { useConfirmationModalService } from "core/services/ConfirmationModal";
import { useModalService } from "core/services/Modal";
import { useNotificationService } from "core/services/Notification";
import { trackError } from "core/utils/datadog";

import { PrivateLinkDetailModal } from "./PrivateLinkDetailModal";
import { PrivateLinksTable } from "./PrivateLinksTable";

const AWS_SERVICE_NAME_REGEX = /^com\.amazonaws\.vpce\.([a-z0-9-]+)\.vpce-svc-[a-z0-9]+$/;

const PRIVATE_LINK_NAME_REGEX = /^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$/;

const PrivateLinkFormSchema = z.object({
  privateLinkName: z
    .string()
    .trim()
    .nonempty("form.empty.error")
    .regex(PRIVATE_LINK_NAME_REGEX, "settings.privateLinks.form.name.invalid"),
  serviceName: z
    .string()
    .trim()
    .nonempty("form.empty.error")
    .regex(AWS_SERVICE_NAME_REGEX, "settings.privateLinks.form.serviceName.invalid"),
});

type PrivateLinkFormValues = z.infer<typeof PrivateLinkFormSchema>;

export const PrivateLinksSettingsPage: React.FC = () => {
  const { formatMessage } = useIntl();
  const formatError = useFormatError();
  const { registerNotification } = useNotificationService();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const { openModal } = useModalService();
  const { mutateAsync: createPrivateLink } = useCreatePrivateLink();
  const { mutateAsync: deletePrivateLink } = useDeletePrivateLink();
  const { privateLinks } = useListPrivateLinks();

  const onSubmit = async (values: PrivateLinkFormValues) => {
    const existingNames = new Set(privateLinks.map((l) => l.name));
    if (existingNames.has(values.privateLinkName)) {
      throw new Error(formatMessage({ id: "settings.privateLinks.form.name.duplicate" }));
    }

    // Safe to assert — zod already validated the regex
    const serviceRegion = values.serviceName.match(AWS_SERVICE_NAME_REGEX)![1];
    await createPrivateLink({
      name: values.privateLinkName,
      serviceRegion,
      serviceName: values.serviceName,
    });
    return { resetValues: { privateLinkName: "", serviceName: "" } };
  };

  const onError = (e: Error) => {
    if (e instanceof HttpError) {
      trackError(e);
    }
    const backendMessage =
      e instanceof HttpError && typeof e.response === "object" && e.response && "message" in e.response
        ? String((e.response as Record<string, unknown>).message)
        : undefined;
    registerNotification({
      id: "privateLinks/create-failure",
      text: backendMessage ?? formatError(e) ?? e.message,
      type: "error",
    });
  };

  const onDelete = useCallback(
    (link: PrivateLinkRead) => {
      openConfirmationModal({
        title: "settings.privateLinks.deleteModal.title",
        text: "settings.privateLinks.deleteModal.text",
        textValues: { name: link.name },
        submitButtonText: "settings.privateLinks.deleteModal.submit",
        onSubmit: async () => {
          await deletePrivateLink(link.id);
          closeConfirmationModal();
        },
      });
    },
    [closeConfirmationModal, deletePrivateLink, openConfirmationModal]
  );

  const onViewDetails = useCallback(
    (link: PrivateLinkRead) => {
      openModal({
        title: formatMessage({ id: "settings.privateLinks.details.title" }),
        size: "lg",
        content: () => <PrivateLinkDetailModal link={link} />,
      });
    },
    [formatMessage, openModal]
  );

  return (
    <FlexContainer direction="column" gap="xl">
      <Heading as="h1">{formatMessage({ id: "settings.privateLinks.title" })}</Heading>
      <Text>{formatMessage({ id: "settings.privateLinks.description" })}</Text>

      <Card>
        <Form<PrivateLinkFormValues>
          defaultValues={{ privateLinkName: "", serviceName: "" }}
          onSubmit={onSubmit}
          onError={onError}
          zodSchema={PrivateLinkFormSchema}
        >
          <FormControl
            name="privateLinkName"
            fieldType="input"
            label={formatMessage({ id: "settings.privateLinks.form.name" })}
            placeholder={formatMessage({ id: "settings.privateLinks.form.name.placeholder" })}
          />
          <FormControl
            name="serviceName"
            fieldType="input"
            label={formatMessage({ id: "settings.privateLinks.form.serviceName" })}
            placeholder={formatMessage({ id: "settings.privateLinks.form.serviceName.placeholder" })}
          />
          <FormSubmissionButtons
            noCancel
            justify="flex-start"
            submitKey="settings.privateLinks.form.submit"
            allowNonDirtySubmit
          />
        </Form>
      </Card>

      {privateLinks.length > 0 ? (
        <PrivateLinksTable privateLinks={privateLinks} onDelete={onDelete} onViewDetails={onViewDetails} />
      ) : (
        <Text color="grey">
          <FormattedMessage id="settings.privateLinks.empty" />
        </Text>
      )}
    </FlexContainer>
  );
};
