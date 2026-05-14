import React, { useCallback, useMemo } from "react";
import { useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import { z } from "zod";

import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Form, FormControl } from "components/ui/forms";
import { FormSubmissionButtons } from "components/ui/forms/FormSubmissionButtons";
import { Heading } from "components/ui/Heading";
import { ExternalLink } from "components/ui/Link";
import { ButtonTab, Tabs } from "components/ui/Tabs";
import { Text } from "components/ui/Text";

import { HttpError, useCreatePrivateLink, useDeletePrivateLink, useListPrivateLinks } from "core/api";
import { PrivateLinkRead } from "core/api/types/AirbyteClient";
import { useFormatError } from "core/errors";
import { useConfirmationModalService } from "core/services/ConfirmationModal";
import { useModalService } from "core/services/Modal";
import { useNotificationService } from "core/services/Notification";
import { trackError } from "core/utils/datadog";
import { links } from "core/utils/links";

import { PrivateLinkDetailModal } from "./PrivateLinkDetailModal";
import { PrivateLinksTable } from "./PrivateLinksTable";
import { ALL_VARIANTS, endpointAwsVariant, storageAwsVariant } from "./variants";
import { BasePrivateLinkSchema, BASE_DEFAULTS, PrivateLinkVariant } from "./variants/types";

const PrivateLinkFormSchema = z.discriminatedUnion("variantId", [
  endpointAwsVariant.schema.merge(BasePrivateLinkSchema),
  storageAwsVariant.schema.merge(BasePrivateLinkSchema),
]);

type PrivateLinkFormValues = z.infer<typeof PrivateLinkFormSchema>;

const findVariant = (variantId: string): PrivateLinkVariant => {
  const v = ALL_VARIANTS.find((entry) => entry.variantId === variantId);
  if (!v) {
    throw new Error(`No PrivateLink variant registered for id "${variantId}"`);
  }
  return v;
};

const VariantFields: React.FC = () => {
  const { control } = useFormContext<PrivateLinkFormValues>();
  const variantId = useWatch({ control, name: "variantId" });
  const VariantComponent = findVariant(variantId).Fields;
  return <VariantComponent />;
};

const VariantTabs: React.FC = () => {
  const { formatMessage } = useIntl();
  const { control, getValues, reset } = useFormContext<PrivateLinkFormValues>();
  const variantId = useWatch({ control, name: "variantId" });

  const onSelect = (newId: string) => {
    if (newId === variantId) {
      return;
    }
    const variant = findVariant(newId);
    // Swap the variant slice while preserving fields owned by the base schema.
    reset({
      ...BASE_DEFAULTS,
      privateLinkName: getValues("privateLinkName"),
      ...variant.defaultValues,
    } as PrivateLinkFormValues);
  };

  return (
    <Tabs>
      {ALL_VARIANTS.map((v) => (
        <ButtonTab
          key={v.variantId}
          id={v.variantId}
          name={formatMessage({ id: v.labelKey })}
          isActive={v.variantId === variantId}
          onSelect={onSelect}
        />
      ))}
    </Tabs>
  );
};

export const PrivateLinksSettingsPage: React.FC = () => {
  const { formatMessage } = useIntl();
  const formatError = useFormatError();
  const { registerNotification } = useNotificationService();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const { openModal } = useModalService();
  const { mutateAsync: createPrivateLink } = useCreatePrivateLink();
  const { mutateAsync: deletePrivateLink } = useDeletePrivateLink();
  const { privateLinks } = useListPrivateLinks();

  const defaultValues = useMemo<PrivateLinkFormValues>(
    () => ({ ...BASE_DEFAULTS, ...ALL_VARIANTS[0].defaultValues }) as PrivateLinkFormValues,
    []
  );

  const onSubmit = async (values: PrivateLinkFormValues) => {
    const existingNames = new Set(privateLinks.map((l) => l.name));
    if (existingNames.has(values.privateLinkName)) {
      throw new Error(formatMessage({ id: "settings.privateLinks.form.name.duplicate" }));
    }
    const variant = findVariant(values.variantId);
    const body = variant.toCreateRequest(values);
    await createPrivateLink(body);
    // Stay on the variant the user was using; reset only the fields back to that variant's defaults.
    return {
      resetValues: { ...BASE_DEFAULTS, ...variant.defaultValues } as PrivateLinkFormValues,
    };
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
      <Text>
        <FormattedMessage
          id="settings.privateLinks.description"
          values={{
            lnk: (node: React.ReactNode) => <ExternalLink href={links.privateLinkDocs}>{node}</ExternalLink>,
          }}
        />
      </Text>

      <Card>
        <Form<PrivateLinkFormValues>
          defaultValues={defaultValues}
          onSubmit={onSubmit}
          onError={onError}
          zodSchema={PrivateLinkFormSchema}
        >
          <Box mb="xl">
            <VariantTabs />
          </Box>
          <FormControl
            name="privateLinkName"
            fieldType="input"
            label={formatMessage({ id: "settings.privateLinks.form.name" })}
            placeholder={formatMessage({ id: "settings.privateLinks.form.name.placeholder" })}
          />
          <VariantFields />
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
