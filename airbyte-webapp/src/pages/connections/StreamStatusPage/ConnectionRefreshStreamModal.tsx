import { Controller, useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";
import * as yup from "yup";

import { RadioButtonTiles } from "components/connection/CreateConnection/RadioButtonTiles";
import { Form } from "components/forms";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { Text } from "components/ui/Text";

import { FeatureItem, useFeature } from "core/services/features";
import { useZendesk } from "packages/cloud/services/thirdParty/zendesk";

interface ConnectionRefreshStreamModalProps {
  onComplete: () => void;
  onCancel: () => void;
  canMerge: boolean;
  canTruncate: boolean;
  streamNamespace?: string;
  streamName: string;
  refreshStreams: (streams: Array<{ streamName: string; streamNamespace?: string }>) => Promise<void>;
}

export interface ConnectionRefreshStreamFormValues {
  refreshType: "merge" | "truncate";
  streamName: string;
  streamNamespace?: string;
}

const MergeTruncateRadioButtons: React.FC = () => {
  const { setValue, control } = useFormContext<ConnectionRefreshStreamFormValues>();

  return (
    <div>
      <Controller
        control={control}
        name="refreshType"
        render={({ field }) => {
          return (
            <RadioButtonTiles
              light
              direction="column"
              options={[
                {
                  value: "merge",
                  label: <FormattedMessage id="connection.stream.actions.refreshStream.merge.label" />,
                  description: (
                    <Text color="grey400" size="sm" as="span">
                      <FormattedMessage
                        id="connection.stream.actions.refreshStream.merge.description"
                        values={{
                          bold: (children) => (
                            <Text as="span" bold color="grey400" size="sm">
                              {children}
                            </Text>
                          ),
                        }}
                      />
                    </Text>
                  ),
                },
                {
                  value: "truncate",
                  label: <FormattedMessage id="connection.stream.actions.refreshStream.truncate.label" />,
                  description: (
                    <Text color="grey400" size="sm">
                      <FormattedMessage
                        id="connection.stream.actions.refreshStream.truncate.description"
                        values={{
                          bold: (children) => (
                            <Text as="span" bold color="grey400" size="sm">
                              {children}
                            </Text>
                          ),
                        }}
                      />
                    </Text>
                  ),
                },
              ]}
              selectedValue={field.value ?? ""}
              onSelectRadioButton={(value) => {
                setValue("refreshType", value, { shouldDirty: true });
              }}
              name="refreshType"
            />
          );
        }}
      />
    </div>
  );
};

export const ConnectionRefreshStreamModal: React.FC<ConnectionRefreshStreamModalProps> = ({
  canTruncate,
  canMerge,
  refreshStreams,
  streamName,
  streamNamespace,
  onComplete,
  onCancel,
}) => {
  const { openZendesk } = useZendesk();
  const allowSupportChat = useFeature(FeatureItem.AllowInAppSupportChat);

  const onSubmitRefreshStreamForm = async (values: ConnectionRefreshStreamFormValues) => {
    await refreshStreams([{ streamName: values.streamName, streamNamespace: values.streamNamespace }]);
    onComplete();
  };

  const refreshConnectionFormSchema = yup.object().shape({
    refreshType: yup.mixed<ConnectionRefreshStreamFormValues["refreshType"]>().oneOf(["merge", "truncate"]).required(),
    streamNamespace: yup.string().trim(),
    streamName: yup.string().trim().required(),
  });

  return (
    <>
      <Box p="xl">
        <Text>
          <FormattedMessage
            id="connection.stream.actions.refreshStream.description"
            values={{
              Bold: (children) => (
                <Text as="span" bold>
                  {children}
                </Text>
              ),
            }}
          />
          {canMerge && canTruncate && (
            <Text as="span">
              <FormattedMessage id="connection.stream.actions.refreshStream.options" />
            </Text>
          )}
        </Text>
        {allowSupportChat && (
          <Box pt="xs">
            <Text>
              <FormattedMessage
                id="connection.stream.actions.refreshStream.chatWithUs"
                values={{
                  ChatWithUsLink: (children) => (
                    <Button variant="link" onClick={openZendesk}>
                      {children}
                    </Button>
                  ),
                }}
              />
            </Text>
          </Box>
        )}
      </Box>
      <Form<ConnectionRefreshStreamFormValues>
        schema={refreshConnectionFormSchema}
        onSubmit={async (values) => {
          await onSubmitRefreshStreamForm(values);
        }}
        defaultValues={{
          streamName,
          streamNamespace,
          refreshType: canMerge ? "merge" : "truncate",
        }}
      >
        {canMerge ? (
          canTruncate ? (
            <MergeTruncateRadioButtons />
          ) : (
            <Box px="xl" pb="md">
              <Text color="grey400">
                <FormattedMessage
                  id="connection.stream.actions.refreshStream.merge.description"
                  values={{
                    bold: (children) => (
                      <Text as="span" bold color="grey400" size="sm">
                        {children}
                      </Text>
                    ),
                  }}
                />
              </Text>
            </Box>
          )
        ) : (
          <Box px="xl" pb="md">
            <Text color="grey400">
              <FormattedMessage
                id="connection.stream.actions.refreshStream.truncate.description"
                values={{
                  bold: (children) => (
                    <Text as="span" bold color="grey400" size="sm">
                      {children}
                    </Text>
                  ),
                }}
              />
            </Text>
          </Box>
        )}
        <Box p="lg">
          <FormSubmissionButtons
            submitKey="connection.stream.actions.refreshStream.confirm.submit"
            onCancelClickCallback={onCancel}
            allowNonDirtyCancel
            allowNonDirtySubmit
          />
        </Box>
      </Form>
    </>
  );
};
