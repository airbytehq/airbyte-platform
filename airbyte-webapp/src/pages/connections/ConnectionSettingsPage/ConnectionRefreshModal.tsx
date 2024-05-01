import { Controller, useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";
import * as yup from "yup";

import { RadioButtonTiles } from "components/connection/CreateConnection/RadioButtonTiles";
import { Form } from "components/forms";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { ConnectionStream } from "core/api/types/AirbyteClient";
import { FeatureItem, useFeature } from "core/services/features";
import { useZendesk } from "packages/cloud/services/thirdParty/zendesk";

import { StreamsRefreshListBlock } from "./StreamsRefreshListBlock";

interface ConnectionRefreshModalProps {
  refreshScope: "connection" | "stream";
  onComplete: () => void;
  onCancel: () => void;
  streamsSupportingMergeRefresh: ConnectionStream[];
  streamsSupportingTruncateRefresh: ConnectionStream[];
  refreshStreams: (streams?: Array<{ streamName: string; streamNamespace?: string }>) => Promise<void>;
  totalEnabledStreams?: number;
}

export interface ConnectionRefreshFormValues {
  refreshType: "merge" | "truncate";
  streams?: ConnectionStream[];
}

const MergeTruncateRadioButtons: React.FC<{
  refreshScope: "connection" | "stream";
}> = ({ refreshScope }) => {
  const { setValue, control } = useFormContext<ConnectionRefreshFormValues>();

  return (
    <Box pt="sm">
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
                  label: (
                    <FormattedMessage
                      id="connection.actions.refreshStream.merge.label"
                      values={{ value: refreshScope === "connection" ? 2 : 1 }}
                    />
                  ),
                  description: (
                    <Text color="grey400" size="sm" as="span">
                      <FormattedMessage
                        id="connection.actions.refreshStream.merge.description"
                        values={{
                          value: refreshScope === "connection" ? 2 : 1,
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
                  label: (
                    <FormattedMessage
                      id="connection.actions.refreshStream.truncate.label"
                      values={{ value: refreshScope === "connection" ? 2 : 1 }}
                    />
                  ),
                  description: (
                    <Text color="grey400" size="sm">
                      <FormattedMessage
                        id="connection.actions.refreshStream.truncate.description"
                        values={{
                          value: refreshScope === "connection" ? 2 : 1,
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
    </Box>
  );
};

export const ConnectionRefreshModal: React.FC<ConnectionRefreshModalProps> = ({
  refreshScope,
  streamsSupportingMergeRefresh,
  streamsSupportingTruncateRefresh,
  totalEnabledStreams,
  onComplete,
  onCancel,
  refreshStreams,
}) => {
  const { openZendesk } = useZendesk();
  const allowSupportChat = useFeature(FeatureItem.AllowInAppSupportChat);
  const canMerge = streamsSupportingMergeRefresh.length > 0;
  const canTruncate = streamsSupportingTruncateRefresh.length > 0;

  const onSubmitRefreshStreamForm = async (values: ConnectionRefreshFormValues) => {
    await refreshStreams(
      values.refreshType === "merge" ? streamsSupportingMergeRefresh : streamsSupportingTruncateRefresh
    );
    onComplete();
  };

  const refreshConnectionFormSchema = yup.object().shape({
    refreshType: yup.mixed<ConnectionRefreshFormValues["refreshType"]>().oneOf(["merge", "truncate"]).required(),
    streams: yup.array().when("refreshScope", {
      is: "connection",
      then: yup.array().strip(),
      otherwise: yup.array().min(1),
    }),
  });

  return (
    <FlexContainer direction="column">
      <Box px="xl" pt="xl">
        <Text>
          <FormattedMessage
            id="connection.actions.refreshStream.description"
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
              <FormattedMessage id="connection.actions.refreshStream.considerOptions" />
            </Text>
          )}
        </Text>
        {allowSupportChat && (
          <Box pt="xs">
            <Text>
              <FormattedMessage
                id="connection.actions.refreshStream.chatWithUs"
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
      <Form<ConnectionRefreshFormValues>
        schema={refreshConnectionFormSchema}
        onSubmit={async (values) => {
          await onSubmitRefreshStreamForm(values);
        }}
        defaultValues={{
          refreshType: canMerge ? "merge" : "truncate",
          streams:
            refreshScope === "connection"
              ? undefined
              : canMerge
              ? streamsSupportingMergeRefresh
              : streamsSupportingTruncateRefresh,
        }}
      >
        {canMerge ? (
          canTruncate ? (
            <MergeTruncateRadioButtons refreshScope={refreshScope} />
          ) : (
            <Box px="xl">
              <Text color="grey400">
                <FormattedMessage
                  id="connection.actions.refreshStream.merge.description"
                  values={{
                    value: streamsSupportingMergeRefresh.length,
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
          <Box px="xl">
            <Text color="grey400">
              <FormattedMessage
                id="connection.actions.refreshStream.truncate.description"
                values={{
                  count: streamsSupportingTruncateRefresh.length,
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
        {refreshScope === "connection" && totalEnabledStreams && (
          <Box pt="lg" px="lg">
            <StreamsRefreshListBlock
              streamsSupportingMergeRefresh={streamsSupportingMergeRefresh}
              streamsSupportingTruncateRefresh={streamsSupportingTruncateRefresh}
              totalStreams={totalEnabledStreams}
            />
          </Box>
        )}
        <Box p="lg">
          <FormSubmissionButtons
            submitKey={
              refreshScope === "connection"
                ? "connection.actions.refreshConnection"
                : "connection.stream.actions.refreshStream"
            }
            onCancelClickCallback={onCancel}
            allowNonDirtyCancel
            allowNonDirtySubmit
          />
        </Box>
      </Form>
    </FlexContainer>
  );
};
