import { Controller, useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage } from "react-intl";
import * as yup from "yup";

import { RadioButtonTiles } from "components/connection/CreateConnection/RadioButtonTiles";
import { Form } from "components/forms";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { ConnectionStream, JobReadResponse, RefreshMode } from "core/api/types/AirbyteClient";

import { StreamsRefreshListBlock } from "./StreamsRefreshListBlock";

interface ConnectionRefreshModalProps {
  refreshScope: "connection" | "stream";
  onComplete: () => void;
  onCancel: () => void;
  streamsSupportingMergeRefresh: ConnectionStream[];
  streamsSupportingTruncateRefresh: ConnectionStream[];
  refreshStreams: ({
    streams,
    refreshMode,
  }: {
    streams?: ConnectionStream[];
    refreshMode: RefreshMode;
  }) => Promise<JobReadResponse>;
  totalEnabledStreams?: number;
}

export interface ConnectionRefreshFormValues {
  refreshMode: RefreshMode;
  streams?: ConnectionStream[];
}

const ConnectionRefreshModalStreamsListBlock: React.FC<{
  streamsSupportingMergeRefresh: ConnectionStream[];
  streamsSupportingTruncateRefresh: ConnectionStream[];
  totalEnabledStreams: number;
}> = ({ streamsSupportingMergeRefresh, streamsSupportingTruncateRefresh, totalEnabledStreams }) => {
  const refreshMode = useWatch<ConnectionRefreshFormValues, "refreshMode">({ name: "refreshMode" });

  return (
    <StreamsRefreshListBlock
      streamsToList={
        refreshMode === RefreshMode.Merge ? streamsSupportingMergeRefresh : streamsSupportingTruncateRefresh
      }
      totalStreams={totalEnabledStreams}
    />
  );
};

const MergeTruncateRadioButtons: React.FC<{
  refreshScope: "connection" | "stream";
}> = ({ refreshScope }) => {
  const { setValue, control } = useFormContext<ConnectionRefreshFormValues>();

  return (
    <Box pt="sm">
      <Controller
        control={control}
        name="refreshMode"
        render={({ field }) => {
          return (
            <RadioButtonTiles
              light
              direction="column"
              options={[
                {
                  value: RefreshMode.Merge,
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
                  value: RefreshMode.Truncate,
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
                setValue("refreshMode", value, { shouldDirty: true });
              }}
              name="refreshMode"
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
  const canMerge = streamsSupportingMergeRefresh.length > 0;
  const canTruncate = streamsSupportingTruncateRefresh.length > 0;

  const onSubmitRefreshStreamForm = async (values: ConnectionRefreshFormValues) => {
    await refreshStreams({
      streams:
        values.refreshMode === RefreshMode.Merge ? streamsSupportingMergeRefresh : streamsSupportingTruncateRefresh,
      refreshMode: values.refreshMode,
    });
    onComplete();
  };

  const refreshConnectionFormSchema = yup.object().shape({
    refreshMode: yup.mixed<ConnectionRefreshFormValues["refreshMode"]>().required(),
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
        </Text>
        <Box py="sm">
          <Text>
            <FormattedMessage id="connection.actions.refreshStream.description.note" />
          </Text>
        </Box>
      </Box>
      <Form<ConnectionRefreshFormValues>
        schema={refreshConnectionFormSchema}
        onSubmit={async (values) => {
          await onSubmitRefreshStreamForm(values);
        }}
        defaultValues={{
          refreshMode: canMerge ? RefreshMode.Merge : RefreshMode.Truncate,
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
                  value: streamsSupportingTruncateRefresh.length,
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
            <ConnectionRefreshModalStreamsListBlock
              totalEnabledStreams={totalEnabledStreams}
              streamsSupportingMergeRefresh={streamsSupportingMergeRefresh}
              streamsSupportingTruncateRefresh={streamsSupportingTruncateRefresh}
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
