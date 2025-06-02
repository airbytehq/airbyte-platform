import { yupResolver } from "@hookform/resolvers/yup";
import classNames from "classnames";
import merge from "lodash/merge";
import React, { useCallback, useEffect, useMemo, useState } from "react";
import { FormProvider, useForm, useFormContext } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import { v4 as uuid } from "uuid";
import * as yup from "yup";

import { Button } from "components/ui/Button";
import { Option } from "components/ui/ComboBox";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Modal, ModalBody, ModalFooter } from "components/ui/Modal";

import { SimpleRetrieverRequester } from "core/api/types/ConnectorManifest";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { useConnectorBuilderFormState } from "services/connectorBuilder__deprecated/ConnectorBuilderStateService";

import styles from "./AddStreamButton.module.scss";
import {
  convertToAssistFormValuesSync,
  BuilderAssistFindStreamsResponse,
  BuilderAssistInputStreamParams,
  BuilderAssistManifestResponse,
  useBuilderAssistCreateStream,
  useBuilderAssistFindStreams,
} from "./Assist/assist";
import { AssistWaiting } from "./Assist/AssistWaiting";
import { BuilderField } from "./BuilderField";
import {
  BuilderStream,
  DEFAULT_BUILDER_STREAM_VALUES,
  DEFAULT_BUILDER_ASYNC_STREAM_VALUES,
  DEFAULT_SCHEMA,
  BuilderDynamicStream,
} from "../types";
import { useBuilderWatch } from "../useBuilderWatch";

interface AddStreamResponse {
  streamName: string;
  newStreamValues: BuilderStream;
}

interface AddDynamicStreamResponse extends BuilderDynamicStream {}

interface AddStreamButtonProps {
  onAddStream: (addedStreamNum: number) => void;
  streamType: "stream" | "dynamicStream";
  button?: React.ReactElement;
  "data-testid"?: string;
  modalTitle?: string;
  disabled?: boolean;
}

export const AddStreamButton: React.FC<AddStreamButtonProps> = ({
  onAddStream,
  streamType,
  button,
  "data-testid": testId,
  modalTitle,
  disabled,
}) => {
  const analyticsService = useAnalyticsService();
  const baseUrl = useBuilderWatch("formValues.global.urlBase");
  const streams = useBuilderWatch("formValues.streams");
  const dynamicStreams = useBuilderWatch("formValues.dynamicStreams");
  const [isOpen, setIsOpen] = useState(false);
  const { setValue } = useFormContext();
  const numStreams = streams.length;
  const numDynamicStreams = dynamicStreams.length;

  const buttonClickHandler = () => {
    setIsOpen(true);
  };

  const shouldPulse = numStreams === 0 && numDynamicStreams === 0 && baseUrl;

  const handleSubmit = (values: AddStreamResponse | AddDynamicStreamResponse) => {
    const id = uuid();

    if (streamType === "stream") {
      const streamValues = values as AddStreamResponse;
      setValue("formValues.streams", [
        ...streams,
        {
          ...streamValues.newStreamValues,
          name: streamValues.streamName,
          schema: DEFAULT_SCHEMA,
          id,
          testResults: { streamHash: null },
        },
      ]);
      onAddStream(numStreams);
      analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.STREAM_CREATE, {
        actionDescription: "New stream created from the Add Stream button",
        stream_id: id,
        stream_name: streamValues.streamName,
        url_path:
          streamValues.newStreamValues.requestType === "sync"
            ? streamValues.newStreamValues.urlPath
            : streamValues.newStreamValues.creationRequester.url,
      });
    } else {
      const dynamicStreamValues = values as AddDynamicStreamResponse;
      setValue("formValues.dynamicStreams", [
        ...dynamicStreams,
        {
          ...dynamicStreamValues,
          schema: DEFAULT_SCHEMA,
          id,
          testResults: {
            // indicates that this stream was added by the Builder and needs to be tested
            streamHash: null,
          },
        },
      ]);
      onAddStream(numDynamicStreams);
      analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.DYNAMIC_STREAM_CREATE, {
        actionDescription: "New dynamic stream created from the Add Stream button",
        stream_id: id,
        stream_name: dynamicStreamValues.dynamicStreamName,
        url_path: dynamicStreamValues.componentsResolver.retriever.requester.url_base,
      });
    }
    setIsOpen(false);
  };

  return (
    <>
      {button ? (
        React.cloneElement(button, {
          onClick: buttonClickHandler,
          "data-testid": testId,
          disabled: disabled ?? button.props.disabled,
          className: classNames(button.props.className, styles.disableable),
        })
      ) : (
        <div className={classNames(styles.buttonContainer, { [styles["buttonContainer--pulse"]]: shouldPulse })}>
          <Button
            type="button"
            className={classNames(styles.addButton, styles.disableable)}
            onClick={buttonClickHandler}
            icon="plus"
            data-testid={testId}
            disabled={disabled}
          />
        </div>
      )}
      {isOpen && (
        <AddStreamModal
          modalTitle={modalTitle}
          onSubmit={handleSubmit}
          onCancel={() => setIsOpen(false)}
          streams={streams}
          dynamicStreams={dynamicStreams}
          streamType={streamType}
        />
      )}
    </>
  );
};

const getStreamOptions = (currentStreams: BuilderStream[], data: BuilderAssistFindStreamsResponse): Option[] => {
  if (!data?.streams) {
    return [];
  }
  const current = new Set(currentStreams.map((stream) => stream.name.toLowerCase().trim()));
  const given = new Set(data.streams.map((stream) => stream.stream_name.toLowerCase().trim()));
  const couldAdd = [...given].filter((stream) => !current.has(stream));
  return couldAdd
    .sort() // sort by name
    .map((stream) => ({
      value: stream,
      iconRight: <Icon type="aiStars" color="magic" size="sm" />,
    }));
};

interface AddStreamFormValues {
  streamName: string;
  urlPath: string;
  copyOtherStream?: boolean;
  streamToCopy?: string;
  requestType?: BuilderStream["requestType"];
}

interface AddDynamicStreamFormValues {
  dynamicStreamName: string;
  urlPath: string;
}

const AddStreamModal = ({
  modalTitle,
  onSubmit,
  onCancel,
  streams,
  dynamicStreams,
  streamType,
}: {
  modalTitle?: string;
  onSubmit: (values: AddStreamResponse | AddDynamicStreamResponse) => void;
  onCancel: () => void;
  streams: BuilderStream[];
  dynamicStreams: BuilderDynamicStream[];
  streamType: "stream" | "dynamicStream";
}) => {
  const { assistEnabled } = useConnectorBuilderFormState();
  const shouldAssist = assistEnabled && streamType === "stream"; // AI assist only for regular streams

  // TODO refactor to useMutation, as this is a bit of a hack
  const [assistFormValues, setAssistFormValues] = useState<AddStreamFormValues | null>(null);

  const submitResponse = useCallback(
    (values: AddStreamFormValues | AddDynamicStreamFormValues) => {
      if (streamType === "stream") {
        const streamValues = values as AddStreamFormValues;
        const otherStreamValues = streamValues.copyOtherStream
          ? streams.find((stream) => stream.name === streamValues.streamToCopy)
          : undefined;

        onSubmit({
          streamName: streamValues.streamName,
          newStreamValues: merge(
            {},
            streamValues.requestType === "sync" ? DEFAULT_BUILDER_STREAM_VALUES : DEFAULT_BUILDER_ASYNC_STREAM_VALUES,
            otherStreamValues,
            streamValues.requestType === "sync"
              ? {
                  urlPath: streamValues.urlPath,
                }
              : {
                  creationRequester: {
                    url: streamValues.urlPath,
                  },
                }
          ),
        });
      } else {
        const dynamicStreamValues = values as AddDynamicStreamFormValues;
        onSubmit({
          dynamicStreamName: dynamicStreamValues.dynamicStreamName,
          streamTemplate: {
            ...structuredClone(DEFAULT_BUILDER_STREAM_VALUES),
            name: `${dynamicStreamValues.dynamicStreamName}_stream_template`,
          },
          componentsResolver: {
            type: "HttpComponentsResolver",
            retriever: {
              type: "SimpleRetriever",
              requester: {
                $ref: "#/definitions/base_requester",
                path: dynamicStreamValues.urlPath,
              } as unknown as SimpleRetrieverRequester,
              record_selector: {
                type: "RecordSelector",
                extractor: {
                  type: "DpathExtractor",
                  field_path: [],
                },
                // record_filter must be present for the form control logic
                // this component is removed from the manifest if the condition is empty
                record_filter: {
                  type: "RecordFilter",
                  condition: "",
                },
              },
            },
          },
        });
      }
    },
    [onSubmit, streamType, streams]
  );

  const submitAction = useCallback(
    (values: AddStreamFormValues | AddDynamicStreamFormValues) => {
      // use AI Assistant if the user isn't copying from another and AI is on
      const shouldAssistValues = shouldAssist && !(values as AddStreamFormValues).copyOtherStream;
      if (shouldAssistValues) {
        setAssistFormValues(values as AddStreamFormValues);
      } else {
        // return the values as is
        submitResponse(values);
      }
    },
    [shouldAssist, setAssistFormValues, submitResponse]
  );

  const cancelAction = useCallback(() => {
    if (assistFormValues) {
      submitResponse(assistFormValues);
    } else {
      onCancel();
    }
  }, [assistFormValues, submitResponse, onCancel]);

  const assistInput = useMemo(() => {
    if (!assistFormValues) {
      return null;
    }
    return {
      stream_name: assistFormValues.streamName,
    };
  }, [assistFormValues]);

  return (
    <Modal
      size="sm"
      title={
        modalTitle ?? (
          <FormattedMessage
            id={
              streamType === "stream"
                ? "connectorBuilder.addStreamModal.title"
                : "connectorBuilder.addDynamicStreamModal.title"
            }
          />
        )
      }
      onCancel={cancelAction}
    >
      {assistInput ? (
        <AssistProcessing input={assistInput} onComplete={onSubmit} onSkip={cancelAction} />
      ) : streamType === "stream" ? (
        <AddStreamForm onSubmit={submitAction} onCancel={cancelAction} streams={streams} shouldAssist={shouldAssist} />
      ) : (
        <AddDynamicStreamForm onSubmit={submitAction} onCancel={cancelAction} dynamicStreams={dynamicStreams} />
      )}
    </Modal>
  );
};

const AddStreamForm = ({
  onSubmit,
  onCancel,
  streams,
  shouldAssist,
}: {
  onSubmit: (values: AddStreamFormValues) => void;
  onCancel: () => void;
  streams: BuilderStream[];
  shouldAssist: boolean;
}) => {
  const { formatMessage } = useIntl();

  const { data, isFetching } = useBuilderAssistFindStreams({
    enabled: shouldAssist,
  });

  const showCopyFromStream = streams.length > 0;
  const showUrlPath = !shouldAssist;

  const validator: Record<string, yup.StringSchema> = {
    streamName: yup
      .string()
      .required("form.empty.error")
      .notOneOf(
        streams.map((stream) => stream.name),
        "connectorBuilder.duplicateStreamName"
      ),
  };

  if (showUrlPath) {
    validator.urlPath = yup.string().required("form.empty.error");
  }

  // put the main form default values here so the API can use the context in the new form
  const methods = useForm({
    defaultValues: {
      streamName: "",
      urlPath: "",
      copyOtherStream: false,
      streamToCopy: streams[0]?.name,
      requestType: "sync" as const,
    },
    resolver: yupResolver(yup.object().shape(validator)),
    mode: "onChange",
  });

  const useOtherStream = methods.watch("copyOtherStream");
  const requestType = methods.watch("requestType");

  return (
    <FormProvider {...methods}>
      <form onSubmit={methods.handleSubmit(onSubmit)}>
        <ModalBody className={styles.body}>
          {shouldAssist ? (
            <AssistedStreamNameField path="streamName" streams={streams} data={data} isFetching={isFetching} />
          ) : (
            <BuilderField
              path="streamName"
              type="string"
              label={formatMessage({ id: "connectorBuilder.addStreamModal.streamNameLabel" })}
              tooltip={formatMessage({ id: "connectorBuilder.addStreamModal.streamNameTooltip" })}
            />
          )}
          {showUrlPath && (
            <BuilderField
              path="urlPath"
              type="jinja"
              label={formatMessage({
                id:
                  useOtherStream || requestType === "sync"
                    ? "connectorBuilder.addStreamModal.urlPathLabel"
                    : "connectorBuilder.asyncStream.url.label",
              })}
              tooltip={formatMessage({
                id:
                  useOtherStream || requestType === "sync"
                    ? "connectorBuilder.addStreamModal.urlPathTooltip"
                    : "connectorBuilder.asyncStream.url.tooltip",
              })}
              bubbleUpUndoRedo={false}
            />
          )}
          {/* Only allow to copy from another stream within the modal if there aren't initial values set already and there are other streams */}
          {showCopyFromStream && (
            <>
              <BuilderField
                path="copyOtherStream"
                type="boolean"
                label={formatMessage({ id: "connectorBuilder.addStreamModal.copyOtherStreamLabel" })}
              />
              {useOtherStream && (
                <BuilderField
                  label={formatMessage({ id: "connectorBuilder.addStreamModal.streamLabel" })}
                  path="streamToCopy"
                  type="enum"
                  options={streams.map((stream) => stream.name)}
                />
              )}
            </>
          )}
          {!useOtherStream && (
            <BuilderField
              type="enum"
              path="requestType"
              label={formatMessage({ id: "connectorBuilder.requestType" })}
              options={[
                {
                  value: "sync",
                  label: formatMessage({ id: "connectorBuilder.requestType.sync" }),
                },
                {
                  value: "async",
                  label: formatMessage({ id: "connectorBuilder.requestType.async" }),
                },
              ]}
            />
          )}
        </ModalBody>
        <ModalFooter>
          <Button
            variant="secondary"
            type="reset"
            onClick={() => {
              onCancel();
            }}
          >
            <FormattedMessage id="form.cancel" />
          </Button>
          <Button type="submit">
            <FormattedMessage id="form.create" />
          </Button>
        </ModalFooter>
      </form>
    </FormProvider>
  );
};

const AddDynamicStreamForm = ({
  onSubmit,
  onCancel,
  dynamicStreams,
}: {
  onSubmit: (values: AddDynamicStreamFormValues) => void;
  onCancel: () => void;
  dynamicStreams: BuilderDynamicStream[];
}) => {
  const { formatMessage } = useIntl();

  const validator = {
    dynamicStreamName: yup
      .string()
      .required("form.empty.error")
      .notOneOf(
        dynamicStreams.map((stream) => stream.dynamicStreamName),
        "connectorBuilder.duplicateStreamName"
      ),
    urlPath: yup.string().required("form.empty.error"),
  };

  const methods = useForm({
    defaultValues: {
      dynamicStreamName: "",
      urlPath: "",
    },
    resolver: yupResolver(yup.object().shape(validator)),
    mode: "onChange",
  });

  return (
    <FormProvider {...methods}>
      <form onSubmit={methods.handleSubmit(onSubmit)}>
        <ModalBody className={styles.body}>
          <BuilderField
            path="dynamicStreamName"
            type="string"
            label={formatMessage({ id: "connectorBuilder.addDynamicStreamModal.dynamicStreamNameLabel" })}
            tooltip={formatMessage({ id: "connectorBuilder.addDynamicStreamModal.dynamicStreamNameTooltip" })}
          />
          <BuilderField
            path="urlPath"
            type="jinja"
            label={formatMessage({ id: "connectorBuilder.addDynamicStreamModal.urlPathLabel" })}
            tooltip={formatMessage({ id: "connectorBuilder.addDynamicStreamModal.urlPathTooltip" })}
            bubbleUpUndoRedo={false}
          />
        </ModalBody>
        <ModalFooter>
          <Button variant="secondary" type="reset" onClick={onCancel}>
            <FormattedMessage id="form.cancel" />
          </Button>
          <Button type="submit">
            <FormattedMessage id="form.create" />
          </Button>
        </ModalFooter>
      </form>
    </FormProvider>
  );
};

const AssistLoadingMessage = () => {
  const { formatMessage } = useIntl();

  return (
    <FlexContainer gap="sm" direction="row" alignItems="center">
      {formatMessage({ id: "connectorBuilder.assist.addStream.fetching" })}
      <Icon type="aiStars" color="magic" size="sm" />
    </FlexContainer>
  );
};

const AssistedStreamNameField = ({
  path,
  streams,
  data,
  isFetching,
}: {
  path: string;
  streams: BuilderStream[];
  data: BuilderAssistFindStreamsResponse | undefined;
  isFetching: boolean;
}) => {
  const { formatMessage } = useIntl();

  const streamOptions = useMemo(() => {
    if (data) {
      return getStreamOptions(streams, data);
    }
    return [];
  }, [data, streams]);

  return (
    <BuilderField
      path={path}
      type="combobox"
      label={formatMessage({ id: "connectorBuilder.addStreamModal.streamNameLabel" })}
      tooltip={formatMessage({ id: "connectorBuilder.addStreamModal.streamNameTooltip" })}
      options={streamOptions}
      optionsConfig={{
        loading: isFetching,
        loadingMessage: <AssistLoadingMessage />,
        instructionMessage: formatMessage({ id: "connectorBuilder.assist.addStream.instructions" }),
      }}
    />
  );
};

const generateAddStreamResponse = (streamName: string, data: BuilderAssistManifestResponse): AddStreamResponse => {
  const updatedForm = convertToAssistFormValuesSync(data);
  const newStreamValues = updatedForm.streams[0];
  return {
    streamName,
    newStreamValues,
  };
};

const AssistProcessing = ({
  input,
  onComplete,
  onSkip,
}: {
  input: BuilderAssistInputStreamParams;
  onComplete: (values: AddStreamResponse) => void;
  onSkip: () => void;
}) => {
  const { data, isError } = useBuilderAssistCreateStream(input);
  useEffect(() => {
    if (data) {
      const results = generateAddStreamResponse(input.stream_name, data);
      onComplete(results);
    } else if (isError) {
      onSkip();
    }
  }, [data, isError, input.stream_name, onComplete, onSkip]);

  return (
    <ModalBody>
      <AssistWaiting onSkip={onSkip} />
    </ModalBody>
  );
};
