import classNames from "classnames";
import merge from "lodash/merge";
import React, { useCallback, useEffect, useMemo, useState } from "react";
import { get, useWatch, useFormContext, useFormState } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import { z } from "zod";

import { Form, FormControl } from "components/forms";
import { FormControlErrorMessage, FormControlFooter, FormLabel } from "components/forms/FormControl";
import { Button } from "components/ui/Button";
import { ComboBox, Option } from "components/ui/ComboBox";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Modal, ModalBody, ModalFooter } from "components/ui/Modal";
import { Switch } from "components/ui/Switch";

import {
  AsyncRetriever,
  AsyncRetrieverType,
  CustomRetriever,
  DeclarativeComponentSchemaStreamsItem,
  DeclarativeStream,
  DeclarativeStreamType,
  DynamicDeclarativeStream,
  HttpComponentsResolverType,
  SimpleRetriever,
  SimpleRetrieverType,
} from "core/api/types/ConnectorManifest";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

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
import { DEFAULT_ASYNC_STREAM, DEFAULT_DYNAMIC_STREAM, DEFAULT_SYNC_STREAM } from "../constants";
import { useBuilderWatch } from "../useBuilderWatch";

type AddStreamResponse =
  | {
      streamType: "stream";
      newStream: DeclarativeStream;
    }
  | {
      streamType: "dynamicStream";
      newStream: DynamicDeclarativeStream;
    };

interface AddStreamButtonProps {
  onAddStream: (addedStreamNum: number) => void;
  streamType: "stream" | "dynamicStream";
  "data-testid"?: string;
  modalTitle?: string;
  disabled?: boolean;
}

export const AddStreamButton: React.FC<AddStreamButtonProps> = ({
  onAddStream,
  streamType,
  "data-testid": testId,
  modalTitle,
  disabled,
}) => {
  const analyticsService = useAnalyticsService();
  const streams = useBuilderWatch("manifest.streams") ?? [];
  const dynamicStreams = useBuilderWatch("manifest.dynamic_streams") ?? [];
  const [isOpen, setIsOpen] = useState(false);
  const { setValue } = useFormContext();
  const numStreams = streams.length;
  const numDynamicStreams = dynamicStreams.length;

  const buttonClickHandler = () => {
    setIsOpen(true);
  };

  const shouldPulse = numStreams === 0 && numDynamicStreams === 0;

  const handleSubmit = (values: AddStreamResponse) => {
    if (values.streamType === "stream") {
      setValue("manifest.streams", [
        ...streams,
        {
          ...values.newStream,
        },
      ]);
      if (values.newStream.name) {
        setValue(`manifest.metadata.autoImportSchema.${values.newStream.name}`, true);
      }
      onAddStream(numStreams);
      analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.STREAM_CREATE, {
        actionDescription: "New stream created from the Add Stream button",
        stream_name: values.newStream.name,
        url: getStreamUrl(values.newStream.retriever),
      });
    } else {
      setValue("manifest.dynamic_streams", [
        ...dynamicStreams,
        {
          ...values.newStream,
        },
      ]);
      if (values.newStream.name) {
        setValue(`manifest.metadata.autoImportSchema.${values.newStream.name}`, true);
      }
      onAddStream(numDynamicStreams);
      analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.DYNAMIC_STREAM_CREATE, {
        actionDescription: "New dynamic stream created from the Add Stream button",
        stream_name: values.newStream.name,
        url:
          values.newStream.components_resolver.type === HttpComponentsResolverType.HttpComponentsResolver
            ? getStreamUrl(values.newStream.components_resolver.retriever)
            : undefined,
      });
    }
    setIsOpen(false);
  };

  return (
    <>
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

const getStreamOptions = (currentStreamNames: string[], data: BuilderAssistFindStreamsResponse): Option[] => {
  if (!data?.streams) {
    return [];
  }
  const current = new Set(currentStreamNames);
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
  copyFromStreamName?: string;
  retrievalType?: "sync" | "async";
}

interface AddDynamicStreamFormValues {
  dynamicStreamName: string;
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
  onSubmit: (values: AddStreamResponse) => void;
  onCancel: () => void;
  streams: DeclarativeComponentSchemaStreamsItem[];
  dynamicStreams: DynamicDeclarativeStream[];
  streamType: "stream" | "dynamicStream";
}) => {
  const { assistEnabled } = useConnectorBuilderFormState();
  const shouldAssist = assistEnabled && streamType === "stream"; // AI assist only for regular streams

  const [assistFormValues, setAssistFormValues] = useState<AddStreamFormValues | null>(null);

  const streamNames = useMemo(() => {
    return [
      ...streams.filter((stream) => "name" in stream).map((stream) => (stream as { name: string }).name),
      ...dynamicStreams.map((stream) => stream.name),
    ].filter(Boolean) as string[];
  }, [streams, dynamicStreams]);

  const submitResponse = useCallback(
    (values: AddStreamFormValues | AddDynamicStreamFormValues) => {
      if (streamType === "stream") {
        const streamValues = values as AddStreamFormValues;
        const otherStreamValues = streamValues.copyFromStreamName
          ? streams.find((stream) => "name" in stream && stream.name === streamValues.copyFromStreamName)
          : undefined;

        onSubmit({
          streamType,
          newStream: merge(
            {},
            streamValues.retrievalType === "sync" ? DEFAULT_SYNC_STREAM : DEFAULT_ASYNC_STREAM,
            otherStreamValues,
            {
              name: streamValues.streamName,
            }
          ),
        });
      } else {
        const dynamicStreamValues = values as AddDynamicStreamFormValues;
        onSubmit({
          streamType,
          newStream: merge({}, DEFAULT_DYNAMIC_STREAM, {
            name: dynamicStreamValues.dynamicStreamName,
          }),
        });
      }
    },
    [onSubmit, streamType, streams]
  );

  const submitAction = useCallback(
    async (values: AddStreamFormValues | AddDynamicStreamFormValues) => {
      // use AI Assistant if the user isn't copying from another and AI is on
      const shouldAssistValues = shouldAssist && !(values as AddStreamFormValues).copyFromStreamName;
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
      {assistFormValues ? (
        <AssistProcessing
          input={{ stream_name: assistFormValues.streamName }}
          onComplete={onSubmit}
          onSkip={cancelAction}
        />
      ) : streamType === "stream" ? (
        <AddStreamForm
          onSubmit={submitAction}
          onCancel={cancelAction}
          streamNames={streamNames}
          shouldAssist={shouldAssist}
        />
      ) : (
        <AddDynamicStreamForm onSubmit={submitAction} onCancel={cancelAction} streamNames={streamNames} />
      )}
    </Modal>
  );
};

const AddStreamForm = ({
  onSubmit,
  onCancel,
  streamNames,
  shouldAssist,
}: {
  onSubmit: (values: AddStreamFormValues) => Promise<void>;
  onCancel: () => void;
  streamNames: string[];
  shouldAssist: boolean;
}) => {
  const { formatMessage } = useIntl();

  const { data, isFetching } = useBuilderAssistFindStreams({
    enabled: shouldAssist,
  });

  const [copyStreamEnabled, setCopyStreamEnabled] = useState(false);

  const showCopyFromStream = streamNames.length > 0;
  const validator = {
    streamName: z
      .string()
      .trim()
      .nonempty("form.empty.error")
      .refine((val) => !streamNames.includes(val), {
        message: "connectorBuilder.duplicateStreamName",
      }),
    copyFromStreamName: z.string().optional(),
    retrievalType: z.enum(["sync", "async"]),
  };

  const defaultValues = {
    streamName: "",
    retrievalType: "sync" as const,
  };

  return (
    <Form zodSchema={z.object(validator)} defaultValues={defaultValues} onSubmit={onSubmit}>
      <ModalBody className={styles.body}>
        {shouldAssist ? (
          <AssistedStreamNameField path="streamName" streamNames={streamNames} data={data} isFetching={isFetching} />
        ) : (
          <FormControl
            name="streamName"
            fieldType="input"
            label={formatMessage({ id: "connectorBuilder.addStreamModal.streamNameLabel" })}
            labelTooltip={formatMessage({ id: "connectorBuilder.addStreamModal.streamNameTooltip" })}
          />
        )}
        {/* Only allow to copy from another stream within the modal if there aren't initial values set already and there are other streams */}
        {showCopyFromStream && (
          <>
            <FlexContainer direction="row" alignItems="center" gap="sm" className={styles.relativeWithPadding}>
              <Switch
                id="copyOtherStream"
                checked={copyStreamEnabled}
                onChange={() => setCopyStreamEnabled((prev) => !prev)}
              />
              <FormLabel
                label={formatMessage({ id: "connectorBuilder.addStreamModal.copyOtherStreamLabel" })}
                htmlFor="copyOtherStream"
              />
            </FlexContainer>
            {copyStreamEnabled && (
              <FormControl
                name="copyFromStreamName"
                fieldType="dropdown"
                label={formatMessage({ id: "connectorBuilder.addStreamModal.copyOtherStreamLabel" })}
                options={streamNames.map((name) => {
                  return {
                    value: name,
                    label: name,
                  };
                })}
              />
            )}
          </>
        )}
        {!copyStreamEnabled && (
          <FormControl
            name="retrievalType"
            fieldType="dropdown"
            label={formatMessage({ id: "connectorBuilder.retrievalType" })}
            options={[
              {
                value: "sync",
                label: formatMessage({ id: "connectorBuilder.retrievalType.sync" }),
              },
              {
                value: "async",
                label: formatMessage({ id: "connectorBuilder.retrievalType.async" }),
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
    </Form>
  );
};

const AddDynamicStreamForm = ({
  onSubmit,
  onCancel,
  streamNames,
}: {
  onSubmit: (values: AddDynamicStreamFormValues) => Promise<void>;
  onCancel: () => void;
  streamNames: string[];
}) => {
  const { formatMessage } = useIntl();

  const validator = {
    dynamicStreamName: z
      .string()
      .trim()
      .nonempty("form.empty.error")
      .refine((val) => !streamNames.includes(val), {
        message: "connectorBuilder.duplicateStreamName",
      }),
  };

  const defaultValues = {
    dynamicStreamName: "",
  };

  return (
    <Form zodSchema={z.object(validator)} defaultValues={defaultValues} onSubmit={onSubmit}>
      <ModalBody className={styles.body}>
        <FormControl
          name="dynamicStreamName"
          fieldType="input"
          label={formatMessage({ id: "connectorBuilder.addDynamicStreamModal.dynamicStreamNameLabel" })}
          labelTooltip={formatMessage({ id: "connectorBuilder.addDynamicStreamModal.dynamicStreamNameTooltip" })}
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
    </Form>
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
  streamNames,
  data,
  isFetching,
}: {
  path: string;
  streamNames: string[];
  data: BuilderAssistFindStreamsResponse | undefined;
  isFetching: boolean;
}) => {
  const { formatMessage } = useIntl();
  const { setValue } = useFormContext();
  // const { field, fieldState } = useController({ name: path });
  const { errors } = useFormState();
  console.log(errors);
  const error = get(errors, path);
  const value = useWatch({ name: path });
  const hasError = !!error;

  const streamOptions = useMemo(() => {
    if (data) {
      return getStreamOptions(streamNames, data);
    }
    return [];
  }, [data, streamNames]);

  return (
    <FlexContainer direction="column" gap="none" className={styles.relativeWithPadding}>
      <FormLabel
        label={formatMessage({ id: "connectorBuilder.addStreamModal.streamNameLabel" })}
        labelTooltip={formatMessage({ id: "connectorBuilder.addStreamModal.streamNameTooltip" })}
        htmlFor={path}
      />
      <ComboBox
        options={streamOptions}
        value={value}
        onChange={(val) => setValue(path, val ?? "", { shouldValidate: true, shouldDirty: true, shouldTouch: true })}
        error={hasError}
        filterOptions
        allowCustomValue
        optionsConfig={{
          loading: isFetching,
          loadingMessage: <AssistLoadingMessage />,
          instructionMessage: formatMessage({ id: "connectorBuilder.assist.addStream.instructions" }),
        }}
      />
      {hasError && (
        <FormControlFooter>
          <FormControlErrorMessage name={path} />
        </FormControlFooter>
      )}
    </FlexContainer>
  );
};

const generateAddStreamResponse = (streamName: string, data: BuilderAssistManifestResponse): AddStreamResponse => {
  const updatedForm = convertToAssistFormValuesSync(data);
  const generatedStream = updatedForm?.streams?.[0];

  if (!generatedStream || generatedStream.type !== DeclarativeStreamType.DeclarativeStream) {
    return {
      streamType: "stream",
      newStream: merge({}, DEFAULT_SYNC_STREAM, {
        name: streamName,
      }),
    };
  }

  return {
    streamType: "stream",
    newStream: merge({}, generatedStream, {
      name: streamName,
    }),
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

const getStreamUrl = (retriever: SimpleRetriever | AsyncRetriever | CustomRetriever) => {
  return retriever.type === SimpleRetrieverType.SimpleRetriever
    ? retriever.requester?.url
    : retriever.type === AsyncRetrieverType.AsyncRetriever
    ? retriever.creation_requester?.url
    : undefined;
};
