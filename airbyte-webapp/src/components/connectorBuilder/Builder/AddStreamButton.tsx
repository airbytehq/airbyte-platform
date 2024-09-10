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

import {
  BuilderAssistCreateStreamParams,
  BuilderAssistFindStreamsResponse,
  BuilderAssistGlobalParams,
  BuilderAssistManifestResponse,
  useBuilderAssistCreateStream,
  useBuilderAssistFindStreams,
} from "core/api";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./AddStreamButton.module.scss";
import { convertToAssistFormValuesSync } from "./Assist/assist";
import { AssistWaiting } from "./Assist/AssistWaiting";
import { BuilderField } from "./BuilderField";
import { BuilderFieldWithInputs } from "./BuilderFieldWithInputs";
import { AssistData, BuilderStream, DEFAULT_BUILDER_STREAM_VALUES, DEFAULT_SCHEMA, useBuilderWatch } from "../types";

interface AddStreamResponse {
  streamName: string;
  newStreamValues?: BuilderStream;
}

interface AddStreamButtonProps {
  onAddStream: (addedStreamNum: number) => void;
  button?: React.ReactElement;
  initialValues?: Partial<BuilderStream>;
  "data-testid"?: string;
  modalTitle?: string;
  disabled?: boolean;
}

export const AddStreamButton: React.FC<AddStreamButtonProps> = ({
  onAddStream,
  button,
  initialValues,
  "data-testid": testId,
  modalTitle,
  disabled,
}) => {
  const analyticsService = useAnalyticsService();

  const baseUrl = useBuilderWatch("formValues.global.urlBase");
  const streams = useBuilderWatch("formValues.streams");

  const [isOpen, setIsOpen] = useState(false);

  const { setValue } = useFormContext();
  const numStreams = streams.length;
  const isDuplicate = !!initialValues;

  const buttonClickHandler = () => {
    setIsOpen(true);
  };

  const shouldPulse = numStreams === 0 && baseUrl;

  const handleSubmit = (values: AddStreamResponse) => {
    const id = uuid();
    setValue("formValues.streams", [
      ...streams,
      merge({}, DEFAULT_BUILDER_STREAM_VALUES, {
        ...initialValues,
        ...values.newStreamValues,
        name: values.streamName,
        schema: DEFAULT_SCHEMA,
        id,
        testResults: {
          // indicates that this stream was added by the Builder and needs to be tested
          streamHash: null,
        },
      }),
    ]);
    setIsOpen(false);
    onAddStream(numStreams);
    if (isDuplicate) {
      analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.STREAM_COPY, {
        actionDescription: "Existing stream copied into a new stream",
        existing_stream_id: initialValues.id,
        existing_stream_name: initialValues.name,
        new_stream_id: id,
        new_stream_name: values.streamName,
        new_stream_url_path: values.newStreamValues?.urlPath,
      });
    } else {
      analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.STREAM_CREATE, {
        actionDescription: "New stream created from the Add Stream button",
        stream_id: id,
        stream_name: values.streamName,
        url_path: values.newStreamValues?.urlPath,
      });
    }
  };

  return (
    <>
      {button ? (
        React.cloneElement(button, {
          onClick: buttonClickHandler,
          "data-testid": testId,
          disabled: disabled ?? button.props.disabled, // respect `disabled` from both AddStreamButton and the custom button
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
          isDuplicate={isDuplicate}
          streams={streams}
          initialUrlPath={initialValues?.urlPath}
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

const AddStreamModal = ({
  modalTitle,
  onSubmit,
  onCancel,
  isDuplicate,
  streams,
  initialUrlPath,
}: {
  modalTitle?: string;
  onSubmit: (values: AddStreamResponse) => void;
  onCancel: () => void;
  isDuplicate: boolean;
  streams: BuilderStream[];
  initialUrlPath?: string;
}) => {
  const baseUrl = useBuilderWatch("formValues.global.urlBase");
  const appName = useBuilderWatch("name") || "Connector";
  const assistData: AssistData = useBuilderWatch("formValues.assist");
  const { assistEnabled } = useConnectorBuilderFormState();

  const shouldAssist = assistEnabled && !isDuplicate;

  // TODO refactor to useMutation, as this is a bit of a hack
  const [assistFormValues, setAssistFormValues] = useState<AddStreamFormValues | null>(null);

  const assistParams = useMemo(
    () => ({
      docs_url: assistData?.docsUrl,
      openapi_spec_url: assistData?.openApiSpecUrl,
      app_name: appName,
      url_base: baseUrl,
    }),
    [assistData?.docsUrl, assistData?.openApiSpecUrl, appName, baseUrl]
  );

  const submitResponse = useCallback(
    (values: AddStreamFormValues) => {
      const otherStreamValues = values.copyOtherStream
        ? streams.find((stream) => stream.name === values.streamToCopy)
        : undefined;

      onSubmit({
        streamName: values.streamName,
        newStreamValues: merge({}, DEFAULT_BUILDER_STREAM_VALUES, otherStreamValues, {
          urlPath: values.urlPath,
        }),
      });
    },
    [streams, onSubmit]
  );

  const submitAction = useCallback(
    (values: AddStreamFormValues) => {
      // use AI Assist if the user isn't copying from another and AI is on
      const shouldAssistValues = shouldAssist && !values.copyOtherStream;
      if (shouldAssistValues) {
        setAssistFormValues(values);
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
      ...assistParams,
      stream_name: assistFormValues.streamName,
    };
  }, [assistParams, assistFormValues]);

  return (
    <Modal
      size="sm"
      title={modalTitle ?? <FormattedMessage id="connectorBuilder.addStreamModal.title" />}
      onCancel={cancelAction}
    >
      {assistInput ? (
        <AssistProcessing input={assistInput} onComplete={onSubmit} onSkip={cancelAction} />
      ) : (
        <AddStreamForm
          onSubmit={submitAction}
          onCancel={cancelAction}
          isDuplicate={isDuplicate}
          streams={streams}
          initialUrlPath={initialUrlPath}
          assistParams={assistParams}
          shouldAssist={shouldAssist}
        />
      )}
    </Modal>
  );
};

interface AddStreamFormValues {
  streamName: string;
  urlPath: string;
  copyOtherStream?: boolean;
  streamToCopy?: string;
}

const AddStreamForm = ({
  onSubmit,
  onCancel,
  isDuplicate,
  streams,
  initialUrlPath,
  assistParams,
  shouldAssist,
}: {
  onSubmit: (values: AddStreamFormValues) => void;
  onCancel: () => void;
  isDuplicate: boolean;
  streams: BuilderStream[];
  initialUrlPath?: string;
  assistParams: BuilderAssistGlobalParams;
  shouldAssist: boolean;
}) => {
  const { formatMessage } = useIntl();

  const showCopyFromStream = !isDuplicate && streams.length > 0;
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

  const methods = useForm({
    defaultValues: {
      streamName: "",
      urlPath: initialUrlPath ?? "",
      copyOtherStream: false,
      streamToCopy: streams[0]?.name,
    },
    resolver: yupResolver(yup.object().shape(validator)),
    mode: "onChange",
  });

  const useOtherStream = methods.watch("copyOtherStream");

  return (
    <FormProvider {...methods}>
      <form onSubmit={methods.handleSubmit(onSubmit)}>
        <ModalBody className={styles.body}>
          {shouldAssist ? (
            <AssistedStreamNameField path="streamName" streams={streams} assistParams={assistParams} />
          ) : (
            <BuilderField
              path="streamName"
              type="string"
              label={formatMessage({ id: "connectorBuilder.addStreamModal.streamNameLabel" })}
              tooltip={formatMessage({ id: "connectorBuilder.addStreamModal.streamNameTooltip" })}
            />
          )}
          {showUrlPath && (
            <BuilderFieldWithInputs
              path="urlPath"
              type="string"
              label={formatMessage({ id: "connectorBuilder.addStreamModal.urlPathLabel" })}
              tooltip={formatMessage({ id: "connectorBuilder.addStreamModal.urlPathTooltip" })}
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
  assistParams,
}: {
  path: string;
  streams: BuilderStream[];
  assistParams: BuilderAssistGlobalParams;
}) => {
  const { formatMessage } = useIntl();

  const { data, isFetching } = useBuilderAssistFindStreams({
    ...assistParams,
    enabled: true,
  });

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
  input: BuilderAssistCreateStreamParams;
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
