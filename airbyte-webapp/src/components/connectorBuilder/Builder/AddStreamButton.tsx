import { yupResolver } from "@hookform/resolvers/yup";
import classNames from "classnames";
import merge from "lodash/merge";
import { useState } from "react";
import React from "react";
import { FormProvider, useForm, useFormContext } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import { v4 as uuid } from "uuid";
import * as yup from "yup";

import { Button } from "components/ui/Button";
import { Modal, ModalBody, ModalFooter } from "components/ui/Modal";

import { Action, Namespace } from "core/services/analytics";
import { useAnalyticsService } from "core/services/analytics";
import { useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./AddStreamButton.module.scss";
import { BuilderField } from "./BuilderField";
import { BuilderFieldWithInputs } from "./BuilderFieldWithInputs";
import { ReactComponent as PlusIcon } from "../../connection/ConnectionOnboarding/plusIcon.svg";
import { BuilderStream, DEFAULT_BUILDER_STREAM_VALUES, useBuilderWatch } from "../types";
import { useBuilderErrors } from "../useBuilderErrors";

interface AddStreamValues {
  streamName: string;
  urlPath: string;
  copyOtherStream?: boolean;
  streamToCopy?: string;
}

interface AddStreamButtonProps {
  onAddStream: (addedStreamNum: number) => void;
  button?: React.ReactElement;
  initialValues?: Partial<BuilderStream>;
  "data-testid"?: string;
  modalTitle?: string;
}

export const AddStreamButton: React.FC<AddStreamButtonProps> = ({
  onAddStream,
  button,
  initialValues,
  "data-testid": testId,
  modalTitle,
}) => {
  const analyticsService = useAnalyticsService();
  const { hasErrors } = useBuilderErrors();
  const { builderFormValues } = useConnectorBuilderFormState();
  const [isOpen, setIsOpen] = useState(false);

  const streams = useBuilderWatch("streams");
  const { setValue } = useFormContext();
  const numStreams = streams.length;

  const buttonClickHandler = () => {
    setIsOpen(true);
  };

  const shouldPulse =
    numStreams === 0 && !hasErrors(["global"]) && builderFormValues.global.authenticator.type !== "NoAuth";

  const handleSubmit = (values: AddStreamValues) => {
    const otherStreamValues = values.copyOtherStream
      ? streams.find((stream) => stream.name === values.streamToCopy)
      : undefined;
    const id = uuid();
    setValue("streams", [
      ...streams,
      merge({}, DEFAULT_BUILDER_STREAM_VALUES, {
        ...initialValues,
        ...otherStreamValues,
        name: values.streamName,
        urlPath: values.urlPath,
        id,
      }),
    ]);
    setIsOpen(false);
    onAddStream(numStreams);
    if (initialValues) {
      analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.STREAM_COPY, {
        actionDescription: "Existing stream copied into a new stream",
        existing_stream_id: initialValues.id,
        existing_stream_name: initialValues.name,
        new_stream_id: id,
        new_stream_name: values.streamName,
        new_stream_url_path: values.urlPath,
      });
    } else {
      analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.STREAM_CREATE, {
        actionDescription: "New stream created from the Add Stream button",
        stream_id: id,
        stream_name: values.streamName,
        url_path: values.urlPath,
      });
    }
  };

  return (
    <>
      {button ? (
        React.cloneElement(button, {
          onClick: buttonClickHandler,
          "data-testid": testId,
        })
      ) : (
        <div className={classNames(styles.buttonContainer, { [styles["buttonContainer--pulse"]]: shouldPulse })}>
          <Button
            type="button"
            className={styles.addButton}
            onClick={buttonClickHandler}
            icon={<PlusIcon />}
            data-testid={testId}
          />
        </div>
      )}
      {isOpen && (
        <Modal
          size="sm"
          title={modalTitle ?? <FormattedMessage id="connectorBuilder.addStreamModal.title" />}
          onClose={() => {
            setIsOpen(false);
          }}
        >
          <AddStreamForm
            onSubmit={handleSubmit}
            onCancel={() => setIsOpen(false)}
            showCopyFromStream={!initialValues && numStreams > 0}
            streams={streams}
          />
        </Modal>
      )}
    </>
  );
};

const AddStreamForm = ({
  onSubmit,
  onCancel,
  showCopyFromStream,
  streams,
}: {
  onSubmit: (values: AddStreamValues) => void;
  onCancel: () => void;
  showCopyFromStream: boolean;
  streams: BuilderStream[];
}) => {
  const { formatMessage } = useIntl();
  const methods = useForm({
    defaultValues: { streamName: "", urlPath: "", copyOtherStream: false, streamToCopy: streams[0]?.name },
    resolver: yupResolver(
      yup.object().shape({
        streamName: yup.string().required("form.empty.error"),
        urlPath: yup.string().required("form.empty.error"),
      })
    ),
    mode: "onChange",
  });

  const useOtherStream = methods.watch("copyOtherStream");

  return (
    <FormProvider {...methods}>
      <form onSubmit={methods.handleSubmit(onSubmit)}>
        <ModalBody className={styles.body}>
          <BuilderField
            path="streamName"
            type="string"
            label={formatMessage({ id: "connectorBuilder.addStreamModal.streamNameLabel" })}
            tooltip={formatMessage({ id: "connectorBuilder.addStreamModal.streamNameTooltip" })}
          />
          <BuilderFieldWithInputs
            path="urlPath"
            type="string"
            label={formatMessage({ id: "connectorBuilder.addStreamModal.urlPathLabel" })}
            tooltip={formatMessage({ id: "connectorBuilder.addStreamModal.urlPathTooltip" })}
          />
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
