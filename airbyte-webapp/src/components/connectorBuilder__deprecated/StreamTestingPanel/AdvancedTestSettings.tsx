import { useFormContext } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import { z } from "zod";

import { Form, FormControl } from "components/forms";
import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Modal, ModalBody, ModalFooter } from "components/ui/Modal";
import { Tooltip } from "components/ui/Tooltip";

import {
  TestReadContext,
  useConnectorBuilderTestRead,
} from "services/connectorBuilder__deprecated/ConnectorBuilderStateService";

import styles from "./AdvancedTestSettings.module.scss";
import { BuilderField } from "../Builder/BuilderField";
import { zodJsonString } from "../useBuilderValidationSchema";

const MAX_RECORD_LIMIT = 5000;
const MAX_PAGE_LIMIT = 20;
const MAX_SLICE_LIMIT = 20;
const MAX_STREAM_LIMIT = 100;

interface AdvancedTestSettingsFormValues {
  recordLimit: number;
  pageLimit: number;
  sliceLimit: number;
  streamLimit: number;
  testState?: string;
}

const testLimitsValidation = z.object({
  recordLimit: z.coerce.number().min(1).max(MAX_RECORD_LIMIT),
  pageLimit: z.coerce.number().min(1).max(MAX_PAGE_LIMIT),
  sliceLimit: z.coerce.number().min(1).max(MAX_SLICE_LIMIT),
  streamLimit: z.coerce.number().min(1).max(MAX_STREAM_LIMIT),
  testState: zodJsonString.optional(),
});

interface AdvancedTestSettingsProps {
  className?: string;
  isOpen: boolean;
  setIsOpen: (open: boolean) => void;
}

export const AdvancedTestSettings: React.FC<AdvancedTestSettingsProps> = ({ className, isOpen, setIsOpen }) => {
  const { formatMessage } = useIntl();
  const {
    testReadLimits: { recordLimit, setRecordLimit, pageLimit, setPageLimit, sliceLimit, setSliceLimit, defaultLimits },
    testState,
    setTestState,
    streamRead: { isFetching },
    generatedStreamsLimits: { streamLimit, setStreamLimit, defaultGeneratedLimits },
  } = useConnectorBuilderTestRead();

  return (
    <>
      <Tooltip
        containerClassName={className}
        control={
          <Button
            className={styles.button}
            type="button"
            size="sm"
            variant="clear"
            data-testid="test-read-settings"
            disabled={isFetching}
            onClick={() => setIsOpen(true)}
            icon="gear"
          />
        }
      >
        <FormattedMessage id="connectorBuilder.testReadSettings.button" />
      </Tooltip>
      {isOpen && (
        <Modal
          size="md"
          onCancel={() => setIsOpen(false)}
          title={formatMessage({ id: "connectorBuilder.testReadSettings.modalTitle" })}
        >
          <Form<AdvancedTestSettingsFormValues>
            zodSchema={testLimitsValidation}
            defaultValues={{
              recordLimit,
              pageLimit,
              sliceLimit,
              streamLimit,
              testState,
            }}
            onSubmit={async (data: AdvancedTestSettingsFormValues) => {
              setRecordLimit(data.recordLimit);
              setSliceLimit(data.sliceLimit);
              setPageLimit(data.pageLimit);
              setStreamLimit(data.streamLimit);
              setTestState(data.testState ?? "");
              setIsOpen(false);
            }}
          >
            <AdvancedTestSettingsForm
              defaultLimits={defaultLimits}
              recordLimit={recordLimit}
              setRecordLimit={setRecordLimit}
              pageLimit={pageLimit}
              setPageLimit={setPageLimit}
              sliceLimit={sliceLimit}
              setSliceLimit={setSliceLimit}
              defaultGeneratedLimits={defaultGeneratedLimits}
              streamLimit={streamLimit}
              setStreamLimit={setStreamLimit}
              testState={testState}
              setTestState={setTestState}
              setIsOpen={setIsOpen}
            />
          </Form>
        </Modal>
      )}
    </>
  );
};

const AdvancedTestSettingsForm: React.FC<
  Pick<AdvancedTestSettingsProps, "setIsOpen"> &
    TestReadContext["testReadLimits"] &
    TestReadContext["generatedStreamsLimits"] &
    Pick<TestReadContext, "testState" | "setTestState">
> = ({
  defaultLimits,
  recordLimit,
  setRecordLimit,
  pageLimit,
  setPageLimit,
  sliceLimit,
  setSliceLimit,
  defaultGeneratedLimits,
  streamLimit,
  setStreamLimit,
  testState,
  setTestState,
  setIsOpen,
}) => {
  const { formatMessage } = useIntl();
  const {
    formState: { isValid },
    reset,
  } = useFormContext<AdvancedTestSettingsFormValues>();

  return (
    <>
      <ModalBody className={styles.body}>
        <FormControl<AdvancedTestSettingsFormValues>
          fieldType="input"
          type="number"
          name="recordLimit"
          min={1}
          max={MAX_RECORD_LIMIT}
          label={formatMessage({ id: "connectorBuilder.testReadSettings.recordLimit" })}
        />
        <FormControl<AdvancedTestSettingsFormValues>
          fieldType="input"
          type="number"
          name="pageLimit"
          min={1}
          max={MAX_PAGE_LIMIT}
          label={formatMessage({ id: "connectorBuilder.testReadSettings.pageLimit" })}
        />
        <FormControl<AdvancedTestSettingsFormValues>
          fieldType="input"
          type="number"
          name="sliceLimit"
          min={1}
          max={MAX_SLICE_LIMIT}
          label={formatMessage({ id: "connectorBuilder.testReadSettings.sliceLimit" })}
        />
        <FormControl<AdvancedTestSettingsFormValues>
          fieldType="input"
          type="number"
          name="streamLimit"
          min={1}
          max={MAX_STREAM_LIMIT}
          label={formatMessage({ id: "connectorBuilder.generateStreamsSettings.streamLimit" })}
        />
        <BuilderField
          type="jsoneditor"
          path="testState"
          label={formatMessage({ id: "connectorBuilder.testReadSettings.testState" })}
        />
      </ModalBody>
      <ModalFooter>
        <FlexContainer className={styles.footer}>
          <FlexItem grow>
            <Button
              type="button"
              variant="danger"
              onClick={() => {
                setRecordLimit(defaultLimits.recordLimit);
                setSliceLimit(defaultLimits.sliceLimit);
                setPageLimit(defaultLimits.pageLimit);
                setStreamLimit(defaultGeneratedLimits.streamLimit);
                setTestState("");
                reset({ ...defaultLimits });
                reset({ ...defaultGeneratedLimits });
              }}
            >
              <FormattedMessage id="form.reset" />
            </Button>
          </FlexItem>
          <Button
            type="button"
            variant="secondary"
            onClick={() => {
              reset({ recordLimit, pageLimit, sliceLimit, streamLimit, testState });
              setIsOpen(false);
            }}
          >
            <FormattedMessage id="form.cancel" />
          </Button>
          <Button type="submit" disabled={!isValid}>
            <FormattedMessage id="form.saveChanges" />
          </Button>
        </FlexContainer>
      </ModalFooter>
    </>
  );
};
