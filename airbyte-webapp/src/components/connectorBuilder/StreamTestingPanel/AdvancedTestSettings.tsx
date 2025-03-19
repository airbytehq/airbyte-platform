import { useFormContext } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import * as yup from "yup";

import { Form, FormControl } from "components/forms";
import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Modal, ModalBody, ModalFooter } from "components/ui/Modal";
import { Tooltip } from "components/ui/Tooltip";

import { TestReadContext, useConnectorBuilderTestRead } from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./AdvancedTestSettings.module.scss";
import { BuilderField } from "../Builder/BuilderField";
import { jsonString } from "../useBuilderValidationSchema";

const MAX_RECORD_LIMIT = 5000;
const MAX_PAGE_LIMIT = 20;
const MAX_SLICE_LIMIT = 20;

const numericCountField = yup
  .number()
  .positive()
  .required()
  .transform((value) => (isNaN(value) ? 0 : value));
const testReadLimitsValidation = yup.object({
  recordLimit: numericCountField.label("Record limit").max(MAX_RECORD_LIMIT).min(1),
  pageLimit: numericCountField.label("Page limit").max(MAX_PAGE_LIMIT).min(1),
  sliceLimit: numericCountField.label("Partition limit").max(MAX_SLICE_LIMIT).min(1),
  testState: jsonString,
});

interface AdvancedTestSettingsFormValues {
  recordLimit: number;
  pageLimit: number;
  sliceLimit: number;
  testState?: string;
}

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
            schema={testReadLimitsValidation}
            defaultValues={{
              recordLimit,
              pageLimit,
              sliceLimit,
              testState,
            }}
            onSubmit={async (data: AdvancedTestSettingsFormValues) => {
              setRecordLimit(data.recordLimit);
              setSliceLimit(data.sliceLimit);
              setPageLimit(data.pageLimit);
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
    Pick<TestReadContext, "testState" | "setTestState">
> = ({
  defaultLimits,
  recordLimit,
  setRecordLimit,
  pageLimit,
  setPageLimit,
  sliceLimit,
  setSliceLimit,
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
                setTestState("");
                reset({ ...defaultLimits });
              }}
            >
              <FormattedMessage id="form.reset" />
            </Button>
          </FlexItem>
          <Button
            type="button"
            variant="secondary"
            onClick={() => {
              reset({ recordLimit, pageLimit, sliceLimit, testState });
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
