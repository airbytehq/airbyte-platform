import { yupResolver } from "@hookform/resolvers/yup";
import { useForm } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import * as yup from "yup";

import { ControlLabels } from "components/LabeledControl";
import { Button } from "components/ui/Button";
import { CodeEditor } from "components/ui/CodeEditor";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Input } from "components/ui/Input";
import { Modal, ModalBody, ModalFooter } from "components/ui/Modal";
import { Tooltip } from "components/ui/Tooltip";

import { TestReadContext, useConnectorBuilderTestRead } from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./AdvancedTestSettings.module.scss";
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
  recordLimit: numericCountField.label("Record limit").max(MAX_RECORD_LIMIT),
  pageLimit: numericCountField.label("Page limit").max(MAX_PAGE_LIMIT),
  sliceLimit: numericCountField.label("Partition limit").max(MAX_SLICE_LIMIT),
  testState: jsonString,
});

interface AdvancedTestSettingsProps {
  isOpen: boolean;
  setIsOpen: (open: boolean) => void;
}

export const AdvancedTestSettings: React.FC<AdvancedTestSettingsProps> = ({ isOpen, setIsOpen }) => {
  const {
    testReadLimits: { recordLimit, setRecordLimit, pageLimit, setPageLimit, sliceLimit, setSliceLimit, defaultLimits },
    testState,
    setTestState,
    streamRead: { isFetching },
  } = useConnectorBuilderTestRead();

  return (
    <>
      <Tooltip
        control={
          <Button
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
        <AdvancedTestSettingsModal
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
      )}
    </>
  );
};

const AdvancedTestSettingsModal: React.FC<
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
  const {
    register,
    handleSubmit,
    reset,
    formState: { errors, isValid },
    watch,
    setValue,
  } = useForm({
    defaultValues: {
      recordLimit,
      pageLimit,
      sliceLimit,
      testState,
    },
    resolver: yupResolver(testReadLimitsValidation),
  });
  const { formatMessage } = useIntl();

  return (
    <Modal
      size="md"
      onCancel={() => setIsOpen(false)}
      title={formatMessage({ id: "connectorBuilder.testReadSettings.modalTitle" })}
    >
      <form
        onSubmit={handleSubmit((data) => {
          setRecordLimit(data.recordLimit);
          setSliceLimit(data.sliceLimit);
          setPageLimit(data.pageLimit);
          setTestState(data.testState);
          setIsOpen(false);
        })}
      >
        <ModalBody className={styles.body}>
          <ControlLabels
            label={formatMessage({ id: "connectorBuilder.testReadSettings.recordLimit" })}
            error={Boolean(errors.recordLimit)}
            message={errors.recordLimit?.message}
          >
            {/* Adding step="50" would make this input's arrow buttons useful, but the browser validation is a bad UX */}
            <Input {...register("recordLimit")} type="number" />
          </ControlLabels>
          <ControlLabels
            label={formatMessage({ id: "connectorBuilder.testReadSettings.pageLimit" })}
            error={Boolean(errors.pageLimit)}
            message={errors.pageLimit?.message}
          >
            <Input {...register("pageLimit")} type="number" />
          </ControlLabels>
          <ControlLabels
            label={formatMessage({ id: "connectorBuilder.testReadSettings.sliceLimit" })}
            error={Boolean(errors.sliceLimit)}
            message={errors.sliceLimit?.message}
          >
            <Input {...register("sliceLimit")} type="number" />
          </ControlLabels>
          <ControlLabels
            label={formatMessage({ id: "connectorBuilder.testReadSettings.testState" })}
            error={Boolean(errors.testState)}
            message={errors.testState ? formatMessage({ id: errors.testState.message }) : undefined}
            className={styles.stateEditorContainer}
          >
            <CodeEditor
              value={watch("testState")}
              language="json"
              onChange={(val: string | undefined) => {
                setValue("testState", val ?? "", {
                  shouldValidate: true,
                  shouldDirty: true,
                  shouldTouch: true,
                });
              }}
            />
          </ControlLabels>
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
                reset({ recordLimit, pageLimit, sliceLimit });
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
      </form>
    </Modal>
  );
};
