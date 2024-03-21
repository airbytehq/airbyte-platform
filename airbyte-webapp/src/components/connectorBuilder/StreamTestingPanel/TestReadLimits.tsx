import { yupResolver } from "@hookform/resolvers/yup";
import { useForm } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import * as yup from "yup";

import { ControlLabels } from "components/LabeledControl";
import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Input } from "components/ui/Input";
import { Modal, ModalBody, ModalFooter } from "components/ui/Modal";
import { Tooltip } from "components/ui/Tooltip";

import { TestReadContext, useConnectorBuilderTestRead } from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./TestReadLimits.module.scss";

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
});

interface TestReadLimitsProps {
  isOpen: boolean;
  setIsOpen: (open: boolean) => void;
}

export const TestReadLimits: React.FC<TestReadLimitsProps> = ({ isOpen, setIsOpen }) => {
  const {
    testReadLimits: { recordLimit, setRecordLimit, pageLimit, setPageLimit, sliceLimit, setSliceLimit, defaultLimits },
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
        <TestReadLimitsModal
          defaultLimits={defaultLimits}
          recordLimit={recordLimit}
          setRecordLimit={setRecordLimit}
          pageLimit={pageLimit}
          setPageLimit={setPageLimit}
          sliceLimit={sliceLimit}
          setSliceLimit={setSliceLimit}
          setIsOpen={setIsOpen}
        />
      )}
    </>
  );
};

const TestReadLimitsModal: React.FC<Pick<TestReadLimitsProps, "setIsOpen"> & TestReadContext["testReadLimits"]> = ({
  defaultLimits,
  recordLimit,
  setRecordLimit,
  pageLimit,
  setPageLimit,
  sliceLimit,
  setSliceLimit,
  setIsOpen,
}) => {
  const {
    register,
    handleSubmit,
    reset,
    formState: { isDirty, errors },
  } = useForm({
    defaultValues: {
      recordLimit,
      pageLimit,
      sliceLimit,
    },
    resolver: yupResolver(testReadLimitsValidation),
  });
  const { formatMessage } = useIntl();

  return (
    <Modal
      size="sm"
      onCancel={() => setIsOpen(false)}
      title={formatMessage({ id: "connectorBuilder.testReadSettings.modalTitle" })}
    >
      <form
        onSubmit={handleSubmit((data) => {
          setRecordLimit(data.recordLimit);
          setSliceLimit(data.sliceLimit);
          setPageLimit(data.pageLimit);
          setIsOpen(false);
        })}
      >
        <ModalBody>
          <div>
            <ControlLabels
              label={formatMessage({ id: "connectorBuilder.testReadSettings.recordLimit" })}
              error={Boolean(errors.recordLimit)}
              message={errors.recordLimit?.message}
              className={styles.input}
            >
              {/* Adding step="50" would make this input's arrow buttons useful, but the browser validation is a bad UX */}
              <Input {...register("recordLimit")} type="number" />
            </ControlLabels>
            <ControlLabels
              label={formatMessage({ id: "connectorBuilder.testReadSettings.pageLimit" })}
              error={Boolean(errors.pageLimit)}
              message={errors.pageLimit?.message}
              className={styles.input}
            >
              <Input {...register("pageLimit")} type="number" />
            </ControlLabels>
            <ControlLabels
              label={formatMessage({ id: "connectorBuilder.testReadSettings.sliceLimit" })}
              error={Boolean(errors.sliceLimit)}
              message={errors.sliceLimit?.message}
              className={styles.input}
            >
              <Input {...register("sliceLimit")} type="number" />
            </ControlLabels>
          </div>
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
            <Button type="submit" disabled={!isDirty}>
              <FormattedMessage id="form.saveChanges" />
            </Button>
          </FlexContainer>
        </ModalFooter>
      </form>
    </Modal>
  );
};
