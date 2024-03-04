import { FormattedMessage, useIntl } from "react-intl";
import * as yup from "yup";

import { Form, FormControl } from "components/forms";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { Icon } from "components/ui/Icon";
import { ModalFooter } from "components/ui/Modal";
import { Tooltip } from "components/ui/Tooltip";

import { useCreateApplication, useListApplications } from "core/api";
import { ApplicationCreate } from "core/api/types/AirbyteClient";
import { useModalService } from "hooks/services/Modal";

export const CreateApplicationControl = () => {
  const { formatMessage } = useIntl();
  const { mutateAsync: createApplication } = useCreateApplication();
  const { applications } = useListApplications();
  const { openModal, closeModal } = useModalService();

  const schema = yup.object().shape({
    name: yup.string().required("form.empty.error"),
  });

  const onCreateApplicationSubmission = async (values: ApplicationCreate) => {
    await createApplication(values);
    closeModal();
  };

  const onAddApplicationButtonClick = async () => {
    openModal({
      title: formatMessage({ id: "settings.application.create" }),
      content: () => (
        <Form<ApplicationCreate> schema={schema} defaultValues={{ name: "" }} onSubmit={onCreateApplicationSubmission}>
          <Box px="xl" py="md">
            <FormControl
              fieldType="input"
              label={formatMessage({ id: "settings.application.name" })}
              name="name"
              placeholder={formatMessage({ id: "settings.application.name.placeholder" })}
            />
          </Box>
          <ModalFooter>
            <FormSubmissionButtons />
          </ModalFooter>
        </Form>
      ),
      size: "md",
    });
  };

  return (
    <>
      {applications.length === 2 ? (
        <Tooltip
          control={
            <Button icon={<Icon type="plus" />} onClick={onAddApplicationButtonClick} variant="primary" disabled>
              <FormattedMessage id="settings.application.create" />
            </Button>
          }
        >
          <FormattedMessage id="settings.applications.create.disabledTooltip" />
        </Tooltip>
      ) : (
        <Button
          icon={<Icon type="plus" />}
          onClick={onAddApplicationButtonClick}
          variant="primary"
          disabled={applications.length >= 2}
        >
          <FormattedMessage id="settings.application.create" />
        </Button>
      )}
    </>
  );
};
