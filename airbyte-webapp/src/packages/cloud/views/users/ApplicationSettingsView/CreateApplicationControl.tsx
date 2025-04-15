import { FormattedMessage, useIntl } from "react-intl";
import { z } from "zod";

import { Form, FormControl } from "components/forms";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { ModalFooter } from "components/ui/Modal";
import { Tooltip } from "components/ui/Tooltip";

import { useCreateApplication, useListApplications } from "core/api";
import { ApplicationCreate } from "core/api/types/AirbyteClient";
import { useModalService } from "hooks/services/Modal";

export const CreateApplicationControl = () => {
  const { formatMessage } = useIntl();
  const { mutateAsync: createApplication } = useCreateApplication();
  const { applications } = useListApplications();
  const { openModal } = useModalService();

  const schema = z.object({
    name: z.string().nonempty("form.empty.error"),
  });

  const onAddApplicationButtonClick = () =>
    openModal<void>({
      title: formatMessage({ id: "settings.application.create" }),
      content: ({ onComplete, onCancel }) => (
        <Form<ApplicationCreate>
          zodSchema={schema}
          defaultValues={{ name: "" }}
          onSubmit={async (values: ApplicationCreate) => {
            await createApplication(values);
            onComplete();
          }}
        >
          <Box px="xl" py="md">
            <FormControl
              fieldType="input"
              label={formatMessage({ id: "settings.application.name" })}
              name="name"
              placeholder={formatMessage({ id: "settings.application.name.placeholder" })}
            />
          </Box>
          <ModalFooter>
            <FormSubmissionButtons allowNonDirtyCancel onCancelClickCallback={onCancel} />
          </ModalFooter>
        </Form>
      ),
      size: "md",
    });

  return (
    <>
      {applications.length === 2 ? (
        <Tooltip
          control={
            <Button icon="plus" onClick={onAddApplicationButtonClick} variant="primary" disabled>
              <FormattedMessage id="settings.application.create" />
            </Button>
          }
        >
          <FormattedMessage id="settings.applications.create.disabledTooltip" />
        </Tooltip>
      ) : (
        <Button icon="plus" onClick={onAddApplicationButtonClick} variant="primary" disabled={applications.length >= 2}>
          <FormattedMessage id="settings.application.create" />
        </Button>
      )}
    </>
  );
};
