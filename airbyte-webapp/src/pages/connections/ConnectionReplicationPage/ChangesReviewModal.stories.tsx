import { ComponentMeta, ComponentStory } from "@storybook/react";
import { FormattedMessage } from "react-intl";

import { Modal } from "components/ui/Modal";

import { ModalServiceProvider } from "core/services/Modal";

import { ChangesReviewModal } from "./ChangesReviewModal";

export default {
  title: "connection/ChangesReviewModal",
  component: ChangesReviewModal,
} as ComponentMeta<typeof ChangesReviewModal>;

const Template: ComponentStory<typeof ChangesReviewModal> = (args) => {
  return (
    <ModalServiceProvider>
      <Modal size="md" title={<FormattedMessage id="connection.reviewChanges.title" />}>
        <ChangesReviewModal
          changes={args.changes}
          onCancel={() => alert("Cancelled")}
          onContinue={(decisions) => alert(`Continued with decisions: ${JSON.stringify(decisions)}`)}
        />
      </Modal>
    </ModalServiceProvider>
  );
};

const singleStreamFullRefresh = {
  fullRefreshHighFrequency: [
    {
      id: "1",
      streamName: "users_table",
    },
  ],
};

const multipleStreamsFullRefresh = {
  fullRefreshHighFrequency: [
    {
      id: "1",
      streamName: "users_table",
    },
    {
      id: "2",
      streamName: "orders_table",
    },
    {
      id: "3",
      streamName: "products_table",
    },
  ],
};

const clearChanges = {
  fullRefreshHighFrequency: [
    {
      id: "1",
      streamName: "users_table",
    },
    {
      id: "2",
      streamName: "orders_table",
    },
  ],
  clear: [
    {
      id: "4",
      streamName: "categories_table",
    },
    {
      id: "5",
      streamName: "tags_table",
    },
  ],
};
const refreshChanges = {
  fullRefreshHighFrequency: [
    {
      id: "1",
      streamName: "users_table",
    },
    {
      id: "2",
      streamName: "orders_table",
    },
  ],
  refresh: [
    {
      id: "5",
      streamName: "tags_table",
    },
  ],
};

export const SingleStreamWarning = Template.bind({});
SingleStreamWarning.args = {
  changes: singleStreamFullRefresh,
};

export const MultipleStreamsWarning = Template.bind({});
MultipleStreamsWarning.args = {
  changes: multipleStreamsFullRefresh,
};

export const ClearWarning = Template.bind({});
ClearWarning.args = {
  changes: clearChanges,
};
export const RefreshWarning = Template.bind({});
RefreshWarning.args = {
  changes: refreshChanges,
};
