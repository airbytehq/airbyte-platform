import { Meta, StoryFn } from "@storybook/react";

import { Modal } from "components/ui/Modal";

import { CloudSubscriptionSuccessModal } from "./CloudSubscriptionSuccessModal";

export default {
  title: "Modals/CloudSubscriptionSuccessModal",
  component: CloudSubscriptionSuccessModal,
} as Meta<typeof CloudSubscriptionSuccessModal>;

const Template: StoryFn<typeof CloudSubscriptionSuccessModal> = (args) => (
  <Modal size="sm" title="Thanks for subscribing to Cloud!">
    <CloudSubscriptionSuccessModal {...args} />
  </Modal>
);

export const Default = Template.bind({});
Default.args = {
  onComplete: () => {
    console.log("Modal completed");
    alert("User clicked 'Got it'");
  },
};
