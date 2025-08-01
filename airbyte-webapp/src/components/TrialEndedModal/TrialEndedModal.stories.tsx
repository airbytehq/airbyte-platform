import { Meta, StoryFn } from "@storybook/react";

import { Modal } from "components/ui/Modal";

import { TrialEndedModal } from "./TrialEndedModal";

export default {
  title: "Modals/TrialEndedModal",
  component: TrialEndedModal,
} as Meta<typeof TrialEndedModal>;

const Template: StoryFn<typeof TrialEndedModal> = (args) => (
  <Modal size="md" title="">
    <TrialEndedModal {...args} />
  </Modal>
);

export const Default = Template.bind({});
Default.args = {
  onComplete: (result) => {
    console.log("Modal completed with result:", result);
    alert(`User selected: ${result.action}`);
  },
};
