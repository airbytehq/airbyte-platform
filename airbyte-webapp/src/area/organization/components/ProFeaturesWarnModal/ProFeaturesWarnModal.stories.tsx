import { Meta, StoryFn } from "@storybook/react";

import { Modal } from "components/ui/Modal";

import { ProFeaturesWarnModal } from "./ProFeaturesWarnModal";

export default {
  title: "Modals/ProFeaturesWarnModal",
  component: ProFeaturesWarnModal,
} as Meta<typeof ProFeaturesWarnModal>;

const Template: StoryFn<typeof ProFeaturesWarnModal> = (args) => (
  <Modal size="xl" title="">
    <ProFeaturesWarnModal {...args} />
  </Modal>
);

export const UpgradeVariant = Template.bind({});
UpgradeVariant.args = {
  onContinue: () => {
    console.log("User clicked button (Talk to sales or No thanks)");
  },
};
UpgradeVariant.storyName = "Upgrade - Post Trial / Standard Plan";
