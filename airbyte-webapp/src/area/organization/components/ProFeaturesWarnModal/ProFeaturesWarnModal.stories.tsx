import { Meta, StoryFn } from "@storybook/react";

import { Modal } from "components/ui/Modal";

import { ProFeaturesWarnModal } from "./ProFeaturesWarnModal";

export default {
  title: "Modals/ProFeaturesWarnModal",
  component: ProFeaturesWarnModal,
  argTypes: {
    variant: {
      control: { type: "radio" },
      options: ["warning", "upgrade"],
    },
  },
} as Meta<typeof ProFeaturesWarnModal>;

const Template: StoryFn<typeof ProFeaturesWarnModal> = (args) => (
  <Modal size="xl" title="">
    <ProFeaturesWarnModal {...args} />
  </Modal>
);

export const WarningVariant = Template.bind({});
WarningVariant.args = {
  variant: "warning",
  onContinue: () => {
    console.log("User clicked Continue button");
  },
};
WarningVariant.storyName = "Warning - In Trial";

export const UpgradeVariant = Template.bind({});
UpgradeVariant.args = {
  variant: "upgrade",
  onContinue: () => {
    console.log("User clicked button (Talk to sales or No thanks)");
  },
};
UpgradeVariant.storyName = "Upgrade - Post Trial / Standard Plan";
