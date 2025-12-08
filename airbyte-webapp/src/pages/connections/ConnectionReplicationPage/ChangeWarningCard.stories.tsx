import { ComponentMeta, ComponentStory } from "@storybook/react";

import { ChangeWarningCard } from "./ChangeWarningCard";
import { ChangeWarning } from "./connectionUpdateHelpers";

export default {
  title: "connection/ChangeWarningCard",
  component: ChangeWarningCard,
} as ComponentMeta<typeof ChangeWarningCard>;

const Template: ComponentStory<typeof ChangeWarningCard> = (args) => (
  <ChangeWarningCard
    warning={args.warning}
    affectedStreams={args.affectedStreams}
    decision={args.decision}
    onDecision={args.onDecision}
  />
);

const singleStream = [
  {
    id: "1",
    streamName: "users",
  },
];

const multipleStreams = [
  {
    id: "1",
    streamName: "users",
  },
  {
    id: "2",
    streamName: "orders",
  },
  {
    id: "3",
    streamName: "products",
  },
  {
    id: "4",
    streamName: "categories",
  },
];

export const FullRefreshHighFrequencySingle = Template.bind({});
FullRefreshHighFrequencySingle.args = {
  warning: "fullRefreshHighFrequency" as ChangeWarning,
  affectedStreams: singleStream,
  decision: "accept" as const,
  onDecision: (decision) => console.log("Decision:", decision),
};

export const FullRefreshHighFrequencyMultiple = Template.bind({});
FullRefreshHighFrequencyMultiple.args = {
  warning: "fullRefreshHighFrequency" as ChangeWarning,
  affectedStreams: multipleStreams,
  decision: "accept" as const,
  onDecision: (decision) => console.log("Decision:", decision),
};

export const FullRefreshHighFrequencyReject = Template.bind({});
FullRefreshHighFrequencyReject.args = {
  warning: "fullRefreshHighFrequency" as ChangeWarning,
  affectedStreams: singleStream,
  decision: "reject" as const,
  onDecision: (decision) => console.log("Decision:", decision),
};

export const RefreshSingle = Template.bind({});
RefreshSingle.args = {
  warning: "refresh" as ChangeWarning,
  affectedStreams: singleStream,
  decision: "accept" as const,
  onDecision: (decision) => console.log("Decision:", decision),
};

export const RefreshMultiple = Template.bind({});
RefreshMultiple.args = {
  warning: "refresh" as ChangeWarning,
  affectedStreams: multipleStreams,
  decision: "accept" as const,
  onDecision: (decision) => console.log("Decision:", decision),
};

export const ClearSingle = Template.bind({});
ClearSingle.args = {
  warning: "clear" as ChangeWarning,
  affectedStreams: singleStream,
  decision: "accept" as const,
  onDecision: (decision) => console.log("Decision:", decision),
};

export const ClearMultiple = Template.bind({});
ClearMultiple.args = {
  warning: "clear" as ChangeWarning,
  affectedStreams: multipleStreams,
  decision: "reject" as const,
  onDecision: (decision) => console.log("Decision:", decision),
};

export const EmptyStreams = Template.bind({});
EmptyStreams.args = {
  warning: "fullRefreshHighFrequency" as ChangeWarning,
  affectedStreams: [],
  decision: "accept" as const,
  onDecision: (decision) => console.log("Decision:", decision),
};
