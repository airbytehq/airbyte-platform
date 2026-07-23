import { ComponentStory, ComponentMeta } from "@storybook/react";

import { JobFailure } from "./JobFailure";

export default {
  title: "Common/JobFailure",
  component: JobFailure,
  argTypes: {
    text: { type: { name: "string", required: false } },
    type: { type: { name: "string", required: false } },
    onAction: { table: { disable: true } },
    actionBtnText: { type: { name: "string", required: false } },
    onClose: { table: { disable: true } },
  },
} as ComponentMeta<typeof JobFailure>;

const Template: ComponentStory<typeof JobFailure> = (args) => <JobFailure {...args} />;

const jobBasics = {
  id: "123",
  createdAt: 123,
  endedAt: 456,
  succeeded: false,
};

const failureReasonBasics = {
  timestamp: 123,
};

export const Basic = Template.bind({});
Basic.args = {
  job: {
    ...jobBasics,
    configType: "check_connection_source",
  },
};

export const WithLogs = Template.bind({});
WithLogs.args = {
  job: {
    ...jobBasics,
    configType: "check_connection_source",
    logs: {
      logLines: ["line 1", "line 2", "line 3"],
    },
  },
};

export const WithLogsAndExternalMessage = Template.bind({});
WithLogsAndExternalMessage.args = {
  job: {
    ...jobBasics,
    configType: "check_connection_source",
    failureReason: {
      ...failureReasonBasics,
      externalMessage: "The problem is such and such",
    },
    logs: {
      logLines: ["line 1", "line 2", "line 3"],
    },
  },
};

export const EverythingSet = Template.bind({});
EverythingSet.args = {
  job: {
    ...jobBasics,
    configType: "check_connection_source",
    failureReason: {
      ...failureReasonBasics,
      externalMessage: "The problem is such and such",
      internalMessage: "Actually, it was something more complicated",
      failureOrigin: "source",
      failureType: "system_error",
      stacktrace: `Traceback (most recent call last):
  File "/airbyte/integration_code/main.py", line 13, in <module>
    launch(source, sys.argv[1:])
  File "/usr/local/lib/python3.9/site-packages/airbyte_cdk/entrypoint.py", line 131, in launch
    for message in source_entrypoint.run(parsed_args):
  File "/usr/local/lib/python3.9/site-packages/airbyte_cdk/entrypoint.py", line 116, in run
    catalog = self.source.discover(self.logger, config)
  File "/airbyte/integration_code/source_breaker/source.py", line 72, in discover
    raise Exception("Test Exception")
Exception: Test Exception`,
    },
    logs: {
      logLines: ["line 1", "line 2", "line 3"],
    },
  },
};
