import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useFormContext } from "react-hook-form";
import * as yup from "yup";

import { Form } from "components/ui/forms";
import { render } from "test-utils";
import { mockSourceDefinition, mockSourceDefinitionSpecification } from "test-utils/mock-data/mockSource";
import { mockWorkspace } from "test-utils/mock-data/mockWorkspace";

import { ConnectorTitleBlock } from "area/connector/components/ConnectorTitleBlock";
import { ActorDefinitionVersionRead, SourceRead } from "core/api/types/AirbyteClient";

import { FormCopyConfigButton, ShareFormConfigJson } from "./CopyConfigButton";
import { ConnectorFormContextProvider } from "../../ConnectorForm/connectorFormContext";
import { ConnectorFormValues } from "../../ConnectorForm/types";
import { CopyConfigProvider } from "../CopyConfigContext";

jest.mock("core/api", () => ({ useCurrentWorkspace: () => mockWorkspace }));

jest.mock("area/connector/components/ConnectorIcon", () => ({ ConnectorIcon: () => null }));
jest.mock("pages/connectorBuilder/components/ForkConnectorButton", () => ({ ForkConnectorButton: () => null }));

const connectionSpecification = {
  type: "object",
  properties: { host: { type: "string" }, password: { type: "string", airbyte_secret: true } },
};
const specification = { ...mockSourceDefinitionSpecification, connectionSpecification };
const values = { name: "My source", connectionConfiguration: { host: "saved-host", password: "hunter2" } };
const schema = yup.object({
  name: yup.string(),
  connectionConfiguration: yup.object({ host: yup.string(), password: yup.string() }),
});

const EditHost: React.FC = () => {
  const { setValue } = useFormContext<ConnectorFormValues>();
  return (
    <button type="button" onClick={() => setValue("connectionConfiguration", { host: "edited", password: "hunter2" })}>
      Edit host
    </button>
  );
};

const TestForm: React.FC<React.PropsWithChildren> = ({ children }) => (
  <Form defaultValues={values} schema={schema}>
    <ConnectorFormContextProvider
      formType="source"
      castValues={(formValues) => formValues}
      selectedConnectorDefinition={mockSourceDefinition}
      selectedConnectorDefinitionSpecification={specification}
      validationSchema={schema}
    >
      <EditHost />
      {children}
    </ConnectorFormContextProvider>
  </Form>
);

const copiedJson = () => JSON.parse((navigator.clipboard.writeText as jest.Mock).mock.calls[0][0]);
const clickCopy = () => userEvent.click(screen.getByRole("button", { name: "Copy JSON" }));

const EDITED_AND_MASKED_JSON = {
  name: "My source",
  workspaceId: mockWorkspace.workspaceId,
  definitionId: mockSourceDefinition.sourceDefinitionId,
  configuration: { host: "edited", password: "******" },
};

describe("Copy JSON", () => {
  beforeEach(() => {
    Object.defineProperty(navigator, "clipboard", {
      value: { writeText: jest.fn().mockResolvedValue(undefined) },
      writable: true,
      configurable: true,
    });
  });

  it("copies the unsaved form values with secrets masked", async () => {
    await render(
      <TestForm>
        <FormCopyConfigButton />
      </TestForm>
    );

    await userEvent.click(screen.getByRole("button", { name: "Edit host" }));
    await clickCopy();

    expect(copiedJson()).toEqual(EDITED_AND_MASKED_JSON);
  });

  it("shows in the title block only while a connector form is on the page, and copies that form's values", async () => {
    const titleBlock = (
      <ConnectorTitleBlock
        connector={{ name: "My source", icon: "" } as SourceRead}
        connectorDefinition={mockSourceDefinition}
        actorDefinitionVersion={{ dockerImageTag: "1.0.0", supportLevel: "certified" } as ActorDefinitionVersionRead}
      />
    );
    const page = (showForm: boolean) => (
      <CopyConfigProvider>
        {titleBlock}
        {showForm && (
          <TestForm>
            <ShareFormConfigJson />
          </TestForm>
        )}
      </CopyConfigProvider>
    );

    const { rerender } = await render(page(true));

    await userEvent.click(screen.getByRole("button", { name: "Edit host" }));
    await clickCopy();
    expect(copiedJson()).toEqual(EDITED_AND_MASKED_JSON);

    rerender(page(false));
    await waitFor(() => expect(screen.queryByRole("button", { name: "Copy JSON" })).not.toBeInTheDocument());
  });
});
