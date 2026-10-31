/* eslint-disable @typescript-eslint/no-non-null-assertion */

import {
  getAllByRole,
  getByTestId,
  screen,
  waitFor,
  getByRole,
  getByText,
  fireEvent,
  queryByTestId,
} from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { BroadcastChannel } from "broadcast-channel";
import React from "react";
import { useFormContext } from "react-hook-form";

import { mockWebappConfig } from "test-utils/mock-data/mockWebappConfig";
import { mockExperiments } from "test-utils/mockExperiments";
import { render, useMockIntersectionObserver } from "test-utils/testutils";

import { Controls } from "area/connector/components/ConnectorCard/components/Controls";
import { ConnectorDocumentationLayout } from "area/connector/components/ConnectorDocumentationLayout/ConnectorDocumentationLayout";
import { ConnectorForm, ConnectorFormProps } from "area/connector/components/ConnectorForm";
import { OAUTH_BROADCAST_CHANNEL_NAME } from "area/connector/utils/oauthConstants";
import { CommandErrorWithJobInfo, useCompleteOAuth } from "core/api";
import { DestinationDefinitionSpecificationRead, JobConfigType, OAuthConsentRead } from "core/api/types/AirbyteClient";
import { ConnectorDefinition, ConnectorDefinitionSpecificationRead } from "core/domain/connector";
import { AirbyteJSONSchema } from "core/jsonSchema/types";
import { FeatureItem } from "core/services/features";

import { useConnectorForm } from "./connectorFormContext";
import { ConnectorFormValues } from "./types";
import { DocumentationPanelContext } from "../ConnectorDocumentationLayout/DocumentationPanelContext";

// hack to fix tests. https://github.com/remarkjs/react-markdown/issues/635
jest.mock("components/ui/Markdown", () => ({ children }: React.PropsWithChildren<unknown>) => <>{children}</>);

jest.mock("core/api", () => ({
  CommandErrorWithJobInfo: jest.requireActual("core/api/errors").CommandErrorWithJobInfo,
  useDestinationList: () => ({ destinations: [] }),
  useConsentUrls: () => ({
    getSourceConsentUrl: (): OAuthConsentRead => ({
      consentUrl: "http://example.com",
    }),
  }),
  useCompleteOAuth: jest.fn(() => ({
    completeSourceOAuth: () => Promise.resolve({}),
    completeDestinationOAuth: () => Promise.resolve({}),
  })),
  useGetWebappConfig: () => mockWebappConfig,
}));

jest.mock("../ConnectorDocumentationLayout/DocumentationPanelContext", () => {
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  const emptyFn = () => {};

  const useDocumentationPanelContext: () => DocumentationPanelContext = () => ({
    documentationPanelOpen: false,
    documentationUrl: "",
    setDocumentationPanelOpen: emptyFn,
    selectedConnectorDefinition: {} as ConnectorDefinition,
    setSelectedConnectorDefinition: emptyFn,
    focusedField: undefined,
    setFocusedField: emptyFn,
  });

  return {
    useDocumentationPanelContext,
    useOptionalDocumentationPanelContext: useDocumentationPanelContext,
  };
});

jest.setTimeout(40000);

const nextTick = () => new Promise((r) => setTimeout(r, 0));

const connectorDefinition = {
  sourceDefinitionId: "1",
  documentationUrl: "",
} as ConnectorDefinition;

const selectDropdownOption = (container: HTMLElement, dropdownContainerTestId: string, option: string) => {
  const dropdownControlButton = getByTestId(container, `${dropdownContainerTestId}-listbox-button`);
  fireEvent.click(dropdownControlButton);

  const dropdownOptionsMenu = screen.getByTestId(`${dropdownContainerTestId}-listbox-options`);

  const listBoxOption = getByRole(dropdownOptionsMenu, "option", { name: option });
  fireEvent.click(listBoxOption);
};

const selectSegmentedOption = async (container: HTMLElement, segmentedControlTestId: string, option: string) => {
  const segmentedControl = getByTestId(container, segmentedControlTestId);
  await userEvent.click(getByRole(segmentedControl, "radio", { name: option }));
};

const buildOneOfField = (optionCount: number, overrides: AirbyteJSONSchema = {}): AirbyteJSONSchema => ({
  type: "object",
  title: "Choice",
  ...overrides,
  oneOf: Array.from({ length: optionCount }, (_, index) => ({
    title: `choice ${index}`,
    required: [`field_${index}`],
    properties: {
      type: { type: "string", const: `type_${index}`, default: `type_${index}` },
      [`field_${index}`]: { type: "string" },
    },
  })),
});

const useAddPriceListItem = (container: HTMLElement, initialIndex = 0) => {
  const priceList = getByTestId(container, "connectionConfiguration.priceList");
  let index = initialIndex;

  return async (name: string, price: string, originType: "city" | "country", origin: string) => {
    const addButton = getByTestId(priceList, "addItemButton");
    await userEvent.click(addButton);

    const getPriceListInput = (index: number, key: string) =>
      priceList.querySelector<HTMLInputElement>(`input[name='connectionConfiguration.priceList.${index}.${key}']`);

    // Type items into input
    const nameInput = getPriceListInput(index, "name");
    await userEvent.type(nameInput!, name);
    await waitFor(() => {
      expect(nameInput?.value).toEqual(name);
    });

    const priceInput = getPriceListInput(index, "price");
    await userEvent.type(priceInput!, price);
    await waitFor(() => {
      expect(priceInput?.value).toEqual(price);
    });

    await selectSegmentedOption(priceList, `connectionConfiguration.priceList.${index}.origin`, originType);

    const originInput = priceList.querySelector(
      `input[name='connectionConfiguration.priceList.${index}.origin.${originType}']`
    );
    await userEvent.type(originInput!, origin);

    index++;
  };
};

function getOAuthButton(container: HTMLElement) {
  return container.querySelector("[data-testid='oauth-button']");
}

function getSubmitButton(container: HTMLElement) {
  return container.querySelector("[type='submit']");
}

function getInputByName(container: HTMLElement, name: string) {
  return container.querySelector(`input[name='${name}']`);
}

async function executeOAuthFlow(container: HTMLElement) {
  const oAuthButton = getOAuthButton(container);
  await waitFor(() => userEvent.click(oAuthButton!));
  // wait for the mocked consent url call to finish
  await waitFor(nextTick);
  // mock the message coming from the separate oauth window
  const bc = new BroadcastChannel(OAUTH_BROADCAST_CHANNEL_NAME);
  bc.postMessage({
    type: "completed",
    query: {},
  });
  // mock the complete oauth request
  await waitFor(nextTick);
}

async function submitForm(container: HTMLElement) {
  const submit = getSubmitButton(container);
  await waitFor(() => userEvent.click(submit!));
}

const schema: AirbyteJSONSchema = {
  type: "object",
  required: ["host", "port", "password", "credentials", "message", "priceList", "emails", "workTime"],
  properties: {
    host: {
      type: "string",
      description: "Hostname of the database.",
      title: "Host",
    },
    port: {
      title: "Port",
      type: "integer",
      description: "Port of the database.",
    },
    password: {
      title: "Password",
      airbyte_secret: true,
      type: "string",
      description: "Password associated with the username.",
    },
    credentials: {
      type: "object",
      oneOf: [
        {
          title: "api key",
          required: ["api_key"],
          properties: {
            api_key: {
              type: "string",
            },
            type: {
              type: "string",
              const: "api",
              default: "api",
            },
          },
        },
        {
          title: "oauth",
          required: ["redirect_uri"],
          properties: {
            redirect_uri: {
              type: "string",
              examples: ["https://api.hubspot.com/"],
            },
            type: {
              type: "string",
              const: "oauth",
              default: "oauth",
            },
          },
        },
      ],
    },
    message: {
      type: "string",
      multiline: true,
      title: "Message",
    },
    priceList: {
      type: "array",
      items: {
        type: "object",
        required: ["name", "price", "origin"],
        properties: {
          name: {
            type: "string",
            title: "Product name",
          },
          price: {
            type: "integer",
            title: "Price ($)",
          },
          origin: {
            type: "object",
            oneOf: [
              {
                type: "object",
                title: "city",
                required: ["type", "city"],
                properties: {
                  type: {
                    type: "string",
                    const: "city",
                    default: "city",
                  },
                  city: {
                    type: "string",
                  },
                },
              },
              {
                type: "object",
                title: "country",
                required: ["type", "country"],
                properties: {
                  type: {
                    type: "string",
                    const: "country",
                    default: "country",
                  },
                  country: {
                    type: "string",
                  },
                },
              },
            ],
          },
        },
      },
    },
    emails: {
      type: "array",
      items: {
        type: "string",
      },
    },
    workTime: {
      type: "array",
      title: "Work time",
      items: {
        type: "string",
        enum: ["day", "night"],
      },
    },
  },
};

jest.mock("area/workspace/utils/useWorkspace", () => ({
  useCurrentWorkspace: () => ({
    workspace: {
      workspaceId: "workspaceId",
    },
  }),
}));

describe("Connector form", () => {
  let result: ConnectorFormValues | undefined;

  async function renderForm({
    formValuesOverride,
    propertiesOverride,
    specificationOverride,
    features,
    formAutomation,
    renderFooter = () => <button type="submit">Submit</button>,
    onCheckSection,
    title,
    description,
    onSubmit = async (values) => {
      result = values;
    },
  }: {
    formValuesOverride?: Record<string, unknown>;
    specificationOverride?: Partial<ConnectorDefinitionSpecificationRead>;
    propertiesOverride?: Record<string, AirbyteJSONSchema>;
    features?: FeatureItem[];
    formAutomation?: React.ReactNode;
    renderFooter?: ConnectorFormProps["renderFooter"];
    onCheckSection?: (values: ConnectorFormValues) => Promise<void>;
    title?: string;
    description?: React.ReactNode;
    onSubmit?: ConnectorFormProps["onSubmit"];
  } = {}) {
    const renderResult = await render(
      <ConnectorForm
        canEdit
        formType="source"
        formValues={{ name: "test-name", connectionConfiguration: { ...formValuesOverride } }}
        onSubmit={onSubmit}
        isEditMode={Boolean(formValuesOverride)}
        renderFooter={renderFooter}
        formAutomation={formAutomation}
        onCheckSection={onCheckSection}
        title={title}
        description={description}
        selectedConnectorDefinition={connectorDefinition}
        selectedConnectorDefinitionSpecification={
          {
            sourceDefinitionId: "test-service-type",
            documentationUrl: "",
            connectionSpecification: {
              ...schema,
              properties: {
                ...schema.properties,
                ...propertiesOverride,
              },
            },
            ...specificationOverride,
          } as unknown as DestinationDefinitionSpecificationRead
        }
      />,
      undefined,
      features ? [...features] : undefined
    );
    return renderResult.container;
  }

  beforeEach(async () => {
    result = undefined;
    mockExperiments({ "connector.sectionedSetupForm": false });
  });

  beforeAll(() => {
    // mock window.open because jsdom is not doing that
    window.open = jest.fn();
    // Rewrite message event because jsdom is not setting the origin
    window.addEventListener("message", (event: MessageEvent) => {
      if (event.origin === "") {
        event.stopImmediatePropagation();
        const eventWithOrigin: MessageEvent = new MessageEvent("message", {
          data: event.data,
          origin: "http://localhost",
        });
        window.dispatchEvent(eventWithOrigin);
      }
    });
    // IntersectionObserver isn't available in test environment but is used by headless-ui dialog
    // used for this component
    useMockIntersectionObserver();
  });

  it("renders formAutomation inside the form and connector form contexts", async () => {
    const FormAutomationProbe = () => {
      const { getValues } = useFormContext<ConnectorFormValues>();
      const { formType } = useConnectorForm();
      return <span data-testid="form-automation">{`${formType}:${getValues("name")}`}</span>;
    };

    await renderForm({ formAutomation: <FormAutomationProbe /> });

    expect(screen.getByTestId("form-automation")).toHaveTextContent("source:test-name");
  });

  it("submits from the footer buttons in the layout's footer slot, outside the form element", async () => {
    const onSubmit = jest.fn().mockResolvedValue(undefined);
    await render(
      <ConnectorDocumentationLayout>
        <ConnectorForm
          canEdit
          formType="source"
          formValues={{ name: "test-name" }}
          onSubmit={onSubmit}
          selectedConnectorDefinition={connectorDefinition}
          selectedConnectorDefinitionSpecification={
            {
              sourceDefinitionId: "test-service-type",
              documentationUrl: "",
              connectionSpecification: { type: "object", properties: {} },
            } as unknown as DestinationDefinitionSpecificationRead
          }
          renderFooter={({ dirty, isSubmitting, isValid, emptyRequiredFields, formElementId, canEdit }) => (
            <Controls
              formType="source"
              isSubmitting={isSubmitting}
              isValid={isValid}
              emptyRequiredFields={emptyRequiredFields}
              dirty={dirty}
              onCancelClick={jest.fn()}
              onRetestClick={jest.fn()}
              onCancelTesting={jest.fn()}
              connectionTestSuccess={false}
              hasDefinition
              isEditMode={false}
              formElementId={formElementId}
              canEdit={canEdit}
            />
          )}
        />
      </ConnectorDocumentationLayout>
    );

    const submitButton = screen.getByRole("button", { name: "Save & Test" });
    expect(document.querySelector("form")).not.toContainElement(submitButton);

    await userEvent.click(submitButton);

    await waitFor(() => expect(onSubmit).toHaveBeenCalledTimes(1));
  });

  describe("empty required fields", () => {
    const renderFooter: ConnectorFormProps["renderFooter"] = ({ emptyRequiredFields }) => (
      <>
        <button type="submit" disabled={emptyRequiredFields.length > 0}>
          Submit
        </button>
        <output data-testid="empty-required-fields">{emptyRequiredFields.join(" | ")}</output>
      </>
    );
    const emptyRequiredFields = () => screen.getByTestId("empty-required-fields");

    it("lists an empty required field until it has a value, even if the value breaks its pattern", async () => {
      const container = await renderForm({
        renderFooter,
        specificationOverride: {
          connectionSpecification: {
            type: "object",
            required: ["host"],
            properties: { host: { type: "string", title: "Host", pattern: "^[a-z]+$" }, port: { type: "integer" } },
          },
        },
      });
      const hostInput = getInputByName(container, "connectionConfiguration.host")!;

      expect(emptyRequiredFields()).toHaveTextContent("Host");
      expect(screen.getByRole("button", { name: "Submit" })).toBeDisabled();

      await userEvent.type(hostInput, "123");
      await waitFor(() => expect(screen.getByRole("button", { name: "Submit" })).toBeEnabled());
      expect(emptyRequiredFields()).toBeEmptyDOMElement();

      await userEvent.click(screen.getByRole("button", { name: "Submit" }));
      expect(await screen.findByText("The value must match the pattern ^[a-z]+$")).toBeInTheDocument();
      expect(result).toBeUndefined();

      await userEvent.clear(hostInput);
      await waitFor(() => expect(emptyRequiredFields()).toHaveTextContent("Host"));
      expect(screen.getByRole("button", { name: "Submit" })).toBeDisabled();
    });

    it("lists only the required fields of the selected oneOf option", async () => {
      const container = await renderForm({
        renderFooter,
        specificationOverride: {
          connectionSpecification: {
            type: "object",
            required: ["auth"],
            properties: {
              auth: {
                type: "object",
                title: "Authentication",
                oneOf: [
                  {
                    title: "Option A",
                    required: ["a_key"],
                    properties: {
                      mode: { type: "string", const: "a", default: "a" },
                      a_key: { type: "string", title: "A key" },
                    },
                  },
                  {
                    title: "Option B",
                    required: ["b_key"],
                    properties: {
                      mode: { type: "string", const: "b", default: "b" },
                      b_key: { type: "string", title: "B key" },
                    },
                  },
                ],
              },
            },
          },
        },
      });

      expect(emptyRequiredFields()).toHaveTextContent(/^A key$/);

      await selectSegmentedOption(container, "connectionConfiguration.auth", "Option B");

      await waitFor(() => expect(emptyRequiredFields()).toHaveTextContent(/^B key$/));
    });

    it("lists a field that two oneOf options share by the title in the selected option", async () => {
      const container = await renderForm({
        renderFooter,
        specificationOverride: {
          connectionSpecification: {
            type: "object",
            required: ["auth"],
            properties: {
              auth: {
                type: "object",
                title: "Authentication",
                oneOf: [
                  {
                    title: "Option A",
                    required: ["key"],
                    properties: {
                      mode: { type: "string", const: "a", default: "a" },
                      key: { type: "string", title: "API key" },
                    },
                  },
                  {
                    title: "Option B",
                    required: ["key"],
                    properties: {
                      mode: { type: "string", const: "b", default: "b" },
                      key: { type: "string", title: "Access token" },
                    },
                  },
                ],
              },
            },
          },
        },
      });

      await selectSegmentedOption(container, "connectionConfiguration.auth", "Option B");

      await waitFor(() => expect(emptyRequiredFields()).toHaveTextContent(/^Access token$/));
    });

    it("lists an empty required field of a nested object by its own title", async () => {
      await renderForm({
        renderFooter,
        specificationOverride: {
          connectionSpecification: {
            type: "object",
            required: ["database"],
            properties: {
              database: {
                type: "object",
                title: "Database",
                required: ["host"],
                properties: { host: { type: "string", title: "Database host" } },
              },
            },
          },
        },
      });

      expect(emptyRequiredFields()).toHaveTextContent(/^Database host$/);
    });

    it("lists an empty required field of array items once, however many items lack it", async () => {
      await renderForm({
        renderFooter,
        formValuesOverride: { items: [{ price: 1 }, {}] },
        specificationOverride: {
          connectionSpecification: {
            type: "object",
            properties: {
              items: {
                type: "array",
                title: "Items",
                items: {
                  type: "object",
                  required: ["name"],
                  properties: { name: { type: "string", title: "Item name" }, price: { type: "integer" } },
                },
              },
            },
          },
        },
      });

      expect(emptyRequiredFields()).toHaveTextContent(/^Item name$/);
    });
  });

  describe("should display json schema specs", () => {
    let container: HTMLElement;
    beforeEach(async () => {
      container = await renderForm();
    });

    it("should display general components: submit button, name and serviceType fields", () => {
      const name = getInputByName(container, "name");
      const submit = getSubmitButton(container);

      expect(name).toBeInTheDocument();
      expect(submit).toBeInTheDocument();
    });

    it("should display text input field", () => {
      const host = getInputByName(container, "connectionConfiguration.host");
      expect(host).toBeInTheDocument();
      expect(host?.getAttribute("type")).toEqual("text");
    });

    it("should display number input field", () => {
      const port = getInputByName(container, "connectionConfiguration.port");
      expect(port).toBeInTheDocument();
      expect(port?.getAttribute("type")).toEqual("number");
    });

    it("should display secret input field", () => {
      const password = getInputByName(container, "connectionConfiguration.password");
      expect(password).toBeInTheDocument();
      expect(password?.getAttribute("type")).toEqual("password");
    });

    it("should display textarea field", () => {
      const message = container.querySelector("textarea[name='connectionConfiguration.message']");
      expect(message).toBeInTheDocument();
    });

    it("should display oneOf field", () => {
      const credentials = screen.getByRole("radiogroup", { name: "credentials" });
      const apiKey = getInputByName(container, "connectionConfiguration.credentials.api_key");
      expect(credentials).toHaveAttribute("data-testid", "connectionConfiguration.credentials");
      expect(getByRole(credentials, "radio", { name: "api key" })).toHaveAttribute("aria-checked", "true");
      expect(getByRole(credentials, "radio", { name: "oauth" })).toHaveAttribute("aria-checked", "false");
      expect(apiKey).toBeInTheDocument();
    });

    it("should display array of simple entity field", () => {
      const emails = getInputByName(container, "connectionConfiguration.emails");
      expect(emails).toBeInTheDocument();
    });

    it("should display array with items list field", () => {
      const workTime = screen.getByTestId("connectionConfiguration.workTime");
      expect(workTime).toBeInTheDocument();
    });

    it("should display array of objects field", () => {
      const priceList = container.querySelector("div[data-testid='connectionConfiguration.priceList']");
      const addButton = priceList?.querySelector("button[data-testid='addItemButton']");
      expect(priceList).toBeInTheDocument();
      expect(addButton).toBeInTheDocument();
    });
  });

  describe("filling service form", () => {
    const filledForm: Record<string, unknown> = {
      credentials: { api_key: "test-api-key", type: "api" },
      emails: ["test@test.com"],
      host: "test-host",
      message: "test-message",
      password: "test-password",
      port: 123,
      priceList: [{ name: "test-price-list-name", price: 1, origin: { type: "city", city: "test-city" } }],
      workTime: ["day"],
    };

    it("should render optional field hidden, but allow to open and edit", async () => {
      const container = await renderForm({
        formValuesOverride: { ...filledForm },
        propertiesOverride: {
          additional_separate_group: { type: "string", group: "abc" },
          additional_same_group: { type: "string" },
        },
      });

      const additionalSeparateGroupInput = getInputByName(
        container,
        "connectionConfiguration.additional_separate_group"
      );
      const additionalSameGroupInput = getInputByName(container, "connectionConfiguration.additional_same_group");

      const optionalFields = screen.getAllByTestId("optional-fields");
      expect(additionalSameGroupInput).not.toBeVisible();

      await userEvent.click(optionalFields.at(0)!);
      expect(additionalSameGroupInput).toBeVisible();

      expect(additionalSeparateGroupInput).not.toBeVisible();
      await userEvent.click(optionalFields.at(1)!);
      expect(additionalSeparateGroupInput).toBeVisible();

      const input1 = additionalSameGroupInput;
      const input2 = additionalSeparateGroupInput;
      await userEvent.type(input1!, "input1");
      await userEvent.type(input2!, "input2");

      await submitForm(container);

      expect(result).toEqual({
        name: "test-name",
        connectionConfiguration: {
          ...filledForm,
          additional_same_group: "input1",
          additional_separate_group: "input2",
        },
      });
    });

    it("should always render always_show optional field, but not require it", async () => {
      const container = await renderForm({
        formValuesOverride: { ...filledForm },
        propertiesOverride: {
          optional_always_show: { type: "string", always_show: true },
        },
      });
      expect(getInputByName(container, "connectionConfiguration.optional_always_show")).toBeVisible();

      await submitForm(container);

      expect(result).toEqual({
        name: "test-name",
        connectionConfiguration: {
          ...filledForm,
        },
      });

      await userEvent.type(getInputByName(container, "connectionConfiguration.optional_always_show")!, "input1");

      await submitForm(container);

      expect(result).toEqual({
        name: "test-name",
        connectionConfiguration: {
          ...filledForm,
          optional_always_show: "input1",
        },
      });
    });

    it("should not render entire optional object and oneOf fields as hidden, but render the optional sub-fields as hidden", async () => {
      const container = await renderForm({
        formValuesOverride: { ...filledForm, optional_oneof: { const_prop: "FIRST_CHOICE" } },
        propertiesOverride: {
          optional_object: {
            type: "object",
            required: ["required_obj_subfield"],
            properties: { required_obj_subfield: { type: "string" }, optional_obj_subfield: { type: "string" } },
          },
          optional_oneof: {
            type: "object",
            oneOf: [
              {
                title: "First choice",
                required: ["required_oneof_subfield"],
                properties: {
                  required_oneof_subfield: { type: "string" },
                  optional_oneof_subfield: { type: "string" },
                  const_prop: { type: "string", const: "FIRST_CHOICE" },
                },
              },
              {
                title: "Second choice",
                required: ["different_required_oneof_subfield"],
                properties: {
                  different_required_oneof_subfield: { type: "integer" },
                  optional_oneof_subfield: { type: "string" },
                  const_prop: { type: "string", const: "SECOND_CHOICE" },
                },
              },
            ],
          },
        },
      });
      expect(getInputByName(container, "connectionConfiguration.optional_object.required_obj_subfield")).toBeVisible();
      expect(
        getInputByName(container, "connectionConfiguration.optional_object.optional_obj_subfield")
      ).not.toBeVisible();
      expect(getInputByName(container, "connectionConfiguration.optional_oneof.required_oneof_subfield")).toBeVisible();
      expect(
        getInputByName(container, "connectionConfiguration.optional_oneof.optional_oneof_subfield")
      ).not.toBeVisible();

      await waitFor(() => userEvent.click(screen.getAllByTestId("optional-fields").at(0)!));

      expect(getInputByName(container, "connectionConfiguration.optional_object.optional_obj_subfield")).toBeVisible();

      await waitFor(() => userEvent.click(screen.getAllByTestId("optional-fields").at(1)!));

      expect(getInputByName(container, "connectionConfiguration.optional_oneof.optional_oneof_subfield")).toBeVisible();

      const input1 = getInputByName(container, "connectionConfiguration.optional_object.required_obj_subfield");
      await userEvent.type(input1!, "required obj subfield value");
      const input2 = getInputByName(container, "connectionConfiguration.optional_object.optional_obj_subfield");
      await userEvent.type(input2!, "optional obj subfield value");
      const input3 = getInputByName(container, "connectionConfiguration.optional_oneof.required_oneof_subfield");
      await userEvent.type(input3!, "required oneof subfield value");
      const input4 = getInputByName(container, "connectionConfiguration.optional_oneof.optional_oneof_subfield");
      await userEvent.type(input4!, "optional oneof subfield value");

      await submitForm(container);

      expect(result).toEqual({
        name: "test-name",
        connectionConfiguration: {
          ...filledForm,
          optional_object: {
            required_obj_subfield: "required obj subfield value",
            optional_obj_subfield: "optional obj subfield value",
          },
          optional_oneof: {
            required_oneof_subfield: "required oneof subfield value",
            optional_oneof_subfield: "optional oneof subfield value",
            const_prop: "FIRST_CHOICE",
          },
        },
      });
    });

    it("should load optional fields' default values in collapsed fields", async () => {
      const container = await renderForm({
        formValuesOverride: { ...filledForm, additional_separate_group: "additional_separate_group_default" },
        propertiesOverride: {
          additional_same_group: { type: "string" },
          additional_separate_group: { type: "string", group: "abc", default: "additional_separate_group_default" },
        },
      });
      expect(getInputByName(container, "connectionConfiguration.additional_same_group")).not.toBeVisible();
      expect(getInputByName(container, "connectionConfiguration.additional_separate_group")).not.toBeVisible();
      expect(getInputByName(container, "connectionConfiguration.additional_same_group")?.getAttribute("value")).toEqual(
        ""
      );
      expect(
        getInputByName(container, "connectionConfiguration.additional_separate_group")?.getAttribute("value")
      ).toEqual("additional_separate_group_default");
    });

    it("should auto-expand optional sections containing a field with an existing non-default value", async () => {
      const container = await renderForm({
        formValuesOverride: { ...filledForm, additional_same_group: "input1", additional_separate_group: "input2" },
        propertiesOverride: {
          additional_same_group: { type: "string" },
          additional_separate_group: { type: "string", group: "abc", default: "additional_separate_group_default" },
        },
      });
      expect(getInputByName(container, "connectionConfiguration.additional_same_group")).toBeVisible();
      expect(getInputByName(container, "connectionConfiguration.additional_separate_group")).toBeVisible();
      expect(getInputByName(container, "connectionConfiguration.additional_same_group")?.getAttribute("value")).toEqual(
        "input1"
      );
      expect(
        getInputByName(container, "connectionConfiguration.additional_separate_group")?.getAttribute("value")
      ).toEqual("input2");
    });

    it("should allow nested one ofs", async () => {
      const container = await renderForm({
        formValuesOverride: {
          ...filledForm,
        },
        propertiesOverride: {
          condition: {
            type: "object",
            oneOf: [
              {
                title: "option1",
                required: ["api_key"],
                properties: {
                  api_key: {
                    type: "string",
                  },
                  type: {
                    type: "string",
                    const: "api",
                    default: "api",
                  },
                },
              },
              {
                title: "option2",
                required: ["redirect_uri"],
                properties: {
                  nestedcondition: {
                    type: "object",
                    oneOf: [
                      {
                        title: "nestoption1",
                        required: ["api_key"],
                        properties: {
                          api_key: {
                            type: "string",
                          },
                          type: {
                            type: "string",
                            const: "api",
                            default: "api",
                          },
                        },
                      },
                      {
                        title: "nestedoption2",
                        required: ["doublenestedinput"],
                        properties: {
                          doublenestedinput: {
                            type: "string",
                          },
                          type: {
                            type: "string",
                            const: "second",
                            default: "second",
                          },
                        },
                      },
                    ],
                  },
                  type: {
                    type: "string",
                    const: "oauth",
                    default: "oauth",
                  },
                },
              },
            ],
          },
        },
      });

      await selectSegmentedOption(container, "connectionConfiguration.condition", "option2");
      await selectSegmentedOption(container, "connectionConfiguration.condition.nestedcondition", "nestedoption2");

      const uri = container.querySelector(
        "input[name='connectionConfiguration.condition.nestedcondition.doublenestedinput']"
      );
      await userEvent.type(uri!, "doublenestedvalue");

      await submitForm(container);

      expect(result?.connectionConfiguration).toEqual({
        ...filledForm,
        condition: { nestedcondition: { doublenestedinput: "doublenestedvalue", type: "second" }, type: "oauth" },
      });
    });

    it("should not submit with failed validation", async () => {
      const container = await renderForm({
        formValuesOverride: { ...filledForm },
        propertiesOverride: {
          additional_same_group: { type: "string", pattern: "input" },
        },
      });

      await userEvent.type(getInputByName(container, "connectionConfiguration.additional_same_group")!, "inp");

      await submitForm(container);

      // can't submit, no result
      expect(result).toBeFalsy();

      expect(screen.getByText("The value must match the pattern input")).toBeInTheDocument();

      await userEvent.type(getInputByName(container, "connectionConfiguration.additional_same_group")!, "ut");

      await waitFor(() => userEvent.click(getSubmitButton(container)!));

      expect(result?.connectionConfiguration).toEqual({
        ...filledForm,
        additional_same_group: "input",
      });
    });

    it("should cast existing data, throwing away unknown properties and cast existing ones", async () => {
      const container = await renderForm({
        formValuesOverride: { ...filledForm, port: "123", unknown_field: { abc: "def" } },
      });

      await submitForm(container);

      expect(result?.connectionConfiguration).toEqual(filledForm);
    });

    it("should fill password", async () => {
      const container = await renderForm({ formValuesOverride: { ...filledForm, password: undefined } });
      const password = getInputByName(container, "connectionConfiguration.password");
      await userEvent.type(password!, "testword");

      await submitForm(container);

      expect(result?.connectionConfiguration).toEqual({
        ...filledForm,
        password: "testword",
      });
    });

    it("should change password", async () => {
      const container = await renderForm({ formValuesOverride: { ...filledForm, password: "*****" } });
      await waitFor(() => userEvent.click(screen.getByTestId("edit-secret")));
      const password = getInputByName(container, "connectionConfiguration.password");
      await userEvent.type(password!, "testword");

      await submitForm(container);

      expect(result?.connectionConfiguration).toEqual({
        ...filledForm,
        password: "testword",
      });
    });

    it("should fill right values in array of simple entity field", async () => {
      const container = await renderForm({ formValuesOverride: { ...filledForm, emails: [] } });
      const emails = screen.getByTestId("tag-input").querySelector("input");
      await userEvent.type(emails!, "test1@test.com{enter}test2@test.com{enter}test3@test.com{enter}");

      await submitForm(container);

      expect(result?.connectionConfiguration).toEqual({
        ...filledForm,
        emails: ["test1@test.com", "test2@test.com", "test3@test.com"],
      });
    });

    it("should extend right values in array of simple entity field", async () => {
      const container = await renderForm({ formValuesOverride: { ...filledForm } });
      const emails = screen.getByTestId("tag-input").querySelector("input");
      await userEvent.type(emails!, "another@test.com{enter}");

      await submitForm(container);

      expect(result?.connectionConfiguration).toEqual({
        ...filledForm,
        emails: ["test@test.com", "another@test.com"],
      });
    });

    it("should fill right values in array with items list field", async () => {
      const container = await renderForm({ formValuesOverride: { ...filledForm, workTime: undefined } });
      const workTimeButton = screen.getByTestId("connectionConfiguration.workTime-button");
      await userEvent.click(workTimeButton);
      await waitFor(() => userEvent.click(screen.getByRole("option", { name: "day" })));
      await waitFor(() => userEvent.click(screen.getByRole("option", { name: "night" })));

      await submitForm(container);

      expect(result?.connectionConfiguration).toEqual({ ...filledForm, workTime: ["day", "night"] });
    });

    it("should add values in array with items list field", async () => {
      const container = await renderForm({ formValuesOverride: { ...filledForm } });
      const workTimeButton = screen.getByTestId("connectionConfiguration.workTime-button");
      await userEvent.click(workTimeButton);
      await waitFor(() => userEvent.click(screen.getByRole("option", { name: "night" })));

      await submitForm(container);

      expect(result?.connectionConfiguration).toEqual({ ...filledForm, workTime: ["day", "night"] });
    });

    it("change oneOf field value", async () => {
      const container = await renderForm({ formValuesOverride: { ...filledForm, credentials: { type: "api" } } });
      const apiKey = getInputByName(container, "connectionConfiguration.credentials.api_key");
      expect(apiKey).toBeInTheDocument();

      await selectSegmentedOption(container, "connectionConfiguration.credentials", "oauth");

      expect(apiKey).not.toBeInTheDocument();
      const uri = getInputByName(container, "connectionConfiguration.credentials.redirect_uri");
      expect(uri).toBeInTheDocument();

      await userEvent.type(uri!, "def");

      await submitForm(container);

      expect(result?.connectionConfiguration).toEqual({
        ...filledForm,
        credentials: { type: "oauth", redirect_uri: "def" },
      });
    });

    it("should fill right values oneOf field", async () => {
      const container = await renderForm({ formValuesOverride: { ...filledForm, credentials: { type: "api" } } });

      await selectSegmentedOption(container, "connectionConfiguration.credentials", "oauth");

      const uri = getInputByName(container, "connectionConfiguration.credentials.redirect_uri");
      await userEvent.type(uri!, "test-uri");

      await submitForm(container);

      expect(result?.connectionConfiguration).toEqual({
        ...filledForm,
        credentials: { redirect_uri: "test-uri", type: "oauth" },
      });
    });

    it("should change value in existing oneOf", async () => {
      const container = await renderForm({
        formValuesOverride: { ...filledForm, credentials: { type: "oauth", redirect_uri: "abc" } },
      });

      const uri = getInputByName(container, "connectionConfiguration.credentials.redirect_uri");
      await userEvent.type(uri!, "def");

      await submitForm(container);

      expect(result?.connectionConfiguration).toEqual({
        ...filledForm,
        credentials: { redirect_uri: "abcdef", type: "oauth" },
      });
    });

    it("should fill right values in array of objects field", async () => {
      const container = await renderForm({ formValuesOverride: { ...filledForm, priceList: undefined } });

      const addPriceListItem = useAddPriceListItem(container);
      await addPriceListItem("test-1", "1", "city", "test-city-1");
      await addPriceListItem("test-2", "2", "country", "test-country-1");

      await submitForm(container);

      expect(result?.connectionConfiguration).toEqual({
        ...filledForm,
        priceList: [
          { name: "test-1", price: 1, origin: { type: "city", city: "test-city-1" } },
          { name: "test-2", price: 2, origin: { type: "country", country: "test-country-1" } },
        ],
      });
    });

    it("should extend values in array of objects field", async () => {
      const container = await renderForm({ formValuesOverride: { ...filledForm } });
      const addPriceListItem = useAddPriceListItem(container, 1);
      await addPriceListItem("test-2", "2", "country", "test-country-2");

      await submitForm(container);

      expect(result?.connectionConfiguration).toEqual({
        ...filledForm,
        priceList: [
          { name: "test-price-list-name", price: 1, origin: { type: "city", city: "test-city" } },
          { name: "test-2", price: 2, origin: { type: "country", country: "test-country-2" } },
        ],
      });
    });

    describe("oneOf control type", () => {
      const unknownDisplayType = { display_type: "Dropdown" } as unknown as AirbyteJSONSchema;

      it.each([2, 3, 4])("should render a oneOf with %i options as a segmented control", async (optionCount) => {
        await renderForm({ propertiesOverride: { choice: buildOneOfField(optionCount) } });

        const choice = screen.getByRole("radiogroup", { name: "Choice" });
        expect(getAllByRole(choice, "radio")).toHaveLength(optionCount);
        expect(screen.queryByTestId("connectionConfiguration.choice-listbox-button")).not.toBeInTheDocument();
      });

      it.each([1, 5])("should render a oneOf with %i options as a dropdown", async (optionCount) => {
        await renderForm({ propertiesOverride: { choice: buildOneOfField(optionCount) } });

        expect(screen.getByTestId("connectionConfiguration.choice-listbox-button")).toHaveAttribute(
          "aria-haspopup",
          "listbox"
        );
        expect(screen.queryByRole("radiogroup", { name: "Choice" })).not.toBeInTheDocument();
      });

      it("should count only visible options when choosing the control", async () => {
        const choice = buildOneOfField(5);
        const [first, second, third, fourth, fifth] = choice.oneOf as AirbyteJSONSchema[];
        await renderForm({
          propertiesOverride: {
            choice: {
              ...choice,
              oneOf: [first, second, third, { ...fourth, airbyte_hidden: true }, { ...fifth, airbyte_hidden: true }],
            },
          },
        });

        const segmentedControl = screen.getByRole("radiogroup", { name: "Choice" });
        expect(getAllByRole(segmentedControl, "radio").map((radio) => radio.textContent)).toEqual([
          "choice 0",
          "choice 1",
          "choice 2",
        ]);
      });

      it("should render a oneOf whose options add no fields as a dropdown", async () => {
        await renderForm({
          propertiesOverride: {
            choice: {
              type: "object",
              title: "Choice",
              oneOf: [0, 1].map((index) => ({
                title: `choice ${index}`,
                properties: { type: { type: "string", const: `type_${index}`, default: `type_${index}` } },
              })),
            },
          },
        });

        expect(screen.getByTestId("connectionConfiguration.choice-listbox-button")).toBeInTheDocument();
        expect(screen.queryByRole("radiogroup", { name: "Choice" })).not.toBeInTheDocument();
      });

      it.each([2, 5])("should keep a dropdown when display_type is dropdown with %i options", async (optionCount) => {
        await renderForm({
          propertiesOverride: { choice: buildOneOfField(optionCount, { display_type: "dropdown" }) },
        });

        expect(screen.getByTestId("connectionConfiguration.choice-listbox-button")).toBeInTheDocument();
        expect(screen.queryByRole("radiogroup", { name: "Choice" })).not.toBeInTheDocument();
      });

      it.each([2, 5])("should keep radio tiles when display_type is radio with %i options", async (optionCount) => {
        await renderForm({
          propertiesOverride: { choice: buildOneOfField(optionCount, { display_type: "radio" }) },
        });

        expect(screen.getByTestId("radio-option.0")).toHaveTextContent("choice 0");
        expect(screen.getByTestId(`radio-option.${optionCount - 1}`)).toHaveTextContent(`choice ${optionCount - 1}`);
        expect(screen.queryByRole("radiogroup", { name: "Choice" })).not.toBeInTheDocument();
        expect(screen.queryByTestId("connectionConfiguration.choice-listbox-button")).not.toBeInTheDocument();
      });

      it("should ignore an unknown display_type and render 2 options as a segmented control", async () => {
        await renderForm({ propertiesOverride: { choice: buildOneOfField(2, unknownDisplayType) } });

        expect(screen.getByRole("radiogroup", { name: "Choice" })).toBeInTheDocument();
      });

      it("should ignore an unknown display_type and render 5 options as a dropdown", async () => {
        await renderForm({ propertiesOverride: { choice: buildOneOfField(5, unknownDisplayType) } });

        expect(screen.getByTestId("connectionConfiguration.choice-listbox-button")).toBeInTheDocument();
      });

      it("should not let a read-only segmented control change the selection", async () => {
        const container = await renderForm({
          formValuesOverride: { ...filledForm, choice: { type: "type_0", field_0: "first" } },
          propertiesOverride: { choice: buildOneOfField(2, { readOnly: true }) },
        });
        const choice = screen.getByRole("radiogroup", { name: "Choice" });
        getAllByRole(choice, "radio").forEach((radio) => expect(radio).toHaveAttribute("aria-disabled", "true"));

        await userEvent.click(getByRole(choice, "radio", { name: "choice 1" }));

        expect(getByRole(choice, "radio", { name: "choice 0" })).toHaveAttribute("aria-checked", "true");
        expect(getInputByName(container, "connectionConfiguration.choice.field_0")).toHaveValue("first");
      });

      it("should check no segment when the value matches no visible option", async () => {
        await renderForm({
          formValuesOverride: { ...filledForm, choice: { type: "type_unknown" } },
          propertiesOverride: { choice: buildOneOfField(2) },
        });

        const choice = screen.getByRole("radiogroup", { name: "Choice" });
        getAllByRole(choice, "radio").forEach((radio) => expect(radio).toHaveAttribute("aria-checked", "false"));
      });

      it("should not mark the segmented control invalid when only a sub-field is invalid", async () => {
        const container = await renderForm({ propertiesOverride: { choice: buildOneOfField(2) } });

        await submitForm(container);

        await waitFor(() =>
          expect(getInputByName(container, "connectionConfiguration.choice.field_0")).toHaveAttribute(
            "aria-invalid",
            "true"
          )
        );
        expect(screen.getByRole("radiogroup", { name: "Choice" })).not.toHaveAttribute("aria-invalid");
      });

      it("should not mark the dropdown invalid when only a sub-field is invalid", async () => {
        const container = await renderForm({ propertiesOverride: { choice: buildOneOfField(5) } });

        await submitForm(container);

        await waitFor(() =>
          expect(getInputByName(container, "connectionConfiguration.choice.field_0")).toHaveAttribute(
            "aria-invalid",
            "true"
          )
        );
        expect(screen.getByTestId("connectionConfiguration.choice-listbox-button")).not.toHaveClass(
          "listboxButton--error"
        );
      });

      it("should switch options of a oneOf with 5 options through the dropdown", async () => {
        const container = await renderForm({
          formValuesOverride: { ...filledForm, choice: { type: "type_0", field_0: "first" } },
          propertiesOverride: { choice: buildOneOfField(5) },
        });
        expect(getInputByName(container, "connectionConfiguration.choice.field_0")).toHaveValue("first");

        selectDropdownOption(container, "connectionConfiguration.choice", "choice 4");

        expect(getInputByName(container, "connectionConfiguration.choice.field_0")).not.toBeInTheDocument();
        await userEvent.type(getInputByName(container, "connectionConfiguration.choice.field_4")!, "fifth");
        await submitForm(container);

        expect(result?.connectionConfiguration).toEqual({
          ...filledForm,
          choice: { type: "type_4", field_4: "fifth" },
        });
      });

      it("should swap the sub-fields and form values when switching segments", async () => {
        const container = await renderForm({
          formValuesOverride: { ...filledForm, choice: { type: "type_0", field_0: "first" } },
          propertiesOverride: { choice: buildOneOfField(3) },
        });
        expect(getInputByName(container, "connectionConfiguration.choice.field_0")).toHaveValue("first");

        await selectSegmentedOption(container, "connectionConfiguration.choice", "choice 2");

        expect(screen.getByRole("radio", { name: "choice 0" })).toHaveAttribute("aria-checked", "false");
        expect(screen.getByRole("radio", { name: "choice 2" })).toHaveAttribute("aria-checked", "true");
        expect(getInputByName(container, "connectionConfiguration.choice.field_0")).not.toBeInTheDocument();
        await userEvent.type(getInputByName(container, "connectionConfiguration.choice.field_2")!, "third");
        await submitForm(container);

        expect(result?.connectionConfiguration).toEqual({
          ...filledForm,
          choice: { type: "type_2", field_2: "third" },
        });
      });

      it("should select a segment with the arrow keys", async () => {
        const container = await renderForm({
          formValuesOverride: { ...filledForm, choice: { type: "type_0", field_0: "first" } },
          propertiesOverride: { choice: buildOneOfField(3) },
        });
        const choice = screen.getByRole("radiogroup", { name: "Choice" });

        await userEvent.click(getByRole(choice, "radio", { name: "choice 0" }));
        await userEvent.keyboard("{ArrowRight}");

        const secondSegment = getByRole(choice, "radio", { name: "choice 1" });
        expect(secondSegment).toHaveFocus();
        expect(secondSegment).toHaveAttribute("aria-checked", "true");
        expect(getInputByName(container, "connectionConfiguration.choice.field_0")).not.toBeInTheDocument();
        await userEvent.type(getInputByName(container, "connectionConfiguration.choice.field_1")!, "second");
        await submitForm(container);

        expect(result?.connectionConfiguration).toEqual({
          ...filledForm,
          choice: { type: "type_1", field_1: "second" },
        });
      });
    });
  });

  describe("oauth flow", () => {
    const oauthSchema = {
      ...schema,
      properties: {
        start_date: {
          type: "string",
        },
        credentials: {
          type: "object",
          group: "auth",
          oneOf: [
            {
              type: "object",
              title: "OAuth",
              required: ["access_token"],
              properties: {
                access_token: {
                  type: "string",
                  title: "Access Token",
                  description: "OAuth access token",
                  airbyte_secret: true,
                },
                option_title: {
                  type: "string",
                  const: "OAuth Credentials",
                  order: 0,
                },
              },
            },
            {
              type: "object",
              title: "Personal Access Token",
              required: ["personal_access_token"],
              properties: {
                option_title: {
                  type: "string",
                  const: "PAT Credentials",
                  order: 0,
                },
                personal_access_token: {
                  type: "string",
                  airbyte_secret: true,
                },
              },
            },
          ],
          title: "Authentication",
          description: "Choose how to authenticate to GitHub",
        },
      },
    };
    async function renderNewOAuthForm(
      props: {
        disableOAuth?: boolean;
        formValuesOverride?: Record<string, unknown>;
        specificationOverride?: Partial<ConnectorDefinitionSpecificationRead>;
        renderFooter?: ConnectorFormProps["renderFooter"];
      } = {}
    ) {
      return renderForm({
        ...props,
        specificationOverride: {
          connectionSpecification: oauthSchema,
          advancedAuth: {
            authFlowType: "oauth2.0",
            predicateKey: ["credentials", "option_title"],
            predicateValue: "OAuth Credentials",
            oauthConfigSpecification: {
              completeOAuthOutputSpecification: {
                type: "object",
                properties: {
                  access_token: {
                    type: "string",
                    path_in_connector_config: ["credentials", "access_token"],
                  },
                },
                additionalProperties: false,
              },
              completeOAuthServerInputSpecification: {
                type: "object",
                properties: {
                  client_id: {
                    type: "string",
                  },
                  client_secret: {
                    type: "string",
                  },
                },
                additionalProperties: false,
              },
              completeOAuthServerOutputSpecification: {
                type: "object",
                properties: {
                  client_id: {
                    type: "string",
                    path_in_connector_config: ["credentials", "client_id"],
                  },
                  client_secret: {
                    type: "string",
                    path_in_connector_config: ["credentials", "client_secret"],
                  },
                },
                additionalProperties: false,
              },
            },
          },
          ...props.specificationOverride,
        },
      });
    }

    it("should render the oauth button", async () => {
      const container = await renderNewOAuthForm();
      expect(getOAuthButton(container)).toBeInTheDocument();
      expect(getInputByName(container, "connectionConfiguration.credentials.access_token")).not.toBeInTheDocument();
    });

    it("should not offer manual setup when Airbyte has global oauth credentials", async () => {
      await renderNewOAuthForm({ specificationOverride: { advancedAuthGlobalCredentialsAvailable: true } });
      expect(screen.queryByRole("button", { name: "Set up manually" })).not.toBeInTheDocument();
    });

    it("should offer manual setup when Airbyte has no global oauth credentials", async () => {
      const container = await renderNewOAuthForm({
        specificationOverride: { advancedAuthGlobalCredentialsAvailable: false },
      });
      const manualSetupButton = screen.getByRole("button", { name: "Set up manually" });
      await waitFor(() => userEvent.click(manualSetupButton));
      expect(getInputByName(container, "connectionConfiguration.credentials.access_token")).toBeInTheDocument();
    });

    it("should insert values correctly and submit them", async () => {
      (useCompleteOAuth as jest.MockedFunction<typeof useCompleteOAuth>).mockReturnValue({
        completeDestinationOAuth: jest.fn(),
        completeSourceOAuth: () => {
          return Promise.resolve({
            request_succeeded: true,
            auth_payload: {
              access_token: "mytoken",
            },
          });
        },
      });
      const container = await renderNewOAuthForm();

      await executeOAuthFlow(container);

      expect(await screen.findByText("Authentication successful")).toBeInTheDocument();
      expect(getOAuthButton(container)?.textContent).toEqual("Re-authenticate");

      const submit = getSubmitButton(container);
      await waitFor(() => userEvent.click(submit!));

      expect(result?.connectionConfiguration).toEqual({
        credentials: { access_token: "mytoken", option_title: "OAuth Credentials" },
      });
    });

    it("lists the empty fields that OAuth fills as one entry that asks the user to authenticate", async () => {
      (useCompleteOAuth as jest.MockedFunction<typeof useCompleteOAuth>).mockReturnValue({
        completeDestinationOAuth: jest.fn(),
        completeSourceOAuth: () =>
          Promise.resolve({ request_succeeded: true, auth_payload: { access_token: "mytoken" } }),
      });
      const container = await renderNewOAuthForm({
        renderFooter: ({ emptyRequiredFields }) => (
          <output data-testid="empty-required-fields">{emptyRequiredFields.join(" | ")}</output>
        ),
      });

      expect(screen.getByTestId("empty-required-fields")).toHaveTextContent(/^Authenticate your account$/);

      await executeOAuthFlow(container);

      await waitFor(() => expect(screen.getByTestId("empty-required-fields")).toBeEmptyDOMElement());
    });

    it("should render reauthenticate message if there are form values already", async () => {
      const container = await renderNewOAuthForm({
        formValuesOverride: {
          credentials: { option_title: "OAuth Credentials", access_token: "xyz" },
        },
      });
      expect(getOAuthButton(container)).toBeInTheDocument();
      expect(getOAuthButton(container)?.textContent).toEqual("Re-authenticate");
    });

    it("should hide the oauth button when switching auth strategy", async () => {
      const container = await renderNewOAuthForm();

      await selectSegmentedOption(container, "connectionConfiguration.credentials", "Personal Access Token");

      expect(getOAuthButton(container)).not.toBeInTheDocument();
      expect(
        getInputByName(container, "connectionConfiguration.credentials.personal_access_token")
      ).toBeInTheDocument();
    });

    it("should render the oauth button on the top level", async () => {
      const container = await renderNewOAuthForm({
        specificationOverride: {
          connectionSpecification: {
            ...schema,
            properties: {
              ...schema.properties,
              access_token: {
                type: "string",
                airbyte_secret: true,
              },
            },
            row_batch_size: {
              type: "integer",
              default: 200,
            },
          },
          advancedAuth: {
            authFlowType: "oauth2.0",
            oauthConfigSpecification: {
              completeOAuthOutputSpecification: {
                type: "object",
                properties: {
                  access_token: {
                    type: "string",
                    path_in_connector_config: ["access_token"],
                  },
                },
                additionalProperties: false,
              },
              completeOAuthServerInputSpecification: {
                type: "object",
                properties: {
                  client_id: {
                    type: "string",
                  },
                  client_secret: {
                    type: "string",
                  },
                },
                additionalProperties: false,
              },
              completeOAuthServerOutputSpecification: {
                type: "object",
                properties: {
                  client_id: {
                    type: "string",
                    path_in_connector_config: ["client_id"],
                  },
                  client_secret: {
                    type: "string",
                    path_in_connector_config: ["client_secret"],
                  },
                },
                additionalProperties: false,
              },
            },
          },
        },
      });
      expect(getOAuthButton(container)).toBeInTheDocument();
    });
  });

  describe("Advanced section", () => {
    it("names the collapsible section of optional fields Advanced", async () => {
      await renderForm({
        propertiesOverride: {
          additional_same_group: { type: "string" },
        },
      });

      const optionalFields = screen.getAllByTestId("optional-fields");
      expect(optionalFields).toHaveLength(1);
      expect(optionalFields[0]).toHaveTextContent("Advanced");
    });

    it("does not render the advanced section if Resource Allocation is off + spec does not define it", async () => {
      await renderForm({});

      expect(screen.queryAllByTestId("optional-fields")).toHaveLength(0);
    });

    it("renders the Advanced section if the flag is on and value is present in spec without groups", async () => {
      await renderForm({ features: [FeatureItem.ConnectorResourceAllocation] });

      const optionalFields = screen.getAllByTestId("optional-fields");
      expect(optionalFields[0]).toHaveTextContent("Advanced");
      expect(optionalFields).toHaveLength(1);
    });

    it("renders the Advanced section  if the flag is on and value is present in spec with groups", async () => {
      await renderForm({
        propertiesOverride: {
          additional_separate_group: { type: "string", group: "abc" },
        },
        features: [FeatureItem.ConnectorResourceAllocation],
      });

      const optionalFields = screen.getAllByTestId("optional-fields");
      expect(optionalFields[0]).toHaveTextContent("Advanced");
      expect(optionalFields[1]).toHaveTextContent("Advanced");
      expect(optionalFields).toHaveLength(2);
    });

    it("combines resource allocation Advanced section with one described in the spec", async () => {
      await renderForm({
        propertiesOverride: {
          additional_separate_group: { type: "string", group: "advanced" },
        },
        features: [FeatureItem.ConnectorResourceAllocation],
      });

      const optionalFields = screen.getAllByTestId("optional-fields");
      expect(optionalFields[0]).toHaveTextContent("Advanced");
      expect(optionalFields).toHaveLength(1);
    });
  });

  describe("sections", () => {
    const sectionedSpec = {
      type: "object",
      required: ["host", "port", "database", "username", "replication_method"],
      groups: [{ id: "db" }, { id: "security" }, { id: "auth" }, { id: "sync" }, { id: "advanced", title: "Advanced" }],
      sections: [
        { id: "connection", title: "Connection", order: 0, check: true, groups: ["db", "security"] },
        { id: "credentials", title: "Credentials", order: 1, check: true, groups: ["auth"] },
        { id: "sync_method", title: "Sync method", order: 2, check: true, groups: ["sync"] },
        { id: "review", title: "Review & set up", order: 3, groups: ["advanced"] },
      ],
      properties: {
        host: { type: "string", title: "Host", group: "db", order: 0 },
        port: { type: "integer", title: "Port", default: 5432, group: "db", order: 1 },
        database: { type: "string", title: "Database name", group: "db", order: 2 },
        ssl_mode: {
          type: "string",
          title: "SSL mode",
          enum: ["disable", "require"],
          default: "require",
          group: "security",
        },
        username: { type: "string", title: "Username", group: "auth", order: 0 },
        password: { type: "string", title: "Password", airbyte_secret: true, group: "auth", order: 1 },
        replication_method: {
          type: "object",
          title: "Update method",
          display_type: "radio",
          group: "sync",
          oneOf: [
            { title: "Xmin", required: ["method"], properties: { method: { type: "string", const: "Xmin" } } },
            {
              title: "CDC",
              required: ["method", "replication_slot", "publication"],
              properties: {
                method: { type: "string", const: "CDC" },
                replication_slot: { type: "string", title: "Replication slot" },
                publication: { type: "string", title: "Publication" },
              },
            },
          ],
        },
        jdbc_url_params: { type: "string", title: "JDBC URL parameters", group: "advanced" },
      },
    } satisfies AirbyteJSONSchema & { groups: unknown[]; sections: unknown[] };
    const sectionedSpecification = { connectionSpecification: sectionedSpec };

    beforeEach(() => {
      mockExperiments({ "connector.sectionedSetupForm": true });
    });

    const getSection = (sectionId: string) => screen.getByTestId(`connector-form-section-${sectionId}`);
    const getSectionTitle = (sectionId: string) => getByRole(getSection(sectionId), "heading", { level: 2 });
    const getEditButton = (sectionId: string, title: string) =>
      getByRole(getSection(sectionId), "button", { name: `Edit ${title}` });
    const clickSectionButton = (name: string) => userEvent.click(screen.getByRole("button", { name }));

    async function completeConnectionSection(container: HTMLElement) {
      await userEvent.type(getInputByName(container, "connectionConfiguration.host")!, "db.example.com");
      await userEvent.type(getInputByName(container, "connectionConfiguration.database")!, "analytics");
      await clickSectionButton("Continue");
    }

    it("shows only the first section's fields and keeps the other sections' fields mounted", async () => {
      const container = await renderForm({ specificationOverride: sectionedSpecification });

      expect(getInputByName(container, "connectionConfiguration.host")).toBeVisible();
      expect(getInputByName(container, "connectionConfiguration.username")).toBeInTheDocument();
      expect(getInputByName(container, "connectionConfiguration.username")).not.toBeVisible();
      expect(getInputByName(container, "name")).not.toBeVisible();
      expect(screen.queryByRole("button", { name: "Back" })).not.toBeInTheDocument();
    });

    it("disables Continue until the required fields of the active section are filled", async () => {
      const container = await renderForm({ specificationOverride: sectionedSpecification });
      const continueButton = screen.getByRole("button", { name: "Continue" });

      expect(continueButton).toBeDisabled();
      expect(continueButton).toHaveAccessibleDescription(
        "Fill out required fields before continuing: Host and Database name"
      );

      await userEvent.type(getInputByName(container, "connectionConfiguration.host")!, "db.example.com");
      expect(continueButton).toHaveAccessibleDescription("Fill out required fields before continuing: Database name");

      await userEvent.type(getInputByName(container, "connectionConfiguration.database")!, "analytics");
      expect(continueButton).toBeEnabled();
      await clickSectionButton("Continue");

      await waitFor(() => expect(getInputByName(container, "connectionConfiguration.username")).toBeVisible());
      expect(getInputByName(container, "connectionConfiguration.host")).not.toBeVisible();
      expect(getSection("credentials")).not.toHaveTextContent("Required");
      expect(getSection("credentials")).toContainElement(screen.getByRole("button", { name: "Back" }));
      expect(getSection("credentials")).toContainElement(screen.getByRole("button", { name: "Continue" }));
    });

    it("goes back and reopens a completed section with edit", async () => {
      const container = await renderForm({ specificationOverride: sectionedSpecification });
      await completeConnectionSection(container);
      await waitFor(() => expect(getInputByName(container, "connectionConfiguration.username")).toBeVisible());

      await clickSectionButton("Back");
      await waitFor(() => expect(getInputByName(container, "connectionConfiguration.host")).toBeVisible());
      expect(getInputByName(container, "connectionConfiguration.host")).toHaveValue("db.example.com");

      await completeConnectionSection(container);
      await waitFor(() => expect(getInputByName(container, "connectionConfiguration.username")).toBeVisible());
      await userEvent.click(getEditButton("connection", "Connection"));
      await waitFor(() => expect(getInputByName(container, "connectionConfiguration.host")).toBeVisible());
    });

    it("shows the form title above the steps, with its description in a tooltip", async () => {
      await renderForm({
        specificationOverride: sectionedSpecification,
        title: "New source",
        description: "Sources are where you pull data from.",
      });
      const formTitle = screen.getByRole("heading", { name: "New source" });

      expect(formTitle.compareDocumentPosition(getSection("connection"))).toBe(Node.DOCUMENT_POSITION_FOLLOWING);
      await userEvent.hover(formTitle.nextElementSibling!);
      expect(await screen.findByRole("tooltip")).toHaveTextContent("Sources are where you pull data from.");
    });

    describe("with the connector.sectionedSetupForm experiment off", () => {
      beforeEach(() => {
        mockExperiments({ "connector.sectionedSetupForm": false });
      });

      it("renders a spec with sections as one page", async () => {
        const container = await renderForm({ specificationOverride: sectionedSpecification });

        expect(getInputByName(container, "connectionConfiguration.host")).toBeVisible();
        expect(getInputByName(container, "connectionConfiguration.username")).toBeVisible();
        expect(screen.queryByTestId("connector-form-section-connection")).not.toBeInTheDocument();
        expect(screen.queryByRole("button", { name: "Continue" })).not.toBeInTheDocument();
      });

      it("ignores sections that are not valid", async () => {
        const container = await renderForm({
          specificationOverride: { connectionSpecification: { ...sectionedSpec, sections: [{ id: 1 }, "review"] } },
        });

        expect(getInputByName(container, "connectionConfiguration.host")).toBeVisible();
        expect(screen.queryByRole("button", { name: "Continue" })).not.toBeInTheDocument();
      });
    });

    describe("focus", () => {
      it("does not move focus when the form renders", async () => {
        await renderForm({ specificationOverride: sectionedSpecification });

        expect(getSectionTitle("connection")).not.toHaveFocus();
      });

      it("focuses the first field with an error when a section is not valid", async () => {
        const container = await renderForm({
          specificationOverride: {
            connectionSpecification: {
              ...sectionedSpec,
              properties: {
                ...sectionedSpec.properties,
                host: { ...sectionedSpec.properties.host, pattern: "^[a-z.]+$" },
              },
            },
          },
        });
        await userEvent.type(getInputByName(container, "connectionConfiguration.host")!, "DB.example.com");
        await userEvent.type(getInputByName(container, "connectionConfiguration.database")!, "analytics");

        await clickSectionButton("Continue");

        await waitFor(() => expect(getInputByName(container, "connectionConfiguration.host")).toHaveFocus());
      });

      it("moves focus to the title of the section that opens on Continue, Back and Edit", async () => {
        const container = await renderForm({ specificationOverride: sectionedSpecification });
        const scrollIntoView = jest.mocked(Element.prototype.scrollIntoView);
        scrollIntoView.mockClear();

        await completeConnectionSection(container);
        await waitFor(() => expect(getSectionTitle("credentials")).toHaveFocus());
        expect(scrollIntoView.mock.contexts).toEqual([getSectionTitle("credentials")]);

        await clickSectionButton("Back");
        await waitFor(() => expect(getSectionTitle("connection")).toHaveFocus());

        await clickSectionButton("Continue");
        await waitFor(() => expect(getSectionTitle("credentials")).toHaveFocus());

        await userEvent.click(getEditButton("connection", "Connection"));
        await waitFor(() => expect(getSectionTitle("connection")).toHaveFocus());
      });
    });

    describe("accessibility", () => {
      it("titles every step with a level 2 heading and marks the current and the completed steps", async () => {
        const container = await renderForm({ specificationOverride: sectionedSpecification });

        expect(getSectionTitle("connection")).toHaveAccessibleName("Connection current");
        expect(getSectionTitle("connection")).toHaveAttribute("aria-current", "step");
        expect(getSectionTitle("credentials")).toHaveAccessibleName("Credentials");
        expect(getSectionTitle("credentials")).not.toHaveAttribute("aria-current");
        expect(getSectionTitle("review")).toHaveAccessibleName("Review & set up");

        await completeConnectionSection(container);

        await waitFor(() => expect(getSectionTitle("connection")).toHaveAccessibleName("Connection completed"));
        expect(getSectionTitle("connection")).not.toHaveAttribute("aria-current");
        expect(getSectionTitle("credentials")).toHaveAccessibleName("Credentials current");
        expect(getSectionTitle("credentials")).toHaveAttribute("aria-current", "step");
      });

      it("names each Edit button after its section", async () => {
        const container = await renderForm({ specificationOverride: sectionedSpecification });
        await completeConnectionSection(container);
        await waitFor(() => expect(getInputByName(container, "connectionConfiguration.username")).toBeVisible());
        await userEvent.type(getInputByName(container, "connectionConfiguration.username")!, "airbyte_reader");
        await clickSectionButton("Continue");

        await waitFor(() => expect(getEditButton("credentials", "Credentials")).toBeInTheDocument());
        expect(getEditButton("connection", "Connection")).toBeInTheDocument();
        expect(screen.queryByRole("button", { name: "Edit" })).not.toBeInTheDocument();
      });
    });

    it("shows the title of a titled group as a subheading, with the optional fields of each group in its block", async () => {
      const container = await renderForm({
        specificationOverride: {
          connectionSpecification: {
            ...sectionedSpec,
            required: [...sectionedSpec.required, "ssl_mode"],
            groups: sectionedSpec.groups.map((group) =>
              group.id === "security" ? { ...group, title: "Network & security" } : group
            ),
            properties: {
              ...sectionedSpec.properties,
              schemas: { type: "string", title: "Schemas", group: "db" },
            },
          },
        },
      });
      const connectionSection = getSection("connection");
      const securityHeading = getByRole(connectionSection, "heading", { level: 3, name: "Network & security" });
      const dbOptionalFields = getByTestId(connectionSection, "optional-fields");
      const hostInput = getInputByName(container, "connectionConfiguration.host")!;

      expect(securityHeading.compareDocumentPosition(getByText(connectionSection, "SSL mode"))).toBe(
        Node.DOCUMENT_POSITION_FOLLOWING
      );
      let dbBlock = dbOptionalFields.parentElement;
      while (dbBlock && !dbBlock.contains(hostInput)) {
        dbBlock = dbBlock.parentElement;
      }
      expect(dbBlock).not.toContainElement(securityHeading);
      expect(dbOptionalFields.compareDocumentPosition(securityHeading)).toBe(Node.DOCUMENT_POSITION_FOLLOWING);
    });

    it("requires the fields of the selected condition before leaving its section, then submits all sections", async () => {
      const container = await renderForm({ specificationOverride: sectionedSpecification });
      await completeConnectionSection(container);
      await waitFor(() => expect(getInputByName(container, "connectionConfiguration.username")).toBeVisible());
      await userEvent.type(getInputByName(container, "connectionConfiguration.username")!, "airbyte_reader");
      await clickSectionButton("Continue");
      await waitFor(() => expect(screen.getByTestId("radio-option.1")).toBeVisible());

      await userEvent.click(screen.getByTestId("radio-option.1"));
      await waitFor(() => expect(screen.getByRole("button", { name: "Continue" })).toBeDisabled());
      expect(screen.getByRole("button", { name: "Continue" })).toHaveAccessibleDescription(
        "Fill out required fields before continuing: Publication and Replication slot"
      );

      await userEvent.type(
        getInputByName(container, "connectionConfiguration.replication_method.replication_slot")!,
        "slot"
      );
      await userEvent.type(getInputByName(container, "connectionConfiguration.replication_method.publication")!, "pub");
      await clickSectionButton("Continue");
      await waitFor(() => expect(getInputByName(container, "name")).toBeVisible());

      await submitForm(container);

      expect(result).toEqual({
        name: "test-name",
        connectionConfiguration: {
          host: "db.example.com",
          port: 5432,
          database: "analytics",
          ssl_mode: "require",
          username: "airbyte_reader",
          replication_method: { method: "CDC", replication_slot: "slot", publication: "pub" },
        },
      });
    });

    it("opens the earliest section with an error when the final submit fails", async () => {
      const ClearHost = () => {
        const { setValue } = useFormContext<ConnectorFormValues>();
        return (
          <button type="button" onClick={() => setValue("connectionConfiguration.host", "")}>
            Clear host
          </button>
        );
      };
      const container = await renderForm({
        specificationOverride: sectionedSpecification,
        formAutomation: <ClearHost />,
      });
      await completeConnectionSection(container);
      await waitFor(() => expect(getInputByName(container, "connectionConfiguration.username")).toBeVisible());
      await userEvent.type(getInputByName(container, "connectionConfiguration.username")!, "airbyte_reader");
      await clickSectionButton("Continue");
      await waitFor(() => expect(screen.getByTestId("radio-option.0")).toBeVisible());
      await clickSectionButton("Continue");
      await waitFor(() => expect(getInputByName(container, "name")).toBeVisible());

      await clickSectionButton("Clear host");
      await submitForm(container);

      await waitFor(() => expect(getInputByName(container, "connectionConfiguration.host")).toBeVisible());
      expect(getSectionTitle("connection")).toHaveFocus();
      expect(result).toBeUndefined();
    });

    it("locks Back and Edit while the final submit runs", async () => {
      let finishSubmit: () => void = () => undefined;
      const container = await renderForm({
        specificationOverride: sectionedSpecification,
        onSubmit: () =>
          new Promise<void>((resolve) => {
            finishSubmit = resolve;
          }),
      });
      await completeConnectionSection(container);
      await waitFor(() => expect(getInputByName(container, "connectionConfiguration.username")).toBeVisible());
      await userEvent.type(getInputByName(container, "connectionConfiguration.username")!, "airbyte_reader");
      await clickSectionButton("Continue");
      await waitFor(() => expect(screen.getByTestId("radio-option.0")).toBeVisible());
      await clickSectionButton("Continue");
      await waitFor(() => expect(getInputByName(container, "name")).toBeVisible());

      await submitForm(container);

      await waitFor(() => expect(getEditButton("connection", "Connection")).toBeDisabled());
      expect(getEditButton("credentials", "Credentials")).toBeDisabled();
      expect(getEditButton("sync_method", "Sync method")).toBeDisabled();
      expect(screen.getByRole("button", { name: "Back" })).toBeDisabled();

      finishSubmit();

      await waitFor(() => expect(getEditButton("connection", "Connection")).toBeEnabled());
      expect(screen.getByRole("button", { name: "Back" })).toBeEnabled();
    });

    describe("with OAuth", () => {
      const oauthSectionedSpecification = {
        connectionSpecification: {
          type: "object",
          required: ["credentials", "start_date"],
          groups: [{ id: "auth" }, { id: "filters" }],
          sections: [
            { id: "connect", title: "Connect account", order: 0, groups: ["auth"] },
            { id: "review", title: "Review & set up", order: 1, groups: ["filters"] },
          ],
          properties: {
            credentials: {
              type: "object",
              group: "auth",
              oneOf: [
                {
                  title: "OAuth",
                  required: ["access_token"],
                  properties: {
                    auth_type: { type: "string", const: "oauth" },
                    access_token: { type: "string", airbyte_secret: true },
                  },
                },
              ],
            },
            start_date: { type: "string", title: "Start date", group: "filters" },
          },
        },
        advancedAuth: {
          authFlowType: "oauth2.0",
          predicateKey: ["credentials", "auth_type"],
          predicateValue: "oauth",
          oauthConfigSpecification: {
            completeOAuthOutputSpecification: {
              type: "object",
              properties: {
                access_token: { type: "string", path_in_connector_config: ["credentials", "access_token"] },
              },
            },
          },
        },
      } satisfies Partial<ConnectorDefinitionSpecificationRead>;
      const withConnectSectionCheck = {
        ...oauthSectionedSpecification,
        connectionSpecification: {
          ...oauthSectionedSpecification.connectionSpecification,
          sections: oauthSectionedSpecification.connectionSpecification.sections.map((section) =>
            section.id === "connect" ? { ...section, check: true } : section
          ),
        },
      };

      beforeEach(() => {
        (useCompleteOAuth as jest.MockedFunction<typeof useCompleteOAuth>).mockReturnValue({
          completeDestinationOAuth: jest.fn(),
          completeSourceOAuth: () =>
            Promise.resolve({ request_succeeded: true, auth_payload: { access_token: "mytoken" } }),
        });
      });

      it("does not block a section on fields that OAuth fills, and reopens it when the final submit needs them", async () => {
        const container = await renderForm({ specificationOverride: oauthSectionedSpecification });
        expect(getOAuthButton(container)).toBeVisible();

        await clickSectionButton("Continue");

        await waitFor(() => expect(getInputByName(container, "connectionConfiguration.start_date")).toBeVisible());

        await userEvent.type(getInputByName(container, "connectionConfiguration.start_date")!, "2024-01-01");
        await submitForm(container);

        await waitFor(() => expect(getOAuthButton(container)).toBeVisible());
        expect(getSectionTitle("connect")).toHaveFocus();
        expect(getSection("connect")).toHaveTextContent("Authentication required");
        expect(result).toBeUndefined();
      });

      it("asks the user to authenticate instead of checking a section whose OAuth fields are empty", async () => {
        const onCheckSection = jest.fn().mockResolvedValue(undefined);
        const container = await renderForm({ specificationOverride: withConnectSectionCheck, onCheckSection });

        await clickSectionButton("Continue");

        await waitFor(() => expect(getSection("connect")).toHaveTextContent("Authentication required"));
        expect(document.activeElement).toHaveTextContent("Authentication required");
        expect(getOAuthButton(container)).toBeVisible();
        expect(onCheckSection).not.toHaveBeenCalled();

        await executeOAuthFlow(container);
        await clickSectionButton("Continue");

        await waitFor(() => expect(getInputByName(container, "connectionConfiguration.start_date")).toBeVisible());
        expect(onCheckSection).toHaveBeenCalledTimes(1);
        expect(onCheckSection.mock.calls[0][0].connectionConfiguration).toEqual(
          expect.objectContaining({
            credentials: expect.objectContaining({ access_token: "mytoken" }),
            "check-section": "connect",
          })
        );
      });
    });

    it("fails to build the form when a group is in two sections", async () => {
      jest.spyOn(console, "error").mockImplementation(() => undefined);
      const invalidSpec = {
        ...sectionedSpec,
        sections: [
          { id: "one", title: "One", order: 0, groups: ["db"] },
          { id: "two", title: "Two", order: 1, groups: ["db"] },
        ],
      };

      await expect(renderForm({ specificationOverride: { connectionSpecification: invalidSpec } })).rejects.toThrow(
        "connectorForm.error.invalidSections"
      );
    });

    it("orders sections by their order, not by their place in the list", async () => {
      const container = await renderForm({
        specificationOverride: {
          connectionSpecification: { ...sectionedSpec, sections: [...sectionedSpec.sections].reverse() },
        },
      });

      expect(getInputByName(container, "connectionConfiguration.host")).toBeVisible();
      expect(getInputByName(container, "name")).not.toBeVisible();
    });

    describe("footer", () => {
      const COMPLETE_ALL_STEPS = "Complete all steps before saving";
      const renderControlsFooter: ConnectorFormProps["renderFooter"] = ({
        dirty,
        isSubmitting,
        isValid,
        emptyRequiredFields,
        formElementId,
        canEdit,
        isEditMode,
      }) => (
        <Controls
          formType="source"
          isSubmitting={isSubmitting}
          isValid={isValid}
          emptyRequiredFields={emptyRequiredFields}
          dirty={dirty}
          onCancelClick={jest.fn()}
          onRetestClick={jest.fn()}
          onCancelTesting={jest.fn()}
          connectionTestSuccess={false}
          hasDefinition
          isEditMode={Boolean(isEditMode)}
          formElementId={formElementId}
          canEdit={canEdit}
        />
      );
      const getSaveAndTest = () => screen.getByRole("button", { name: "Save & Test" });
      const getStepCounter = () => screen.getByTestId("connector-form-section-progress");

      it("keeps Save & Test out of the section cards on every section, and enables it only on the last section", async () => {
        const container = await renderForm({
          specificationOverride: sectionedSpecification,
          renderFooter: renderControlsFooter,
        });

        expect(getStepCounter()).toHaveTextContent("Step 1 of 4");
        expect(screen.getByText("Step 1 of 4")).toHaveAttribute("role", "status");
        expect(getSaveAndTest()).toBeDisabled();
        expect(getSaveAndTest()).toHaveAccessibleDescription(COMPLETE_ALL_STEPS);
        expect(getSection("connection")).not.toContainElement(getSaveAndTest());

        await completeConnectionSection(container);
        await waitFor(() => expect(getStepCounter()).toHaveTextContent("Step 2 of 4"));
        expect(getSaveAndTest()).toBeDisabled();

        await userEvent.type(getInputByName(container, "connectionConfiguration.username")!, "airbyte_reader");
        await clickSectionButton("Continue");
        await waitFor(() => expect(getStepCounter()).toHaveTextContent("Step 3 of 4"));
        expect(getSaveAndTest()).toBeDisabled();

        await clickSectionButton("Continue");
        await waitFor(() => expect(getStepCounter()).toHaveTextContent("Step 4 of 4"));
        expect(getSaveAndTest()).toBeEnabled();
        expect(getSaveAndTest()).not.toHaveAccessibleDescription();
        expect(getSection("review")).not.toContainElement(getSaveAndTest());
        expect(getByRole(getSection("review"), "button", { name: "Back" })).toBeInTheDocument();
        expect(screen.queryByRole("button", { name: "Continue" })).not.toBeInTheDocument();

        await clickSectionButton("Back");
        await waitFor(() => expect(getStepCounter()).toHaveTextContent("Step 3 of 4"));
        expect(getSaveAndTest()).toBeDisabled();

        await clickSectionButton("Continue");
        await waitFor(() => expect(getSaveAndTest()).toBeEnabled());
        await userEvent.click(getSaveAndTest());
        await waitFor(() => expect(result).toBeDefined());
      });

      it("shows no step counter and gates Save & Test only on required fields in a form without sections", async () => {
        const container = await renderForm({
          specificationOverride: {
            connectionSpecification: {
              type: "object",
              required: ["host"],
              properties: { host: { type: "string", title: "Host" } },
            },
          },
          renderFooter: renderControlsFooter,
        });

        expect(screen.queryByTestId("connector-form-section-progress")).not.toBeInTheDocument();
        expect(getSaveAndTest()).toBeDisabled();
        expect(getSaveAndTest()).toHaveAccessibleDescription("Fill out required fields before saving: Host");

        await userEvent.type(getInputByName(container, "connectionConfiguration.host")!, "db.example.com");

        await waitFor(() => expect(getSaveAndTest()).toBeEnabled());
      });

      it("shows no step counter and does not gate Save & Test on sections in edit mode", async () => {
        await renderForm({
          specificationOverride: sectionedSpecification,
          formValuesOverride: {
            host: "db.example.com",
            port: 5432,
            database: "analytics",
            username: "airbyte_reader",
            replication_method: { method: "Xmin" },
          },
          renderFooter: renderControlsFooter,
        });

        expect(screen.queryByTestId("connector-form-section-progress")).not.toBeInTheDocument();
        expect(getSaveAndTest()).not.toHaveAccessibleDescription();
      });
    });

    describe("section check", () => {
      async function completeCredentialsSection(container: HTMLElement) {
        await waitFor(() => expect(getInputByName(container, "connectionConfiguration.username")).toBeVisible());
        await userEvent.type(getInputByName(container, "connectionConfiguration.username")!, "airbyte_reader");
        await clickSectionButton("Continue");
      }

      it("checks each section that is not the last with its id, and not the last section or the final submit", async () => {
        const onCheckSection = jest.fn().mockResolvedValue(undefined);
        const container = await renderForm({ specificationOverride: sectionedSpecification, onCheckSection });

        await completeConnectionSection(container);

        await waitFor(() => expect(onCheckSection).toHaveBeenCalledTimes(1));
        const connectionCheckValues: ConnectorFormValues = onCheckSection.mock.calls[0][0];
        expect(connectionCheckValues.connectionConfiguration).toEqual(
          expect.objectContaining({ host: "db.example.com", database: "analytics", "check-section": "connection" })
        );
        expect(connectionCheckValues.connectionConfiguration).not.toHaveProperty("username");

        await completeCredentialsSection(container);
        await waitFor(() => expect(onCheckSection).toHaveBeenCalledTimes(2));
        expect(onCheckSection.mock.calls[1][0].connectionConfiguration).toEqual(
          expect.objectContaining({
            host: "db.example.com",
            database: "analytics",
            username: "airbyte_reader",
            "check-section": "credentials",
          })
        );

        await waitFor(() => expect(screen.getByTestId("radio-option.0")).toBeVisible());
        await clickSectionButton("Continue");
        await waitFor(() => expect(getInputByName(container, "name")).toBeVisible());
        expect(onCheckSection).toHaveBeenCalledTimes(3);
        expect(onCheckSection.mock.calls[2][0].connectionConfiguration["check-section"]).toBe("sync_method");
        expect(screen.queryByRole("button", { name: "Continue" })).not.toBeInTheDocument();
        expect(getSection("review")).not.toContainElement(getSubmitButton(container) as HTMLElement);
        expect(getSection("review")).toContainElement(screen.getByRole("button", { name: "Back" }));

        await submitForm(container);

        await waitFor(() => expect(result).toBeDefined());
        expect(result?.connectionConfiguration).not.toHaveProperty("check-section");
        expect(onCheckSection).toHaveBeenCalledTimes(3);
      });

      it("does not check a section without check: true", async () => {
        const onCheckSection = jest.fn().mockResolvedValue(undefined);
        const container = await renderForm({
          specificationOverride: {
            connectionSpecification: {
              ...sectionedSpec,
              sections: sectionedSpec.sections.map((section) =>
                section.id === "credentials" ? { ...section, check: false } : section
              ),
            },
          },
          onCheckSection,
        });
        await completeConnectionSection(container);
        await waitFor(() => expect(onCheckSection).toHaveBeenCalledTimes(1));

        await completeCredentialsSection(container);

        await waitFor(() => expect(screen.getByTestId("radio-option.0")).toBeVisible());
        expect(onCheckSection).toHaveBeenCalledTimes(1);
      });

      it("leaves the fields of later sections out of the check", async () => {
        const onCheckSection = jest.fn().mockResolvedValue(undefined);
        const container = await renderForm({ specificationOverride: sectionedSpecification, onCheckSection });
        await completeConnectionSection(container);
        await waitFor(() => expect(getInputByName(container, "connectionConfiguration.username")).toBeVisible());
        await userEvent.type(getInputByName(container, "connectionConfiguration.username")!, "airbyte_reader");

        await clickSectionButton("Back");
        await waitFor(() => expect(getInputByName(container, "connectionConfiguration.host")).toBeVisible());
        await userEvent.clear(getInputByName(container, "connectionConfiguration.host")!);
        await userEvent.type(getInputByName(container, "connectionConfiguration.host")!, "replica.example.com");
        await clickSectionButton("Continue");

        await waitFor(() => expect(onCheckSection).toHaveBeenCalledTimes(2));
        const connectionConfiguration = onCheckSection.mock.calls[1][0].connectionConfiguration;
        expect(connectionConfiguration).toEqual(
          expect.objectContaining({ host: "replica.example.com", "check-section": "connection" })
        );
        expect(connectionConfiguration).not.toHaveProperty("username");
      });

      it("keeps the user on the section and shows the message when the check fails, and Back still works", async () => {
        const failure = "The 'credentials' section check requires the 'password' field.";
        const onCheckSection = jest
          .fn()
          .mockResolvedValueOnce(undefined)
          .mockRejectedValueOnce(new Error(failure))
          .mockResolvedValueOnce(undefined);
        const container = await renderForm({ specificationOverride: sectionedSpecification, onCheckSection });
        await completeConnectionSection(container);

        await completeCredentialsSection(container);

        await waitFor(() => expect(getSection("credentials")).toHaveTextContent(failure));
        expect(document.activeElement).toHaveTextContent(failure);
        expect(getInputByName(container, "connectionConfiguration.username")).toBeVisible();
        expect(screen.queryByTestId("radio-option.0")).not.toBeVisible();

        await clickSectionButton("Back");
        await waitFor(() => expect(getInputByName(container, "connectionConfiguration.host")).toBeVisible());
        await clickSectionButton("Continue");
        await waitFor(() => expect(getInputByName(container, "connectionConfiguration.username")).toBeVisible());

        await clickSectionButton("Continue");
        await waitFor(() => expect(screen.getByTestId("radio-option.0")).toBeVisible());
        expect(getSection("credentials")).not.toHaveTextContent(failure);
      });

      it("shows the running check and locks the navigation until it ends", async () => {
        let passCheck: () => void = () => undefined;
        const onCheckSection = jest
          .fn()
          .mockResolvedValueOnce(undefined)
          .mockImplementationOnce(
            () =>
              new Promise<void>((resolve) => {
                passCheck = resolve;
              })
          );
        const container = await renderForm({ specificationOverride: sectionedSpecification, onCheckSection });
        await completeConnectionSection(container);

        await completeCredentialsSection(container);
        await waitFor(() => expect(onCheckSection).toHaveBeenCalledTimes(2));
        const continueButton = screen.getByRole("button", { name: "Continue" });
        expect(continueButton).toBeDisabled();
        await userEvent.click(continueButton);
        expect(onCheckSection).toHaveBeenCalledTimes(2);
        expect(screen.getByRole("button", { name: "Back" })).toBeDisabled();
        expect(getEditButton("connection", "Connection")).toBeDisabled();
        expect(getByRole(getSection("credentials"), "status")).toHaveTextContent("Testing Credentials…");

        passCheck();

        await waitFor(() => expect(screen.getByTestId("radio-option.0")).toBeVisible());
        expect(screen.getByRole("button", { name: "Back" })).toBeEnabled();
        expect(getEditButton("connection", "Connection")).toBeEnabled();
      });

      it("shows the configuration check failure with its job details", async () => {
        const onCheckSection = jest.fn().mockRejectedValueOnce(
          new CommandErrorWithJobInfo("Could not connect", {
            id: "command-id",
            configType: JobConfigType.check_connection_source,
            failureReason: { externalMessage: "The host db.example.com is not reachable.", timestamp: 0 },
          })
        );
        const container = await renderForm({ specificationOverride: sectionedSpecification, onCheckSection });

        await completeConnectionSection(container);

        await waitFor(() => expect(getSection("connection")).toHaveTextContent("Configuration check failed"));
        expect(getSection("connection")).toHaveTextContent("The host db.example.com is not reachable.");
        expect(getInputByName(container, "connectionConfiguration.host")).toBeVisible();
      });

      describe("logs of a failed check", () => {
        const failedCheckWithLogs = new CommandErrorWithJobInfo("Could not connect", {
          id: "command-id",
          configType: JobConfigType.check_connection_source,
          logs: { logLines: ["Could not connect to db.example.com"] },
        });

        it("does not validate the later sections when the user views the logs", async () => {
          const onCheckSection = jest.fn().mockRejectedValueOnce(failedCheckWithLogs).mockResolvedValue(undefined);
          const container = await renderForm({ specificationOverride: sectionedSpecification, onCheckSection });
          await completeConnectionSection(container);
          await waitFor(() => expect(getSection("connection")).toHaveTextContent("Configuration check failed"));
          await userEvent.click(getSection("connection").querySelector("[data-icon='eye']")!.closest("button")!);
          await userEvent.click(await screen.findByTestId("close-modal-button"));

          await clickSectionButton("Continue");

          await waitFor(() => expect(getInputByName(container, "connectionConfiguration.username")).toBeVisible());
          expect(getSection("credentials")).not.toHaveTextContent("Required");
        });
      });

      describe("check passed badge", () => {
        const CHECK_PASSED_BADGE = "connector-form-section-check-passed";
        const ChangeField = ({ path, value }: { path: "host" | "username"; value: string }) => {
          const { setValue } = useFormContext<ConnectorFormValues>();
          return (
            <button type="button" onClick={() => setValue(`connectionConfiguration.${path}`, value)}>
              Change {path}
            </button>
          );
        };
        const ChangeConfiguration = () => {
          const { setValue, getValues } = useFormContext<ConnectorFormValues>();
          return (
            <button
              type="button"
              onClick={() =>
                setValue("connectionConfiguration", {
                  ...getValues().connectionConfiguration,
                  host: "replica.example.com",
                })
              }
            >
              Change configuration
            </button>
          );
        };

        async function renderAtSyncMethodSection(spec = sectionedSpec) {
          const onCheckSection = jest.fn().mockResolvedValue(undefined);
          const container = await renderForm({
            specificationOverride: { connectionSpecification: spec },
            onCheckSection,
            formAutomation: (
              <>
                <ChangeField path="host" value="replica.example.com" />
                <ChangeField path="username" value="airbyte_admin" />
                <ChangeConfiguration />
              </>
            ),
          });
          await completeConnectionSection(container);
          await completeCredentialsSection(container);
          await waitFor(() => expect(screen.getByTestId("radio-option.0")).toBeVisible());
          return onCheckSection;
        }

        it("shows a badge on a completed section whose check passed, and none on a section without a check", async () => {
          await renderAtSyncMethodSection({
            ...sectionedSpec,
            sections: sectionedSpec.sections.map((section) =>
              section.id === "credentials" ? { ...section, check: false } : section
            ),
          });

          expect(getByTestId(getSection("connection"), CHECK_PASSED_BADGE)).toHaveTextContent("Check passed");
          expect(queryByTestId(getSection("credentials"), CHECK_PASSED_BADGE)).not.toBeInTheDocument();
          expect(queryByTestId(getSection("sync_method"), CHECK_PASSED_BADGE)).not.toBeInTheDocument();
        });

        it("hides the badge of an open section", async () => {
          await renderAtSyncMethodSection();

          await userEvent.click(getEditButton("connection", "Connection"));

          await waitFor(() => expect(getSectionTitle("connection")).toHaveAttribute("aria-current", "step"));
          expect(queryByTestId(getSection("connection"), CHECK_PASSED_BADGE)).not.toBeInTheDocument();
        });

        it("clears the badges of the section whose field changes and of the sections after it", async () => {
          await renderAtSyncMethodSection();
          expect(getByTestId(getSection("connection"), CHECK_PASSED_BADGE)).toBeInTheDocument();
          expect(getByTestId(getSection("credentials"), CHECK_PASSED_BADGE)).toBeInTheDocument();

          await clickSectionButton("Change host");

          await waitFor(() =>
            expect(queryByTestId(getSection("connection"), CHECK_PASSED_BADGE)).not.toBeInTheDocument()
          );
          expect(queryByTestId(getSection("credentials"), CHECK_PASSED_BADGE)).not.toBeInTheDocument();
        });

        it("keeps the badges of the sections before the section whose field changes", async () => {
          await renderAtSyncMethodSection();

          await clickSectionButton("Change username");

          await waitFor(() =>
            expect(queryByTestId(getSection("credentials"), CHECK_PASSED_BADGE)).not.toBeInTheDocument()
          );
          expect(getByTestId(getSection("connection"), CHECK_PASSED_BADGE)).toBeInTheDocument();
        });

        it("continues without a new check from a section whose check passed and whose fields did not change", async () => {
          const onCheckSection = await renderAtSyncMethodSection();

          await userEvent.click(getEditButton("connection", "Connection"));
          await waitFor(() => expect(getSectionTitle("connection")).toHaveAttribute("aria-current", "step"));
          await clickSectionButton("Continue");
          await waitFor(() => expect(getSectionTitle("credentials")).toHaveAttribute("aria-current", "step"));
          await clickSectionButton("Continue");

          await waitFor(() => expect(screen.getByTestId("radio-option.0")).toBeVisible());
          expect(onCheckSection).toHaveBeenCalledTimes(2);
          expect(getByTestId(getSection("connection"), CHECK_PASSED_BADGE)).toBeInTheDocument();
          expect(getByTestId(getSection("credentials"), CHECK_PASSED_BADGE)).toBeInTheDocument();
        });

        it("checks every section again after the whole connector configuration changes", async () => {
          const onCheckSection = await renderAtSyncMethodSection();

          await clickSectionButton("Change configuration");
          await waitFor(() =>
            expect(queryByTestId(getSection("connection"), CHECK_PASSED_BADGE)).not.toBeInTheDocument()
          );
          await userEvent.click(getEditButton("connection", "Connection"));
          await clickSectionButton("Continue");

          await waitFor(() => expect(onCheckSection).toHaveBeenCalledTimes(3));
          expect(onCheckSection.mock.calls[2][0].connectionConfiguration).toEqual(
            expect.objectContaining({ host: "replica.example.com", "check-section": "connection" })
          );

          await waitFor(() => expect(getSectionTitle("credentials")).toHaveAttribute("aria-current", "step"));
          await clickSectionButton("Continue");

          await waitFor(() => expect(onCheckSection).toHaveBeenCalledTimes(4));
          expect(onCheckSection.mock.calls[3][0].connectionConfiguration).toEqual(
            expect.objectContaining({ host: "replica.example.com", "check-section": "credentials" })
          );
        });

        it("shows the badge again after the check of a changed section passes again", async () => {
          const onCheckSection = await renderAtSyncMethodSection();
          await clickSectionButton("Change host");
          await waitFor(() =>
            expect(queryByTestId(getSection("connection"), CHECK_PASSED_BADGE)).not.toBeInTheDocument()
          );

          await userEvent.click(getEditButton("connection", "Connection"));
          await clickSectionButton("Continue");

          await waitFor(() => expect(getByTestId(getSection("connection"), CHECK_PASSED_BADGE)).toBeInTheDocument());
          expect(onCheckSection).toHaveBeenCalledTimes(3);
        });
      });
    });

    it("renders edit mode as one page", async () => {
      const container = await renderForm({
        specificationOverride: sectionedSpecification,
        formValuesOverride: { host: "db.example.com", port: 5432, database: "analytics", username: "airbyte_reader" },
      });

      expect(getInputByName(container, "connectionConfiguration.host")).toBeVisible();
      expect(getInputByName(container, "connectionConfiguration.username")).toBeVisible();
      expect(getSubmitButton(container)).toBeInTheDocument();
      expect(screen.queryByRole("button", { name: "Continue" })).not.toBeInTheDocument();
    });
  });
});
