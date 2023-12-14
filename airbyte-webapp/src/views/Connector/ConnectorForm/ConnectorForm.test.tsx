/* eslint-disable @typescript-eslint/no-non-null-assertion */
import { getByTestId, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { BroadcastChannel } from "broadcast-channel";
import React from "react";
import selectEvent from "react-select-event";

import { render, useMockIntersectionObserver } from "test-utils/testutils";

import { useCompleteOAuth } from "core/api";
import { DestinationDefinitionSpecificationRead } from "core/api/types/AirbyteClient";
import { ConnectorDefinition, ConnectorDefinitionSpecification } from "core/domain/connector";
import { AirbyteJSONSchema } from "core/jsonSchema/types";
import { FeatureItem } from "core/services/features";
import { OAUTH_BROADCAST_CHANNEL_NAME } from "hooks/services/useConnectorAuth";
import { ConnectorForm } from "views/Connector/ConnectorForm";

import { ConnectorFormValues } from "./types";
import { DocumentationPanelContext } from "../ConnectorDocumentationLayout/DocumentationPanelContext";

// hack to fix tests. https://github.com/remarkjs/react-markdown/issues/635
jest.mock("components/ui/Markdown", () => ({ children }: React.PropsWithChildren<unknown>) => <>{children}</>);

jest.mock("core/api", () => ({
  useDestinationList: () => ({ destinations: [] }),
  useConsentUrls: () => ({ getSourceConsentUrl: () => "http://example.com" }),
  useCompleteOAuth: jest.fn(() => ({
    completeSourceOAuth: () => Promise.resolve({}),
    completeDestinationOAuth: () => Promise.resolve({}),
  })),
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

const oauthPopupIdentifier = "123456789";
jest.mock("uuid", () => ({ v4: () => oauthPopupIdentifier }));

const nextTick = () => new Promise((r) => setTimeout(r, 0));

const connectorDefinition = {
  sourceDefinitionId: "1",
  documentationUrl: "",
} as ConnectorDefinition;

const useAddPriceListItem = (container: HTMLElement, initialIndex = 0) => {
  const priceList = getByTestId(container, "connectionConfiguration.priceList");
  let index = initialIndex;

  return async (name: string, price: string, originType: "city" | "country", origin: string) => {
    const addButton = getByTestId(priceList, "addItemButton");
    await userEvent.click(addButton);

    const getPriceListInput = (index: number, key: string) =>
      priceList.querySelector(`input[name='connectionConfiguration.priceList.${index}.${key}']`);

    // Type items into input
    const nameInput = getPriceListInput(index, "name");
    await userEvent.type(nameInput!, name);

    const priceInput = getPriceListInput(index, "price");
    await userEvent.type(priceInput!, price);

    const selectContainer = getByTestId(priceList, `connectionConfiguration.priceList.${index}.origin`);
    await waitFor(() =>
      selectEvent.select(selectContainer, originType, {
        container: document.body,
      })
    );

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
    airbyte_oauth_popup_identifier: oauthPopupIdentifier,
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

jest.mock("hooks/services/AppMonitoringService");

jest.mock("hooks/services/useWorkspace", () => ({
  useCurrentWorkspace: () => ({
    workspace: {
      workspaceId: "workspaceId",
    },
  }),
}));

describe("Connector form", () => {
  let result: ConnectorFormValues | undefined;

  async function renderForm({
    disableOAuth,
    formValuesOverride,
    propertiesOverride,
    specificationOverride,
  }: {
    disableOAuth?: boolean;
    formValuesOverride?: Record<string, unknown>;
    specificationOverride?: Partial<ConnectorDefinitionSpecification>;
    propertiesOverride?: Record<string, AirbyteJSONSchema>;
  } = {}) {
    const renderResult = await render(
      <ConnectorForm
        formType="source"
        formValues={{ name: "test-name", connectionConfiguration: { ...formValuesOverride } }}
        onSubmit={async (values) => {
          result = values;
        }}
        isEditMode={Boolean(formValuesOverride)}
        renderFooter={() => <button type="submit">Submit</button>}
        selectedConnectorDefinition={connectorDefinition}
        selectedConnectorDefinitionSpecification={
          // @ts-expect-error Partial objects for testing
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
          } as DestinationDefinitionSpecificationRead
        }
      />,
      undefined,
      disableOAuth ? undefined : [FeatureItem.AllowOAuthConnector]
    );
    return renderResult.container;
  }

  beforeEach(async () => {
    result = undefined;
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
      const credentials = container.querySelector("div[data-testid='connectionConfiguration.credentials']");
      const apiKey = getInputByName(container, "connectionConfiguration.credentials.api_key");
      expect(credentials).toBeInTheDocument();
      expect(credentials?.getAttribute("role")).toEqual("combobox");
      expect(apiKey).toBeInTheDocument();
    });

    it("should display array of simple entity field", () => {
      const emails = getInputByName(container, "connectionConfiguration.emails");
      expect(emails).toBeInTheDocument();
    });

    it("should display array with items list field", () => {
      const workTime = container.querySelector("div[name='connectionConfiguration.workTime']");
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
      expect(getInputByName(container, "connectionConfiguration.additional_separate_group")).not.toBeVisible();
      expect(getInputByName(container, "connectionConfiguration.additional_same_group")).not.toBeVisible();

      await userEvent.click(screen.getAllByTestId("optional-fields").at(0)!);

      expect(getInputByName(container, "connectionConfiguration.additional_separate_group")).toBeVisible();

      await userEvent.click(screen.getAllByTestId("optional-fields").at(1)!);

      expect(getInputByName(container, "connectionConfiguration.additional_same_group")).toBeVisible();

      const input1 = getInputByName(container, "connectionConfiguration.additional_same_group");
      const input2 = getInputByName(container, "connectionConfiguration.additional_separate_group");
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

      const selectContainer = getByTestId(container, "connectionConfiguration.condition");
      await selectEvent.select(selectContainer, "option2", {
        container: document.body,
      });

      const selectContainer2 = getByTestId(container, "connectionConfiguration.condition.nestedcondition");
      await selectEvent.select(selectContainer2, "nestedoption2", {
        container: document.body,
      });

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
      await waitFor(() => userEvent.click(screen.getByTestId("edit-secret")!));
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
      const workTime = container.querySelector("div[name='connectionConfiguration.workTime']");
      await userEvent.type(workTime!.querySelector("input")!, "day");
      await userEvent.click(workTime!.querySelector(".rw-popup [role='option']")!);
      await userEvent.type(workTime!.querySelector("input")!, "abc");
      await userEvent.click(workTime!.querySelector(".rw-popup [role='option']")!);
      await userEvent.type(workTime!.querySelector("input")!, "ni");
      await userEvent.click(workTime!.querySelector(".rw-popup [role='option']")!);

      await submitForm(container);

      expect(result?.connectionConfiguration).toEqual({ ...filledForm, workTime: ["day", "night"] });
    });

    it("should add values in array with items list field", async () => {
      const container = await renderForm({ formValuesOverride: { ...filledForm } });
      const workTime = container.querySelector("div[name='connectionConfiguration.workTime']");
      await userEvent.type(workTime!.querySelector("input")!, "ni");
      await userEvent.click(workTime!.querySelector(".rw-popup [role='option']")!);

      await submitForm(container);

      expect(result?.connectionConfiguration).toEqual({ ...filledForm, workTime: ["day", "night"] });
    });

    it("change oneOf field value", async () => {
      const container = await renderForm({ formValuesOverride: { ...filledForm, credentials: { type: "api" } } });
      const apiKey = getInputByName(container, "connectionConfiguration.credentials.api_key");
      expect(apiKey).toBeInTheDocument();

      const selectContainer = getByTestId(container, "connectionConfiguration.credentials");

      await waitFor(() =>
        selectEvent.select(selectContainer, "oauth", {
          container: document.body,
        })
      );

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
      const selectContainer = getByTestId(container, "connectionConfiguration.credentials");

      await waitFor(() =>
        selectEvent.select(selectContainer, "oauth", {
          container: document.body,
        })
      );

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
        specificationOverride?: Partial<ConnectorDefinitionSpecification>;
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
    it("should render regular inputs for auth fields", async () => {
      const container = await renderNewOAuthForm({ disableOAuth: true });
      expect(getInputByName(container, "connectionConfiguration.credentials.access_token")).toBeInTheDocument();
      expect(getOAuthButton(container)).not.toBeInTheDocument();
    });

    it("should render the oauth button", async () => {
      const container = await renderNewOAuthForm();
      expect(getOAuthButton(container)).toBeInTheDocument();
      expect(getInputByName(container, "connectionConfiguration.credentials.access_token")).not.toBeInTheDocument();
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

      const submit = getSubmitButton(container);
      await waitFor(() => userEvent.click(submit!));

      expect(result?.connectionConfiguration).toEqual({
        credentials: { access_token: "mytoken", option_title: "OAuth Credentials" },
      });
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
      const selectContainer = getByTestId(container, "connectionConfiguration.credentials");

      await waitFor(() =>
        selectEvent.select(selectContainer, "Personal Access Token", {
          container: document.body,
        })
      );
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
});
