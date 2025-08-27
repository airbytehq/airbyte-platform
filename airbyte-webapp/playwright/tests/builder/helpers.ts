import { Page } from "@playwright/test";

// Selects a dropdown option within a field container by clicking on a span first, then the target option
export const selectDropdownOption = async (
  page: Page,
  fieldPath: string,
  initialOption: string,
  targetOption: string
) => {
  await page.locator(`div[data-field-path="${fieldPath}"]`).locator(`span:text-is("${initialOption}")`).click();
  await page.locator(`p:text-is("${targetOption}")`).click();
};

// Helper function to find and fill nested input fields by field path
export const fillInputByLabel = async (page: Page, fieldPath: string, value: string) => {
  const label = page.locator(`label[for="${fieldPath}"]`);
  await label.waitFor({ state: "visible", timeout: 10000 });
  if ((await label.count()) > 0) {
    // First try to find a standard input container
    const inputContainer = label.locator('xpath=../..//div[@data-testid="input-container"]');
    const standardInput = inputContainer.locator('[data-testid="input"]');

    if ((await standardInput.count()) > 0) {
      // Standard HTML input found
      await standardInput.waitFor({ state: "visible", timeout: 5000 });
      await standardInput.fill(value);
    }

    // If no standard input, look for Monaco Editor (JinjaInput)
    const monacoEditor = label.locator("xpath=../..//section").locator(".monaco-editor textarea");
    if ((await monacoEditor.count()) > 0) {
      // Monaco Editor found - focus and type into it
      await monacoEditor.waitFor({ state: "visible", timeout: 5000 });
      await monacoEditor.click();
      await monacoEditor.fill(value);
    }
  }
};

// Useful for ensuring that a name is unique from one test run to the next
export const appendRandomString = (string: string) => {
  const randomString = Math.random().toString(36).substring(2, 10);
  return `${string} _${randomString}`;
};

// Opens a builder connector project and configures the /items endpoint stream
export const initializeBuilderConnector = async (page: Page) => {
  // Navigate to the connector builder
  await page.goto("/connector-builder/create", { waitUntil: "networkidle" });

  // Select the "Start from scratch" option
  await page.locator('button[data-testid="start-from-scratch"]').click({ timeout: 10000 });
  await page.waitForSelector('button[data-testid="start-from-scratch"]', { state: "hidden" });
  await page.waitForSelector('button[data-testid="connector-name-label"]', { timeout: 45000 });
  await page.click('button[data-testid="connector-name-label"]');
  await page.locator('input[data-testid="connector-name-input"]').fill(appendRandomString("dummy_api"));
};

// Configures the stream name and URL for the connector
export const configureStream = async (page: Page) => {
  const streamNameField = page.locator('input[data-field-path="manifest.streams.0.name"]');
  await streamNameField.click();
  await streamNameField.fill("Items");

  const dummyApiHost = getDummyApiHost();

  await page
    .locator('div[data-field-path="manifest.streams.0.retriever.requester.url"]')
    .locator("textarea")
    .fill(`${dummyApiHost}/items/`, { timeout: 5000 });

  await page
    .locator('input[data-testid="tag-input-manifest.streams.0.retriever.record_selector.extractor.field_path"]')
    .fill("items");
};

const getDummyApiHost = () => {
  const mockApiServerHost = process.env.MOCK_API_SERVER_HOST;
  return (
    mockApiServerHost || (process.platform === "darwin" ? "http://host.docker.internal:6767" : "http://172.17.0.1:6767")
  );
};

// Configures a valid auth setup for the connector
export const configureValidAuth = async (page: Page) => {
  await page.click('label[for="manifest.streams.0.retriever.requester.authenticator"]');
  await selectDropdownOption(
    page,
    "manifest.streams.0.retriever.requester.authenticator",
    "API Key Authenticator",
    "Bearer Token Authenticator"
  );
  await page.click('button[data-testid="navbutton-inputs"]');
  await page.locator('input[data-field-path="testingValues.api_key"]').fill("theauthkey");
  await page.locator('input[data-field-path="testingValues.api_key"]').blur();
  await page.click('button[data-testid="navbutton-0"]');
};

// Configures an invalid NoAuth setup for the connector
export const configureInvalidAuth = async (page: Page) => {
  await selectDropdownOption(
    page,
    "manifest.streams.0.retriever.requester.authenticator",
    "Bearer Token Authenticator",
    "No Authentication"
  );
};

export const configurePagination = async (page: Page) => {
  await page.click('button[data-testid="navbutton-0"]');
  await page.click('label[for="manifest.streams.0.retriever.paginator"]');
  await selectDropdownOption(
    page,
    "manifest.streams.0.retriever.paginator.pagination_strategy",
    "Page Increment",
    "Offset Increment"
  );

  await page.locator('label[for="manifest.streams.0.retriever.paginator.pagination_strategy.page_size"]').click();
  await page
    .locator('input[data-field-path="manifest.streams.0.retriever.paginator.pagination_strategy.page_size"]')
    .fill("2");

  await page.locator('label[for="manifest.streams.0.retriever.paginator.page_size_option"]').click();
  await page.locator('[data-field-path="manifest.streams.0.retriever.paginator.page_size_option.inject_into"]').click();
  await page.locator('p:text-is("Header")').click();

  await fillInputByLabel(page, "manifest.streams.0.retriever.paginator.page_size_option.field_name", "limit");

  await page.locator('label[for="manifest.streams.0.retriever.paginator.page_token_option"]').click();
  await page
    .locator('[data-field-path="manifest.streams.0.retriever.paginator.page_token_option.inject_into"]')
    .click();
  await page.locator('p:text-is("Header")').click();

  await fillInputByLabel(page, "manifest.streams.0.retriever.paginator.page_token_option.field_name", "offset");
};

export const configureListPartitionRouter = async (page: Page, numberOfParameters: number) => {
  await page.click('button[data-testid="navbutton-0"]');

  // Enable partition router (checkbox should already be checked based on UI)
  await page.locator('label[for="manifest.streams.0.retriever.partition_router"]').click();

  // Configure the partition router type to "List Partition Router"
  await selectDropdownOption(
    page,
    "manifest.streams.0.retriever.partition_router",
    "Substream Partition Router",
    "List Partition Router"
  );

  // Set "Current Partition Value Identifier" to "item_id"
  await page
    .locator('[data-field-path="manifest.streams.0.retriever.partition_router.cursor_field"] textarea.inputarea')
    .fill("item_id");

  // First select the dropdown to "array of string" to allow multiple values
  await page
    .locator('[data-field-path="manifest.streams.0.retriever.partition_router.values"]')
    .locator('button:has-text("string")')
    .click();
  await page.locator('p:text-is("array of string")').click();

  // Now add values one by one to the tag input (type value, press enter, repeat)
  const valuesArray = Array.from(Array(numberOfParameters).keys());
  const tagInput = page.locator('[data-testid="tag-input-manifest.streams.0.retriever.partition_router.values"]');
  await tagInput.waitFor({ state: "visible", timeout: 5000 });
  await tagInput.click();

  for (const value of valuesArray) {
    await tagInput.pressSequentially(value.toString());
    await tagInput.press("Enter");
  }
};

// By default, we set the URL to the dummy /items endpoint for tests during setup.
// This function updates the queried endpoint when needed, without modifying the base URL.
export const updateUrlPath = async (page: Page, urlPath: string) => {
  const urlPathField = page.locator('div[data-field-path="manifest.streams.0.retriever.requester.url"] textarea');
  await urlPathField.click();

  const currentUrl = await urlPathField.inputValue();
  const baseUrl = currentUrl.replace("/items/", "/");
  const newUrl = baseUrl + urlPath;

  await urlPathField.fill(newUrl);
};

// Runs the stream test
export const testStream = async (page: Page) => {
  await page.click('button[data-testid="read-stream"]');

  // Wait for the test to complete by waiting for the button to stop being in loading state
  await page
    .locator('button[data-testid="read-stream"]:not([aria-busy="true"])')
    .waitFor({ state: "visible", timeout: 10000 });
};

// Waits for the draft to save by waiting for the saving indicator to disappear
export const waitForDraftToSave = async (page: Page) => {
  // Wait for the "Saving draft" indicator to disappear
  await page.locator('text="Draft saved"').waitFor({ state: "visible", timeout: 10000 });
};

// Navigate to the stream schema tab
export const navigateToStreamSchema = async (page: Page) => {
  await page.click('button[data-testid="navbutton-0"]');
  await page.click('button[data-testid="tag-tab-stream-schema"]');
};

// Configure schema loader with custom schema
export const configureSchemaLoader = async (page: Page, schema: string) => {
  await page.locator('label:has-text("Automatically import detected schema")').click();
  await page.locator('label:has-text("Schema Loader")').click();
  await page.locator('label[for="manifest.streams.0.schema_loader.schema"]').click();

  // Wait for the Monaco editor to be ready and fill it with the custom schema
  await page.locator('[data-field-path="manifest.streams.0.schema_loader.schema"] textarea.inputarea').fill(schema);
};

// Navigate to the detected schema tab
export const navigateToDetectedSchema = async (page: Page) => {
  await page.locator('[data-testid="tag-tab-detected-schema"]').waitFor({ state: "visible", timeout: 10000 });
  await page.locator('[data-testid="tag-tab-detected-schema"]').click();
};

// Returns the name of the current builder connector
export const getBuilderConnectorName = async (page: Page) => {
  const name = await page.locator('button[data-testid="connector-name-label"]').first().textContent();
  return name;
};

// TODO: Uncomment this when we add test cases for publish flow
// const publishProject = async (page: Page) => {
//   await page.click('button[data-testid="publish-button"]');
//   await page.locator('[role="dialog"] button[type="submit"]').click();
// };

export const AUTO_IMPORTED_SCHEMA = `{
  "$schema": "http://json-schema.org/schema#",
  "additionalProperties": true,
  "properties": {
    "name": {
      "type": [
        "string",
        "null"
      ]
    }
  },
  "type": "object"
}`;

export const SCHEMA_WITH_MISMATCH =
  '{"$schema": "http://json-schema.org/schema#", "properties": {"sadness": {"type": "string"}}, "type": "object"}';
