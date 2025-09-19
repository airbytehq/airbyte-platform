import { APIRequestContext, Page, expect } from "@playwright/test";
import destinationIds from "@src/area/connector/utils/destinations.json";
import sourceIds from "@src/area/connector/utils/sources.json";
import { ActorListCursorPaginatedRequestBody } from "@src/core/api/generated/AirbyteClient.schemas";

import { getApiBaseUrl } from "./api";
import { mockHelpers } from "./mocks";
import {
  fillFormField,
  submitFormAndWaitForCreation,
  navigateToConnectorCreation,
  selectConnectorFromMarketplace,
} from "./ui";

// This file contains helper methods for connector CRUD operations

// Connector configuration definitions for test connectors
const CONNECTOR_CONFIGS = {
  source: {
    endpoint: "sources",
    definitionId: sourceIds.PokeApi,
    idField: "sourceId",
    responseKey: "sources",
    defaultConfig: {
      pokemon_name: "venusaur",
    },
  },
  destination: {
    endpoint: "destinations",
    definitionId: destinationIds.EndToEndTesting,
    idField: "destinationId",
    responseKey: "destinations",
    defaultConfig: {
      test_destination: {
        test_destination_type: "LOGGING",
        logging_config: {
          logging_type: "FirstN",
          max_entry_count: 100,
        },
      },
    },
  },
} as const;

// Create a connector API interface for the specified connector type
// This factory function generates type-safe API methods for sources or destinations
export const createConnectorAPI = (connectorType: keyof typeof CONNECTOR_CONFIGS) => {
  const config = CONNECTOR_CONFIGS[connectorType];
  const apiBaseUrl = getApiBaseUrl();

  return {
    // Create a new connector via API
    create: async (request: APIRequestContext, name: string, workspaceId: string) => {
      const createData = {
        name,
        [`${connectorType}DefinitionId`]: config.definitionId,
        workspaceId,
        connectionConfiguration: config.defaultConfig,
      };

      const response = await request.post(`${apiBaseUrl}/${config.endpoint}/create`, {
        data: createData,
      });

      if (!response.ok()) {
        throw new Error(`Failed to create ${connectorType}: ${response.status()}`);
      }

      const result = await response.json();
      return result;
    },

    // Delete a connector via API
    delete: async (request: APIRequestContext, connectorId: string) => {
      const deleteRequest = {
        [config.idField]: connectorId,
      };

      const response = await request.post(`${apiBaseUrl}/${config.endpoint}/delete`, {
        data: deleteRequest,
      });

      if (!response.ok()) {
        throw new Error(`Failed to delete ${connectorType}: ${response.status()}`);
      }
    },

    // List existing connectors for a workspace
    list: async (request: APIRequestContext, workspaceId: string) => {
      try {
        const listRequest: ActorListCursorPaginatedRequestBody = {
          workspaceId,
        };

        const response = await request.post(`${apiBaseUrl}/${config.endpoint}/list`, {
          data: listRequest,
        });

        if (response.ok()) {
          const result = await response.json();
          return result[config.responseKey];
        }
      } catch (error) {
        // Warn on listing errors
        console.warn(`⚠️ Failed to list ${connectorType}s:`, error);
      }
      return [];
    },
  };
};

// Pre-configured API instances. Example usage:
// await destinationAPI.create(request, name, workspaceId)
// await destinationAPI.delete(request, destinationId)
// await destinationAPI.list(request, workspaceId)
export const sourceAPI = createConnectorAPI("source");
export const destinationAPI = createConnectorAPI("destination");

// UI helper functions for destination operations
export const destinationUI = {
  navigateToDestinationPage: async (page: Page, workspaceId: string) => {
    await page.goto(`/workspaces/${workspaceId}/destination`, { timeout: 10000 });
  },

  createViaUI: async (page: Page, name: string, workspaceId: string) => {
    // Setup all required mocks
    await mockHelpers.mockEnterpriseConnectors(page);
    await mockHelpers.mockConnectionCheck(page, "destination");

    // Navigate to destination creation page
    await navigateToConnectorCreation(page, "destination", workspaceId);

    // Search for and select E2E Testing connector
    await selectConnectorFromMarketplace(page, "end", "End-to-End Testing (/dev/null)");

    // Fill in the destination name
    await fillFormField(page, "input[name='name']", name);

    // Submit and wait for creation
    return await submitFormAndWaitForCreation(page, "destination");
  },

  updateDestination: async (
    page: Page,
    destinationName: string,
    fieldName: string,
    value: string,
    workspaceId: string
  ) => {
    // Navigate to destinations page
    await destinationUI.navigateToDestinationPage(page, workspaceId);

    // Click on the destination by name - use the specific link within the destinations table
    const destinationLink = page.getByTestId("destinationsTable").getByRole("link", { name: destinationName }).first();
    await destinationLink.waitFor({ state: "visible", timeout: 15000 });
    await destinationLink.click();

    // Wait for the form to be fully loaded with existing values
    await page.waitForSelector('div:has-text("Test Destination")', { timeout: 15000 });
    await page.waitForSelector('div:has-text("Logging Configuration")', { timeout: 15000 });
    await page.waitForSelector(`input[name='${fieldName}']`, { timeout: 15000 });

    // Wait for the field to have the expected current value (100) before updating
    await expect(page.locator(`input[name='${fieldName}']`)).toHaveValue("100", { timeout: 15000 });

    // Update the field
    const fieldInput = page.locator(`input[name='${fieldName}']`);
    await fieldInput.waitFor({ state: "visible", timeout: 10000 });
    await fieldInput.clear();
    await fieldInput.fill(value);

    // Submit the form
    const submitButton = page.locator("button[type='submit']");
    await submitButton.waitFor({ state: "visible", timeout: 10000 });
    await submitButton.click();

    // Wait for success indicator (can be either div or FlexContainer)
    await expect(page.locator("[data-id='success-result']")).toBeVisible({ timeout: 40000 });
  },

  deleteDestination: async (page: Page, destinationName: string, workspaceId: string) => {
    // Navigate to destinations page
    await destinationUI.navigateToDestinationPage(page, workspaceId);

    // Click on the destination by name - use the specific link within the destinations table
    const destinationLink = page.getByTestId("destinationsTable").getByRole("link", { name: destinationName }).first();
    await destinationLink.waitFor({ state: "visible", timeout: 15000 });
    await destinationLink.click();

    // Click delete button
    const deleteButton = page.locator("button[data-id='open-delete-modal']");
    await deleteButton.waitFor({ state: "visible", timeout: 10000 });
    await deleteButton.click();

    // Get the confirmation text from placeholder and enter it
    const confirmationInput = page.locator("input[id='confirmation-text']");
    await confirmationInput.waitFor({ state: "visible", timeout: 10000 });
    const placeholder = await confirmationInput.getAttribute("placeholder");
    if (placeholder) {
      await confirmationInput.fill(placeholder);
    }

    // Confirm deletion
    const confirmButton = page.locator("button[data-id='delete']");
    await confirmButton.waitFor({ state: "visible", timeout: 10000 });
    await confirmButton.click();
  },
};

// UI helper functions for source operations
export const sourceUI = {
  navigateToSourcePage: async (page: Page, workspaceId: string) => {
    await page.goto(`/workspaces/${workspaceId}/source`, { timeout: 10000 });
  },

  createViaUI: async (page: Page, name: string, workspaceId: string) => {
    // Setup all required mocks
    await mockHelpers.mockEnterpriseConnectors(page);
    await mockHelpers.mockConnectionCheck(page, "source");

    // Navigate to source creation page
    await navigateToConnectorCreation(page, "source", workspaceId);

    // Search for and select PokeAPI connector
    await selectConnectorFromMarketplace(page, "poke", "PokeAPI");

    // Fill in the source name
    await fillFormField(page, "input[name='name']", name);

    // Fill in the pokemon name (PokeAPI specific config)
    const pokemonDropdown = page.locator("[data-testid='connectionConfiguration.pokemon_name-listbox-button']");
    await pokemonDropdown.waitFor({ state: "visible", timeout: 10000 });
    await pokemonDropdown.click();

    // Select bulbasaur from the dropdown options
    const bulbasaurOption = page.locator("li").filter({ hasText: "bulbasaur" });
    await bulbasaurOption.waitFor({ state: "visible", timeout: 10000 });
    await bulbasaurOption.click();

    // Submit and wait for creation
    return await submitFormAndWaitForCreation(page, "source");
  },

  updateSource: async (page: Page, sourceName: string, fieldName: string, value: string, workspaceId: string) => {
    // Setup connection check mock for update
    await mockHelpers.mockConnectionCheck(page, "source");

    // Navigate to sources page
    await sourceUI.navigateToSourcePage(page, workspaceId);

    // Click on the source by name - use the specific link within the sources table
    const sourceLink = page.getByTestId("sourcesTable").getByRole("link", { name: sourceName }).first();
    await sourceLink.waitFor({ state: "visible", timeout: 15000 });
    await sourceLink.click();

    // Wait for the form to be fully loaded with existing values
    await page.waitForSelector("[data-testid='connectionConfiguration.pokemon_name-listbox-button']", {
      timeout: 15000,
    });

    // Wait for the field to have the current value before updating
    await expect(page.locator("[data-testid='connectionConfiguration.pokemon_name-listbox-button']")).toBeVisible({
      timeout: 15000,
    });

    // Update the pokemon field (dropdown)
    const pokemonDropdown = page.locator("[data-testid='connectionConfiguration.pokemon_name-listbox-button']");
    await pokemonDropdown.waitFor({ state: "visible", timeout: 10000 });
    await pokemonDropdown.click();

    // Select the new pokemon from dropdown
    const pokemonOption = page.locator("li").filter({ hasText: value });
    await pokemonOption.waitFor({ state: "visible", timeout: 10000 });
    await pokemonOption.click();

    // Submit the form
    const submitButton = page.locator("button[type='submit']");
    await submitButton.waitFor({ state: "visible", timeout: 10000 });
    await submitButton.click();

    // Wait for success indicator (can be either div or FlexContainer)
    await expect(page.locator("[data-id='success-result']")).toBeVisible({ timeout: 90000 });
  },

  deleteSource: async (page: Page, sourceName: string, workspaceId: string) => {
    // Navigate to sources page
    await sourceUI.navigateToSourcePage(page, workspaceId);

    // Click on the source by name - use the specific link within the sources table
    const sourceLink = page.getByTestId("sourcesTable").getByRole("link", { name: sourceName }).first();
    await sourceLink.waitFor({ state: "visible", timeout: 15000 });
    await sourceLink.click();

    // Click delete button
    const deleteButton = page.locator("button[data-id='open-delete-modal']");
    await deleteButton.waitFor({ state: "visible", timeout: 10000 });
    await deleteButton.click();

    // Get the confirmation text from placeholder and enter it
    const confirmationInput = page.locator("input[id='confirmation-text']");
    await confirmationInput.waitFor({ state: "visible", timeout: 10000 });
    const placeholder = await confirmationInput.getAttribute("placeholder");
    if (placeholder) {
      await confirmationInput.fill(placeholder);
    }

    // Confirm deletion
    const confirmButton = page.locator("button[data-id='delete']");
    await confirmButton.waitFor({ state: "visible", timeout: 10000 });
    await confirmButton.click();
  },
};
