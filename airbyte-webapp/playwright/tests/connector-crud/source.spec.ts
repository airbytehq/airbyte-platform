/**
 * Source tests: create, edit and delete a source, and leave the new source page.
 *
 * Every test runs against the mock backend in playwright/mock. Each test starts from the backend state in its own
 * scenario, so no test depends on what an earlier test did.
 *
 * Each test is one user journey. Its `test.step` blocks continue on the same page, in order. Checks that only read
 * the page use `expect.soft`, so one failed check does not hide the checks after it.
 */
import type { Page } from "@playwright/test";

import { fillFormField, navigateToConnectorCreation, selectConnectorFromMarketplace } from "../../helpers/ui";
import { checkOutput, defs, expect, scenario, source, test } from "../../mock";

const { sourceDefinitionId } = defs.pokeApiSource.input.definition;

const pokemonDropdown = (page: Page) =>
  page.locator("[data-testid='connectionConfiguration.pokemon_name-listbox-button']");

/** Opens the Pokémon dropdown of the PokeAPI form and selects one Pokémon. */
async function selectPokemon(page: Page, pokemon: string) {
  await pokemonDropdown(page).click();
  await page.locator("li").filter({ hasText: pokemon }).click();
}

/** Selects PokeAPI in the marketplace, and fills in the name and the Pokémon of the new source. */
async function fillPokeApiForm(page: Page, name: string) {
  await selectConnectorFromMarketplace(page, "poke", "PokeAPI");
  await fillFormField(page, "input[name='name']", name);
  await selectPokemon(page, "bulbasaur");
}

/** The ID of the source whose page is open, from the URL. */
function sourceIdInUrl(page: Page) {
  return page.url().match(/\/source\/([a-f0-9-]{36})/)?.[1];
}

const submitButton = (page: Page) => page.locator("button[type='submit']");
const unsavedChangesModal = (page: Page) => page.locator("[data-testid='confirmationModal']");
const connectionsNavLink = (page: Page) =>
  page
    .locator("nav")
    .getByRole("link", { name: /connections/i })
    .first();

test.describe("New source", () => {
  test.describe(() => {
    // An empty workspace with PokeAPI in the marketplace. Every connection check succeeds.
    test.use({ airbyteScenario: scenario({ sourceDefinitions: [defs.pokeApiSource] }) });

    test("create the first source of a workspace", async ({ page, airbyte }) => {
      await test.step("open the sources page", async () => {
        await page.goto(`/workspaces/${airbyte.workspaceId}/source`);
      });

      await test.step("the new source page opens, because the workspace has no source", async () => {
        await expect(page).toHaveURL(/\/source\/new-source$/);
        await expect.soft(page.locator("h2")).toContainText("Set up a new source");
      });

      await test.step("fill in the PokeAPI source form", async () => {
        await fillPokeApiForm(page, "PokeAPI source");
      });

      await test.step("submit the form", async () => {
        await submitButton(page).click();
        await expect(page).toHaveURL(/\/source\/[a-f0-9-]{36}\/connections$/);
      });

      await test.step("the webapp saved a draft source, checked it, and opened its page", async () => {
        const sourceId = sourceIdInUrl(page);
        expect.soft(airbyte.calls("createSource")).toEqual([
          {
            name: "PokeAPI source",
            sourceDefinitionId,
            workspaceId: airbyte.workspaceId,
            connectionConfiguration: { pokemon_name: "bulbasaur" },
            createAsDraft: true,
          },
        ]);
        expect.soft(airbyte.calls("runCheckCommand")).toEqual([{ id: expect.any(String), actor_id: sourceId }]);
      });
    });
  });

  test.describe(() => {
    // An empty workspace with PokeAPI in the marketplace. Every connection check fails. A failed check keeps the user on
    // the form with unsaved changes, so the test can then try to leave the page.
    test.use({
      airbyteScenario: scenario({
        sourceDefinitions: [defs.pokeApiSource],
        outcomes: { check: checkOutput.failed({ message: "Something went wrong" }) },
      }),
    });

    test("leave the new source page, with and without unsaved changes", async ({ page, airbyte }) => {
      await test.step("open the new source page, then click Connections in the sidebar without changes", async () => {
        await navigateToConnectorCreation(page, "source", airbyte.workspaceId);
        await connectionsNavLink(page).click();
      });

      await test.step("the connections page opens without the unsaved changes modal", async () => {
        await expect.soft(page).toHaveURL(/\/connections$/);
        await expect.soft(unsavedChangesModal(page)).toBeHidden();
      });

      await test.step("open the new source page again, and fill in the PokeAPI source form", async () => {
        await navigateToConnectorCreation(page, "source", airbyte.workspaceId);
        await fillPokeApiForm(page, "testName");
      });

      await test.step("submit the form", async () => {
        await submitButton(page).click();
      });

      await test.step("the failure message of the check shows", async () => {
        await expect(page.getByText("Something went wrong", { exact: true })).toBeVisible();
      });

      await test.step("the webapp saved a draft source and checked it", async () => {
        const [created] = airbyte.calls("createSource");
        expect.soft(created).toEqual({
          name: "testName",
          sourceDefinitionId,
          workspaceId: airbyte.workspaceId,
          connectionConfiguration: { pokemon_name: "bulbasaur" },
          createAsDraft: true,
        });
        const [draft] = airbyte.store.sources.all();
        expect.soft(airbyte.calls("runCheckCommand")).toEqual([{ id: expect.any(String), actor_id: draft.sourceId }]);
      });

      await test.step("click Connections in the sidebar", async () => {
        await connectionsNavLink(page).click();
      });

      await test.step("the unsaved changes modal shows", async () => {
        await expect.soft(unsavedChangesModal(page)).toContainText("Unsaved changes will be lost");
        await expect
          .soft(unsavedChangesModal(page))
          .toContainText(
            "Your changes will be lost if you navigate away from this page. Are you sure you want to leave?"
          );
      });

      await test.step("the new source form stays open", async () => {
        await expect.soft(page).toHaveURL(/\/source\/new-source/);
      });
    });
  });

  test.describe(() => {
    // An empty workspace with PokeAPI in the marketplace, and agent-assisted setup turned on.
    test.use({
      airbyteScenario: scenario({
        sourceDefinitions: [defs.pokeApiSource],
        experiments: { "connector.agentAssistedSetup": true },
      }),
    });

    test("set up a source with the form when agent-assisted setup is on", async ({ page, airbyte }) => {
      await test.step("select PokeAPI on the new source page", async () => {
        await navigateToConnectorCreation(page, "source", airbyte.workspaceId);
        await selectConnectorFromMarketplace(page, "poke", "PokeAPI");
      });

      await test.step("the form shows, and the setup assistant does not", async () => {
        // "Agent" is the label of the Agent/Form toggle. The toggle shows only when agent-assisted setup is on.
        await expect.soft(page.getByText("Agent", { exact: true })).toBeVisible();
        await expect.soft(page.locator("input[name='name']")).toBeVisible();
        await expect.soft(page.getByText("Connector Setup Assistant")).toBeHidden();
      });
    });
  });
});

test.describe("Existing source", () => {
  // A PokeAPI source that fetches venusaur, without a connection. Every connection check succeeds.
  const pokeSource = source({ def: defs.pokeApiSource, name: "PokeAPI source", config: { pokemon_name: "venusaur" } });
  test.use({ airbyteScenario: scenario({ sources: [pokeSource] }) });

  /** Opens the sources page and clicks the source in the list. */
  async function openSource(page: Page, workspaceId: string) {
    await page.goto(`/workspaces/${workspaceId}/source`);
    await page.getByTestId("sourcesTable").getByRole("link", { name: "PokeAPI source" }).first().click();
    await expect(pokemonDropdown(page)).toContainText("venusaur");
  }

  test("edit a source twice without leaving the page", async ({ page, airbyte }) => {
    const { sourceId } = airbyte.store.sources.get(pokeSource);

    await test.step("open the source from the sources page", async () => {
      await openSource(page, airbyte.workspaceId);
    });

    await test.step("select ivysaur and save", async () => {
      await selectPokemon(page, "ivysaur");
      await submitButton(page).click();
      await expect(page.locator("[data-id='success-result']")).toBeVisible();
    });

    await test.step("the source fetches ivysaur, and the save button is disabled", async () => {
      await expect.soft(pokemonDropdown(page)).toContainText("ivysaur");
      await expect.soft(submitButton(page)).toBeDisabled();
    });

    await test.step("the webapp checked the ivysaur configuration, then saved it", async () => {
      expect
        .soft(airbyte.calls("runCheckCommand"))
        .toEqual([{ id: expect.any(String), actor_id: sourceId, config: { pokemon_name: "ivysaur" } }]);
      expect
        .soft(airbyte.calls("updateSource"))
        .toEqual([{ sourceId, name: "PokeAPI source", connectionConfiguration: { pokemon_name: "ivysaur" } }]);
    });

    await test.step("select bulbasaur and save again", async () => {
      await selectPokemon(page, "bulbasaur");
      await expect(submitButton(page)).toBeEnabled();
      await submitButton(page).click();
      // The success message of the first save can still show, so wait for the second save request instead.
      await expect.poll(() => airbyte.calls("updateSource")).toHaveLength(2);
    });

    await test.step("the source fetches bulbasaur, and the save button is disabled", async () => {
      await expect.soft(pokemonDropdown(page)).toContainText("bulbasaur");
      await expect.soft(submitButton(page)).toBeDisabled();
    });

    await test.step("the webapp checked the bulbasaur configuration, then saved it", async () => {
      expect
        .soft(airbyte.calls.last("runCheckCommand"))
        .toEqual({ id: expect.any(String), actor_id: sourceId, config: { pokemon_name: "bulbasaur" } });
      expect.soft(airbyte.calls.last("updateSource")).toEqual({
        sourceId,
        name: "PokeAPI source",
        connectionConfiguration: { pokemon_name: "bulbasaur" },
      });
    });
  });

  test("delete a source", async ({ page, airbyte }) => {
    const { sourceId } = airbyte.store.sources.get(pokeSource);

    await test.step("open the source from the sources page", async () => {
      await openSource(page, airbyte.workspaceId);
    });

    await test.step("delete the source, and type its name to confirm", async () => {
      await page.locator("button[data-id='open-delete-modal']").click();
      await page.locator("input[id='confirmation-text']").fill("PokeAPI source");
      await page.locator("button[data-id='delete']").click();
    });

    await test.step("the new source page opens, because no source is left", async () => {
      await expect.soft(page).toHaveURL(/\/source\/new-source$/);
    });

    await test.step("the webapp deleted the source", async () => {
      expect.soft(airbyte.calls("deleteSource")).toEqual([{ sourceId }]);
    });
  });
});
