import { render } from "test-utils/testutils";

import { EditorHeader } from "./EditorHeader";

describe("<ArrayOfObjectsEditor />", () => {
  describe("edit mode", () => {
    it("renders only relevant items for the mode", async () => {
      const { getByTestId } = await render(
        <EditorHeader
          mainTitle={<div data-testid="mainTitle">"This is the main title"</div>}
          addButtonText={<div data-testid="addButtonText">"button text"</div>}
          itemsCount={0}
          onAddItem={() => {
            return null;
          }}
        />
      );

      expect(getByTestId("mainTitle")).toBeInTheDocument();
      expect(getByTestId("addItemButton")).toBeEnabled();
    });
  });
  describe("readonly mode", () => {
    it("renders only relevant items for the mode", async () => {
      const { getByTestId } = await render(
        // simulate disabled form
        <fieldset disabled>
          <EditorHeader
            mainTitle={<div data-testid="mainTitle">"This is the main title"</div>}
            addButtonText={<div data-testid="addButtonText">"button text"</div>}
            itemsCount={0}
            onAddItem={() => {
              return null;
            }}
          />
        </fieldset>
      );

      expect(getByTestId("mainTitle")).toBeInTheDocument();
      expect(getByTestId("addItemButton")).toBeDisabled();
    });
  });
});
