import { screen } from "@testing-library/react";

import { mockDestination, mockSource, render } from "test-utils";

import { SelectExistingConnector } from "./SelectExistingConnector";

jest.mock("./ExistingConnectorButton", () => ({
  ExistingConnectorButton: ({ connector }: { connector: { name: string } }) => <button>{connector.name}</button>,
}));

describe("SelectExistingConnector", () => {
  it.each([
    [
      "source",
      [
        { ...mockSource, name: "Ready source", isDraft: false },
        { ...mockSource, sourceId: "draft-source", name: "Draft source", isDraft: true },
      ],
    ],
    [
      "destination",
      [
        { ...mockDestination, name: "Ready destination", isDraft: false },
        { ...mockDestination, destinationId: "draft-destination", name: "Draft destination", isDraft: true },
      ],
    ],
  ] as const)("excludes draft %ss from connection selection", async (_actorType, connectors) => {
    await render(
      <SelectExistingConnector
        connectors={[...connectors]}
        selectConnector={jest.fn()}
        hasNextPage={false}
        isFetchingNextPage={false}
        fetchNextPage={jest.fn()}
      />
    );

    expect(screen.getByRole("button", { name: /^Ready/ })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /^Draft/ })).not.toBeInTheDocument();
  });
});
