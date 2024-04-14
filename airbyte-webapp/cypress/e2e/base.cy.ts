describe("Error handling view", () => {
  it("Shows Server Unavailable page", () => {
    cy.intercept("/api/v1/**", {
      statusCode: 502,
      body: "Failed to fetch",
    });

    cy.on("uncaught:exception", () => false);

    cy.visit("/");

    cy.get("p").contains("Airbyte is temporarily unavailable.").should("exist");
  });
});
