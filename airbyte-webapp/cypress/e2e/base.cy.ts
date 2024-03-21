describe("Error handling view", () => {
  it("Shows Server Unavailable page", () => {
    cy.intercept("/api/v1/**", {
      statusCode: 502,
      body: "Failed to fetch",
    });

    cy.on("uncaught:exception", () => false);

    cy.visit("/");

    cy.get("div").contains("Cannot reach server. The server may still be starting up.").should("exist");
  });
});
