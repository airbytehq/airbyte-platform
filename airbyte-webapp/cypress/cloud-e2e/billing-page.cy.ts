describe("billing page", () => {
  after(() => {
    cy.logout();
  });

  it("loads without error", () => {
    cy.login();
    cy.selectWorkspace();
    cy.hasNavigatedTo("/connections");
    cy.contains("Billing").click({ force: true });
    cy.hasNavigatedTo("/billing");
    cy.contains(/(Buy|Remaining) credits/);
    cy.contains(/(You have no|Total) credits usage/);
  });
});
