describe("billing page", () => {
  it("loads without error", () => {
    cy.login();
    cy.selectWorkspace();
    cy.contains("Billing").click({ force: true });
    cy.hasNavigatedTo("/billing");
    cy.contains(/(Buy|Remaining) credits/);
    cy.contains(/(You have no|Total) credits usage/);

    cy.logout();
  });
});
