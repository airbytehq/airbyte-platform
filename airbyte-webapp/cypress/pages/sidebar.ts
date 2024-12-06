const homepage = "[aria-label='Homepage']";

export const openHomepage = () => {
  cy.get(homepage).click();
};
