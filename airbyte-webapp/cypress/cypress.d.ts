declare namespace Cypress {
  interface AUTWindow {
    document: Document;
    navigator: Navigator;
    _e2eOverwrites?: Partial<Experiments>;
  }
}
