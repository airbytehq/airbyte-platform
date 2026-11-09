export enum CloudRoutes {
  Root = "/",
  AcceptInvitation = "/accept-invite",
  Metrics = "metrics",
  OnboardingFlow = "onboarding-flow",

  // Auth routes
  Signup = "/signup",
  Login = "/login",
  Sso = "/sso",
  SsoBookmark = "/sso/:companyIdentifier",
}
