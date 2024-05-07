export enum CloudRoutes {
  Root = "/",
  AuthFlow = "/auth_flow",
  AcceptInvitation = "/accept-invite",
  Metrics = "metrics",
  Billing = "billing",
  UpcomingFeatures = "upcoming-features",

  // Auth routes
  Signup = "/signup",
  Login = "/login",
  Sso = "/sso",
  SsoBookmark = "/sso/:companyIdentifier",
  ResetPassword = "/reset-password",
}
