# Repository automation

Common CI and image mechanics are maintained in [abhi1693/actions](https://github.com/abhi1693/actions).
Workflows use the released `@v1` reference for centrally maintained compatible updates.
See the [shared release history](https://github.com/abhi1693/actions/releases) for changes.
Repository-owned scripts, triggers, scanner exceptions and application smoke checks
remain alongside the application. Image manifests declare components rather than
copying workflow steps.

CI passes the checked dynamic Python source version to the shared image workflow.
PR builds always use `publish: false`, scan unfixed HIGH/CRITICAL findings as well
as secrets, and run `scripts/smoke-runtime.sh` on native ARM64. Both release callers
grant the nested workflow's contents/packages/id-token/attestations permissions.
Tag/manual publication waits for the same CI and runtime gates. The shared
GitHub-hosted ARM64 runner default is used in both paths; no self-hosted runner is
required. Source and release version mismatches fail before building.

Publication also requires the exact `vVERSION` Git tag. Tag pushes and manual
dispatches must have identical full tag ref, resolved tag commit, checked-out
HEAD and workflow source SHA; moving master/main dispatches fail before CI or
image builds. The existing unittest job exercises the real guard against
disposable local Git histories, including later commits with the same version.
