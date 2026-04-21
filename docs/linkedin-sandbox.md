# LinkedIn Sandbox

`linkedin_cli.sandbox.LinkedInSandbox` is an in-process LinkedIn-like backend for
local write-path tests. It is intentionally small: it models only the Voyager and
mobile-web routes used by this CLI, and it never sends network traffic to
LinkedIn.

Use it when a test needs to exercise the real planner, executor, action store,
and reconciler without a live LinkedIn account:

```python
from linkedin_cli.sandbox import LinkedInSandbox
from linkedin_cli.write.executor import execute_action
from linkedin_cli.write.plans import build_post_plan
from linkedin_cli.write.reconcile import reconcile_action

sandbox = LinkedInSandbox()
plan = build_post_plan(sandbox.account_id, "hello from sandbox")

result = execute_action(
    session=sandbox.session,
    action_id="sbx_post",
    plan=plan,
    account_id=sandbox.account_id,
    dry_run=False,
)
assert result["status"] == "succeeded"
assert reconcile_action(sandbox.session, "sbx_post")["reconciled"] is True
```

Covered surfaces:

- text and image post publish
- profile edit
- experience add
- connect and follow profile mutations
- existing-thread and new-thread DM send
- public comment post and comment target discovery
- feed, profile, post-page, and conversation reconciliation reads

This is not a replacement for live smoke tests. It catches local regressions in
request construction, state transitions, idempotency, and reconciliation logic;
the opt-in live tests still cover private LinkedIn request-shape drift.
