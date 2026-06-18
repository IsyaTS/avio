# Testing Truth Rules

Use these rules when adding, reviewing, or auditing tests. The goal is product evidence, not just mocked helper calls.

## Detailed Mock / Truth / Smoke Rules

- Mock external boundaries: Avito API, OpenAI, Telegram/WA transport, and other third-party services.
- Do not mock the system under test in critical integration checks.
- Avoid mocking internal routing, config merge, Redis queue, response pipeline, learning retrieval, or persistence paths unless the test is explicitly unit-level.
- If a critical test has many mocks around the main scenario, add or reference a truth/integration/smoke test that goes through the real internal route.
- Tests should assert observable product results:
  - persisted config;
  - queue item;
  - DB row;
  - outbox payload;
  - prompt content;
  - API response;
  - UI-visible state.
- A test that only asserts "function was called" is not enough for critical scenarios.
- New test helpers may build fixtures, but must not become a second implementation of business logic.

## Critical Test Honesty Rules

- Classify each critical test as:
  - `unit`: pure logic only;
  - `integration`: real in-process route with DB/Redis or app route;
  - `smoke`: running stack path;
  - `prod_readonly`: safe production observation.
- For Avito incoming, keep at least one test/smoke that proves webhook/API -> queue -> worker -> message/outbox.
- For tenant settings, verify restart persistence against the real config merge path and DB/file fallback.
- For learning, prove DB examples enter the actual response pipeline prompt.
- For OAuth, prove callback persists tokens after Redis state loss through signed-state recovery.
- For response quality, verify actual reply text/source/violations, not only prompt builder calls.

Run the audit when relevant:

```bash
python scripts/test_truth_audit.py tests
```

For CI/release, use fail-gate mode if the script supports it.

## Evidence Requirements For Green Claims

Do not claim green unless the evidence includes:

- exact command;
- environment identity;
- pass/fail result;
- what the command proves;
- what it does not prove.

If a smoke/test passes because external calls are mocked or disabled, state that boundary. Example: `OUTBOX_ENABLED=0` proves payload creation, not real Avito delivery.

Warnings must not be hidden just to get green:

- If warnings are unrelated to the changed surface, report them.
- If warnings were introduced by the current change, fix them or explain the blocker.

## Runtime Incident Evidence

For runtime incidents, collect both state and logs:

- DB/Redis/API state result.
- App/worker logs.
- Queue state where relevant.
- Outbox/UI visibility where relevant.

One signal alone is incomplete for critical runtime paths.

## Changed-Test Standard

When changing tests or CI:

- The changed tests should fail for the old broken behavior, or be justified as coverage/gate improvements.
- Do not downgrade a critical runtime scenario from smoke/integration to unit-only coverage.
- Do not replace a product-observable assertion with a mocked helper assertion.
- If a critical mock-heavy test remains, document why and name the follow-up truth check.
