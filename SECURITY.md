# Security Policy

## Supported Versions

NoKV is still evolving quickly. Security fixes are expected to land on the latest release line and `main`.

| Version | Supported |
| --- | --- |
| `main` | Yes |
| latest tagged release line | Yes |
| older releases | Best effort only |

At the time of writing, the latest tagged release is [`v0.8.0`](https://github.com/feichai0017/NoKV/releases/tag/v0.8.0).

## Reporting a Vulnerability

Preferred path:

1. Use GitHub's private vulnerability reporting for this repository if it is available.
2. Include the affected version, impact, reproduction steps, and any proof-of-concept details needed to reproduce the issue.

If private reporting is not available:

1. Open a minimal public issue asking for a private security follow-up.
2. Do **not** include exploit details, secrets, crash artifacts with sensitive data, or full weaponized proof-of-concept material in the public issue.

## What to Include

Please include as much of the following as possible:

- affected commit, branch, tag, or release
- component or package path
- configuration needed to trigger the issue
- reproduction steps
- impact assessment
- suggested fix or mitigation, if known

## Response Expectations

- Initial acknowledgement target: within 7 days
- Status update target: within 14 days when the report is actionable

These are project targets, not contractual SLAs.

## Disclosure

Please give the maintainer reasonable time to assess and fix the issue before public disclosure.

When a fix is available, the project may disclose:

- affected versions
- impact summary
- mitigation guidance
- fix commit or release

