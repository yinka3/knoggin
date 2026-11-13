# Project Context

## Current State
C1 [user_asserted; support: M1, M2, M3]
User wants to dockerize a GUI Windows desktop application (like Spotfire) and run it through Linux Docker, and also explore running such containers on AWS Fargate.

C2 [user_asserted; support: M4]
User wants to provide direct login links without passwords for all users who carefully subscribe to the WordPress site.

C3 [user_asserted; support: M5]
User has set the sprint deadline to July 4, 2025, establishing a clear timeline for project delivery.

C4 [agent_derived; support: M6, M7, M8]
CVSS scores are standardized severity ratings typically provided for each CVE entry, often included when a CVE is published but sometimes added later. They help assess severity and prioritize remediation. Scores are usually calculated by MITRE Corporation and the National Vulnerability Database (NVD) maintained by NIST, and may be updated as more information becomes available. Different sources may have slightly different scores. Examples include:

1. CVE-2021-44228 (Apache Log4j Remote Code Execution) - Score: 10.0 (Critical)
2. CVE-2021-3156 (Sudo Heap-Based Buffer Overflow) - Score: 7.8 (High)
3. CVE-2020-0601 (Windows CryptoAPI Spoofing Vulnerability) - Score: 8.1 (High)

These illustrate how CVSS scores reflect severity and impact.

C5 [user_asserted; support: M9]
Exact error log to remember word for word:
"SegmentationFault at 0x00007fff5a3b2c40 in vorathis_query_planner.so.
Backtrace: frame 0 query_optimize, frame 1 plan_join, frame 2 execute_select.
Core dump saved to /var/crash/core.aurora.18923 on 2024-06-12T03:55:09Z.
Last known good build was commit 7a4f9e2; bisect against recent migrations."


## Active Work
C6 [user_asserted; support: M1, M2, M3]
User is working on dockerizing a GUI Windows desktop application and running it on Linux Docker with X11 forwarding, and considering AWS Fargate as the runtime environment.

C7 [user_asserted; support: M10, M11, M12]
User is working on a Scala function to calculate the number of people sitting strictly behind and in the same or left columns in a theater seating arrangement, given total rows and columns and the user's seat position. The function was corrected to:

```scala
def seatsInTheater(nCols: Int, nRows: Int, col: Int, row: Int): Int = {
  (nCols - col + 1) * (nRows - row)
}
```
This function returns the correct count, e.g., 96 for nCols=16, nRows=11, col=5, row=3.

C8 [user_asserted; support: M13, M14]
User is working on creating a reusable Go library to read values from a YAML file. The library defines a `Config` struct matching the YAML structure and provides a `Load` function that reads and unmarshals the YAML file into the struct, returning the config and any error. Example usage imports this library and loads the config from a YAML file, printing the values.

C9 [user_asserted; support: M15, M16]
User is working on submitting a FHIR CarePlan resource to an API that includes related entities like Adjuster and Referring Doctor, with example Python code including made up names and contact info for these participants.

C10 [user_asserted; support: M17, M18, M19]
User is optimizing Vue 3 composition API code by grouping related state and methods together using the script setup syntax, including debounced search input handling, loading more data on scroll, and back navigation handling.

C11 [user_asserted; support: M20, M21, M22]
User is working on setting up Synpress on a Macbook Pro, including steps to install Node.js, Yarn, clone the Synpress repository, install dependencies, configure environment variables, and run Synpress. Additionally, user is integrating Synpress with an existing cypress.config.js by modifying it to require Synpress and configuring environment variables accordingly.

C12 [user_asserted; support: M23, M24]
User is implementing automation of routine accounting tasks using GPT-4, following a step-by-step plan including:

1. Identifying repetitive accounting tasks such as bank reconciliations, depreciation calculations, and transaction classification.
2. Studying GPT-4's capability to perform these tasks effectively.
3. Acquiring access to GPT-4 API.
4. Preparing and structuring data for GPT-4 processing.
5. Developing scripts or applications (e.g., in Python) to interact with GPT-4 API for task automation.
6. Testing and adjusting scripts to ensure accuracy.
7. Training the accounting team to use the new tools.
8. Implementing the automation in daily workflows with human oversight.
9. Continuously improving the automation based on feedback.
10. Expanding automation to other routine tasks as feasible.

The implementation will comply with applicable data privacy and protection regulations.

C13 [user_asserted; support: M25]
User is working on a behavior intervention plan targeting aggression by replacing it with asking for a break. The plan includes:

- Behavioral definition of asking for a break focusing on observable measurable features: verbal request using phrases like "Can I take a break?" or "I need a break" with appropriate nonverbal cues.
- Task analysis steps for asking for a break.
- Teaching script including introduction, modeling, role-playing, feedback, and reinforcement.
- Materials list including whiteboard, markers, reinforcement items, and optional role-play props.
- Behavioral objective to increase asking for a break frequency from 0.14 to 5 times per hour.
- Plan for evaluating intervention integrity with observer training, reliability checks, data collection, and monitoring.
- Objective criteria for modifying the intervention if goals are not met.
- Plan for generalizing the behavior increase across classrooms and settings.


## Decisions and Constraints
C14 [agent_derived; support: M26, M27, M28]
- The Docker image should be based on a Windows base image (e.g., mcr.microsoft.com/windows/servercore:ltsc2019).
- Dependencies like .NET Framework and Java need to be installed.
- The GUI application (e.g., Spotfire) must be installed inside the container.
- The DISPLAY environment variable must be set to forward GUI output to the Linux host.
- An X11 server must be installed and configured on the Linux host to receive GUI output.
- Running GUI apps in Docker may have performance and compatibility limitations.
- Using AWS Fargate to run GUI Windows desktop containers is possible but may have additional limitations and requires ECS task definitions and services.

C15 [user_asserted; support: M29]
The team's default branch for code is set to 'production', emphasizing the importance of stable deployment practices.


## Preferences
C16 [user_asserted; support: M30, M31, M32]
The team uses Kryptal Auth for authentication with a session TTL of 15 minutes and SAML 2.0 SSO as the authentication method. The auth middleware currently in use is verithos-oauth-middleware v2.1. If the auth provider changes, the authentication method might switch to mTLS Client Cert with a session TTL of 4 hours and middleware config changing to oathkeeper-cert-check v3.2. Tips and considerations include:

- For Kryptal Auth with 15-minute TTL: balance user experience with session expiration.
- When switching SAML providers: check compatibility and custom claims.
- When changing middleware: test token validation and session management thoroughly.
- Switching to mTLS: plan for client certificate management, issuance, and renewal.
- For 4-hour TTL sessions: consider session expiration notifications and re-authentication flows.
- When adopting oathkeeper-cert-check v3.2: review configuration settings and integration impact.

These details are determined by the current auth provider and middleware, and changes affect session management and security configurations.


## Open Questions
C17 [user_asserted; support: M3]
How to best configure and optimize running GUI Windows desktop applications in Docker containers on AWS Fargate, considering performance and compatibility constraints?

C18 [user_asserted; support: M33]
User requests an example of what the direct login link looks like for the passwordless login system.

C19 [user_asserted; support: M29]
User inquires about best practices for merging to the production branch, including code reviews and testing procedures before merging.

Support handles:
- M1: user_message
- M2: user_message
- M3: user_message
- M4: user_message
- M5: user_message
- M6: assistant_message
- M7: assistant_message
- M8: assistant_message
- M9: user_message
- M10: user_message
- M11: user_message
- M12: user_message
- M13: user_message
- M14: user_message
- M15: user_message
- M16: user_message
- M17: user_message
- M18: user_message
- M19: user_message
- M20: user_message
- M21: user_message
- M22: user_message
- M23: user_message
- M24: user_message
- M25: user_message
- M26: assistant_message
- M27: assistant_message
- M28: assistant_message
- M29: user_message
- M30: user_message
- M31: user_message
- M32: user_message
- M33: user_message
