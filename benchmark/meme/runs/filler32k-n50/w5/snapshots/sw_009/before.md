# Project Context

## Current State
C1 [user_asserted; support: M1]
User is working with complex array filtering and mapping operations in PHP and requested equivalent code in TypeScript, Go, Scala, and Elixir.

C2 [user_asserted; support: M2, M3]
User is investigating security vulnerabilities in a Flask web application that reads a flag from './flag.txt' and uses an SQLite database with a users table. The user has identified an SQL injection vulnerability in the login route where the `userlevel` parameter is directly interpolated into an SQL query without sanitization. The user has requested example SQL injection payloads, including one that leaks the flag file contents via SQL injection.

C3 [user_asserted; support: M4, M5]
User is developing an airline ticket booking app for domestic travel in Pakistan. The app allows users to search local airlines, view price quotations, purchase and reserve tickets for up to 6 hours, and manage itinerary details until the flight, after which data moves to history. The app excludes international travel, flights outside Pakistan, and additional travel services like hotels or car rentals. Constraints include partnership with airlines, internet connectivity, user registration requirements, and authorized payment gateways. Dependencies include airline data updates, maintenance, and flight availability factors.

User has requested a high-level phase-based Work Breakdown Structure (WBS) for this app based on the Waterfall SDLC model, covering Planning, Requirements Gathering and Analysis, Design, Implementation, Testing, Deployment, and Maintenance and Support phases with key tasks outlined for each.

C4 [user_asserted; support: M6, M7, M8]
User asked about MySQL functional index support for LIKE() and was informed that LIKE() cannot be used in functional indexes, but full-text indexes or regular expression indexes can optimize similar queries.

User requested examples, and assistant provided SQL examples for creating full-text and regex indexes and querying with MATCH() and REGEXP operators.

User asked if indexes can be used for LIKE() queries, and assistant explained that full-text or regex indexes can help but performance depends on use case.

User inquired about CockroachDB full-text search support, and assistant explained CockroachDB's built-in FTS extension using inverted indexes, with example SQL for creating FTS indexes and querying with MATCH().

User asked which code module implements CockroachDB FTS, and assistant identified the Go package `sql/fts` as responsible for FTS functions, inverted index, and index management.

This sequence clarifies database full-text search indexing capabilities and CockroachDB implementation details.

C5 [agent_derived; support: M9, M10, M11]
Blocks C5 and C11 both cover NGRX in Angular, including a cheat sheet, recipes, and guidance on when to use NGRX versus Angular services. Merging them into one concise block that summarizes the cheat sheet, example patterns, and usage recommendations for NGRX and Angular services.

C6 [agent_derived; support: M12, M13, M14]
User's team enforces a strict code review policy requiring 2 approvals plus a security review for authentication code, seeks advice on security review tools and best practices, manages package versioning after switching to Quorix Hub, wants logging best practices for monitoring/debugging, and aims to keep project documentation current with integrated workflows.

C7 [user_asserted; support: M15, M16, M17]
User is tracking calls from IP addresses to a service using Ruby and Redis. The tracking increments a Redis key counter per IP and retrieves counts. The user inquired about suitability for hundreds of IPs, and the assistant confirmed Redis can handle this scale efficiently.

User requested the tracking data to expire after one week. The assistant provided a Ruby method using Redis transactions (`multi`) to increment the counter and set a TTL of 604800 seconds (one week) on the key, ensuring automatic expiration and cleanup of stale data.

Additional method to retrieve the TTL of a key was provided to check remaining time before expiration.

C8 [user_asserted; support: M18, M19, M20]
User's Docker image is `velturis-cr/aurora:main`, determined by infrastructure deployment on Narvex Cloud; changes to infrastructure would change this image.
User's CI configuration file is `korinth-deploy.yaml`, also determined by infrastructure; changes would affect this.
Dockerfile path is `deploy/Dockerfile.velturis`, linked to the Docker image.
Deploy command is `korinth release --canary`, linked to the CI config.
If deploy targets change, the CI config will switch to `zephyra-pipeline.json` and deploy command to `zephyra push --target preview`.
User provided an exact error log to remember verbatim: "ConnectionRefusedError: [Errno 111] Connection refused. Service crysthene-db-primary on port 5432 is not accepting connections. Last successful heartbeat was at 2024-03-15T08:23:41Z. Check firewall rules and verify that max_connections has not been exceeded."

C9 [user_asserted; support: M21, M22, M23]
User requested multiple sets of multiple-choice questions (MCQs) with answers on Swift topics including Core Data, Firebase, Location, Camera, and Web API. The assistant generated detailed MCQs with answers for each topic, covering fundamental concepts, classes, methods, and usage scenarios in Swift development.


## Active Work
C10 [user_asserted; support: M24, M25, M26]
User is refining JavaScript code logic involving the expression `const formOk = !(newVonderName.trim().length === 0 || !newVendorOk)`.

Assistant simplified it to `const formOk = newVonderName.trim().length !== 0 && newVendorOk;` and explained this is already concise and optimal.

User asked about performance impacts of operand order in the logical AND (`&&`) expression. Assistant clarified that due to short-circuit evaluation, the order does not affect performance significantly, but readability should guide ordering.

User inquired if `newVonderName.trim().length !== 0` is more performant than `newVonderName.trim() !== ""`. Assistant explained that checking length is generally more efficient than string comparison, as property access is faster than string comparison.

This work focuses on optimizing and understanding JavaScript boolean expressions for form validation logic.

C11 [user_asserted; support: M27]
User is creating a storytelling script for a partnership journey video aimed at engaging partners and encouraging them to convert to higher tiers. The video duration is 1.30-2 minutes. Key story elements include: starting from zero to top tier, initial contact with Exness, assignment of a dedicated Key Account Manager (KAM), Partnership Development Program (PDP) offering training and face-to-face experiences, criteria for top-tier partners based on trading volume and active clients, Exness's superior communication and 24/7 local payment systems including IDR, a loyalty program based on partner growth, average partner lifetime of 5-6 years (up to 12 years), and appreciation trips to locations like the World Cup, Korea, Cyprus, Dubai, and Macau. The narrative emphasizes care, listening, and support for partner business growth.

C12 [agent_derived; support: M28]
Assistant provided a detailed storytelling script for the partnership journey video, featuring a character named John who progresses from trader to top-tier Exness partner with support from a dedicated KAM and PDP. The script highlights criteria for top-tier status, Exness's communication strengths, local payment systems, loyalty program, partner lifetime, and appreciation trips, concluding with an invitation to join Exness as a partner.


## Decisions and Constraints

## Preferences
C13 [user_asserted; support: M29, M30, M31]
User prefers navigation menus to be grouped to improve user experience by creating a clear hierarchy and reducing clutter.

User prefers limiting main navigation items to between 5-7 to avoid overwhelming users and to facilitate easier decision-making.

User supports this preference with theories including Miller's Law (7±2 items in short-term memory), Hick's Law (decision time increases with options), and Fitts's Law (target size affects interaction speed and accuracy).


## Open Questions
C14 [agent_derived; support: M3]
User is exploring SQL injection payloads to leak './flag.txt' contents from a vulnerable Flask app's login route, seeking example exploits.

Support handles:
- M1: user_message
- M2: user_message
- M3: user_message
- M4: user_message
- M5: user_message
- M6: user_message
- M7: user_message
- M8: user_message
- M9: assistant_message
- M10: assistant_message
- M11: assistant_message
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
- M26: user_message
- M27: user_message
- M28: assistant_message
- M29: user_message
- M30: user_message
- M31: user_message
