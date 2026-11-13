# Project Context

## Current State
C1 [agent_derived; support: M1, M2, M3]
The team uses Zelthra Lib as their package manager and Thalnex Store as their database, managing dependencies and data storage efficiently. They have a dedicated #aurora-alerts channel for critical production issues, with an incident runbook at runbooks.aurora.internal/playbooks and Kevin Park as the escalation contact. The sprint deadline is March 28, 2025. Standups are held synchronously on Monday and Thursday, with asynchronous updates on other days to balance communication and flexibility.

C2 [user_asserted; support: M4, M5]
The user is working with Logic Pro to create and modify a white noise layer instrument, including pitch modification techniques using MIDI and ES2 synthesizer controls.

C3 [user_asserted; support: M6, M7]
The user needs to create a detailed and imaginative proposal (devis) for a website development project. The website will allow users to register and submit applications for a street dance festival (hip-hop, breakdance, etc). The proposal should include sections that capture the committee's imagination to stand out from other bidders.

C4 [agent_derived; support: M8, M9, M10]
Role assignments depend on the team lead: currently, VP Helen Chen is approval authority and Seoyun Choi handles code reviews, both assigned by Yuna Park. If the lead changes, approval shifts to VP Taehyun Kim. A certificate verification error involving crysthene-db-primary.internal requires rotating the cert via Cryptara Vault and restarting the connection pool.

C5 [user_asserted; support: M11, M12]
The user requested Unix commands to recursively change group ownership of all files under `/global/cfs/cdirs/m3760/hisq/runs` to group `m3760` and to make all those files group writable. The commands provided were:

```bash
chgrp -R m3760 /global/cfs/cdirs/m3760/hisq/runs
chmod -R g+w /global/cfs/cdirs/m3760/hisq/runs
```

Additionally, the user asked for a command to format a USB drive to ext4 on macOS. The assistant explained that macOS does not natively support ext4 formatting but provided a detailed procedure involving installing Homebrew, installing `ext4fuse`, formatting the drive to ExFAT via Disk Utility, then using `mkfs.ext4` via terminal with the appropriate disk identifier.

These details reflect the user's current technical tasks involving Unix file permissions and macOS disk formatting for ext4.


## Active Work
C6 [user_asserted; support: M4, M5]
The user is actively working on creating a white noise layer instrument in Logic Pro and modifying its pitch using pitch bend, MIDI note transposition, and ES2 pitch controls.

C7 [user_asserted; support: M13, M14, M15]
The user is composing various professional emails to learners regarding session scheduling, ticket closure, and follow-ups based on learner participation and communication preferences.

C8 [agent_derived; support: M16, M17]
The user is exploring Fiverr business ideas to generate $10,000: a premium product launch specialist service targeting high-value clients, and a lower-cost digital marketing course or ebook aimed at a large audience with upsell opportunities.

C9 [user_asserted; support: M18, M19]
The user is working on designing a specific order management solution as a web app integrated with e-commerce websites. The solution includes:

- Frontend: React framework chosen for its large community, performance, and suitability for complex web apps.
- Backend: Node.js selected for scalable, high-performance backend with event-driven architecture.
- Integration: Use RESTful APIs and webhooks provided by e-commerce platforms for real-time order data synchronization.
- Key UI Elements: Clear, intuitive interface with search, filtering, order status updates, and dashboard metrics like revenue and order volume.
- Security: HTTPS encryption, strong backend authentication and access control, encrypted database, input validation, error handling, and logging.
- Hosting: Cloud platforms AWS or Azure for scalability and reliability, with Docker containerization for deployment and management.

C10 [user_asserted; support: M20]
The user is developing a semester project for an Introduction to Database course involving a farm management system. The project includes:

- Frontend UI: A user-friendly interface for farmers and managers with dashboards, data visualization, and data entry forms, potentially using React, Angular, or Vue.js.
- Database Design: Entities such as farms, farmers, crops, livestock, equipment, and vendors with logical data modeling for tables and relationships.
- Database Queries: SQL queries for reports, searches, and trend analysis using joins, aggregations, and subqueries.
- Stored Procedures: Precompiled routines for operations like calculating average crop yields.
- Triggers: Automated actions on data changes, e.g., updating crop inventory when new harvest data is added.

The system aims to cover farm, crop, livestock, equipment, vendor management, and reporting, integrating frontend and backend database features as required by the course.


## Decisions and Constraints
C11 [user_asserted; support: M21, M22]
The `%f` format specifier for the `string(TIMESTAMP ...)` command in CMake was introduced in version 3.23. To use this specifier, CMake 3.23 or later must be installed.


## Preferences

## Open Questions
C12 [user_asserted; support: M23, M24, M25]
The user asked a riddle about fairly splitting money between two people with different initial amounts and questioned the assistant's initial incorrect calculation and explanation.

Support handles:
- M1: user_message
- M2: user_message
- M3: user_message
- M4: user_message
- M5: user_message
- M6: user_message
- M7: user_message
- M8: user_message
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
