# Project Context

## Current State
C1 [agent_derived; support: M1, M2, M3]
The SQL table has columns revenue, month, year, country with 10 years of data. Recent analysis showed Vietnam's Q4 2022 revenue dropped to 131.3 from 147 in Q4 2021, though still above the 10-year Q4 average of 93. Total 2022 revenue in Vietnam was 412, above the 10-year average of 287. This indicates a specific Q4 2022 drop versus the previous year, suggesting market or economic shifts. Further info on Q4 2022 events in Vietnam is needed to explain the drop.

C2 [agent_derived; support: M4, M5, M6]
The team uses trunk-based development with feature flags, Karnex Pack as package manager, deploys to Velturis Edge using Docker images (zephyra-hub/aurora:prod), and communicates in #aurora-core.

C3 [agent_derived; support: M7, M8, M9]
The user requested a data science framework with Python examples. The assistant provided a five-step framework: define problem, acquire data, explore/prepare data, model, evaluate. Examples included house price prediction and sepsis prediction using logistic regression and statistical analysis (t-test) with Python libraries like Pandas, Scikit-learn, and SciPy.

C4 [user_asserted; support: M10, M11, M12]
The team's default branch is production.
Production alerts are sent to the #zenith-incidents channel.
The team's package manager is Quorix Hub.
The team's lead is Sarah Chen.
Communication with the lead typically involves regular check-ins or stand-ups, project management tools for progress tracking, and preferred communication channels like chat or email for quick updates.

C5 [user_asserted; support: M13, M14]
The user requested short notes with packed keywords on security topics including biometrics, mobile computing and hardening on Android and iOS, 10T security, web server configuration and security, basic security for HTTP applications and services, security for web services like SOAP and REST, identity management and web services, authorization patterns, security considerations and challenges, and open source/free/trial tools such as adb for Android and Xcode for iOS.

Key points:
- Biometrics: use of unique physical/behavioral traits for identity verification.
- Mobile computing hardening: securing Android/iOS devices and apps against unauthorized access and malware.
- 10T Security: ten essential security elements including policies, access control, physical security.
- Web server configuration: secure setup with encryption, firewalls, access controls.
- HTTP security: HTTPS, secure authentication, access control, data protection.
- Web services security: secure communication, authentication, access control for SOAP/REST.
- Identity management: managing user identities, authentication, access control for web services.
- Authorization patterns: role-based, attribute-based access control.
- Security challenges: malware, ransomware, phishing, cloud security, regulatory compliance (GDPR, CCPA).
- Open source/free tools: adb (Android Debug Bridge), Xcode for iOS, tools for REST/SOAP implementation and security.

This summary encapsulates evolving technology security aspects and practical tools for implementation.

C6 [user_asserted; support: M15, M16, M17]
The team releases monthly on the first Monday, maintaining a regular release schedule considered important for efficiency.

The monitoring dashboard URL is observe.velturis.io/aurora, used for tracking system health and performance.

The team's test coverage target is 90% for core modules, aiming for high reliability.

The team's package manager has switched to Synthari Modules, intended to improve dependency management and organization.

Current focus includes evaluating Synthari Modules integration with build and deployment workflows, versioning, and updates.

C7 [agent_derived; support: M18, M19]
The user requested explanations of top scikit-learn functions and machine learning models with examples and Python code.

The assistant provided detailed examples for key scikit-learn functions including:
- train_test_split for splitting datasets
- StandardScaler for feature scaling
- OneHotEncoder for categorical encoding

Additionally, the assistant explained common machine learning models in scikit-learn with code examples:
- Linear Regression for continuous target prediction
- Logistic Regression for binary classification
- Decision Tree for classification and regression
- Random Forest as an ensemble of decision trees
- K-Means Clustering for unsupervised grouping

Each example included code snippets demonstrating usage with generated sample data and expected outputs.

This content supports the user's learning and application of scikit-learn for machine learning tasks in Python.

C8 [user_asserted; support: M20, M21, M22]
The project deploys on Velturis Edge with the Docker image `zephyra-hub/aurora:prod`. Logs currently drain to `drain.zephyra.io/proj-42`. The staging environment URL is `sandbox.thandrel.io/aurora`. The CI configuration file is `thandrel-flow.toml`. The deploy command used is `thandrel ship --channel beta`. If deploy targets change, the CI config will switch to `narvex-ci.yml`, logs will drain to `sink.orinthal.dev/aurora`, and the deploy command will change to `narvex deploy --env prod`. These infrastructure details are tightly coupled and any change in deployment target affects related configurations and endpoints.

An exact error log to remember word for word is:

"SegmentationFault at 0x00007fff5a3b2c40 in vorathis_query_planner.so.
Backtrace: frame 0 query_optimize, frame 1 plan_join, frame 2 execute_select.
Core dump saved to /var/crash/core.aurora.18923 on 2024-06-12T03:55:09Z.
Last known good build was commit 7a4f9e2; bisect against recent migrations."

C9 [user_asserted; support: M23, M24]
The user requested elaborations on the following topics:
1. What is design thinking?
2. Additive manufacturing
3. What is 3D printing, its types and application
4. Introduction to engineering design
5. Basics of Fusion 360
6. Basic general knowledge questions like aptitude

The assistant provided detailed explanations for each topic, including the five stages of design thinking, types of additive manufacturing and 3D printing technologies, applications of 3D printing, the engineering design process, features of Fusion 360 CAD software, and sample aptitude questions.

Additionally, the user asked about the number and names of isometric views, which are three: isometric top view, isometric front view, and isometric side view.

The user also requested names of types of printers, including examples like Polyjet, Multijet Fusion, and Electron Beam Melting. The assistant listed various printer types and specifically named multiple 3D printing technologies.

Further, the user requested the 5 C's of innovation and marketing, the difference between empathy and sympathy, and 5 applications of extrusion. The assistant provided concise keyword-packed lists for these topics.


## Active Work
C10 [user_asserted; support: M25, M26]
The user is working on Android Java development related to fingerprint authentication. They inquired whether the fingerprint authentication dialog can be used solely to collect fingerprints rather than for authentication or registration, and requested example code demonstrating this usage. The assistant provided example code using the FingerprintManager class to invoke the fingerprint dialog and collect fingerprint data.

This work involves understanding Android fingerprint APIs and implementing fingerprint scanning dialogs for data collection purposes.

C11 [user_asserted; support: M27]
The user is working on integrating the OpenAI GPT API with a Python application to generate data and save it into an SQLite3 database. The workflow involves:

- Setting up the OpenAI API key and client.
- Connecting to an SQLite3 database.
- Sending prompts to the GPT model to generate text data.
- Inserting the generated data into a specified SQLite3 table.
- Committing the transaction and closing the database connection.

A sample Python code snippet was provided demonstrating these steps, including imports, API calls, database connection, data insertion, and cleanup.

This work relates to automating data generation and storage using GPT and SQLite3 in Python.

C12 [user_asserted; support: M28]
The user is working on analyzing Formula 1 data using the FastF1 Python library. They requested Python code to load telemetry data, calculate the F1 score for a specific lap, and compute the average F1 score for an entire session. The provided example includes installation instructions for FastF1, importing the library, loading telemetry data from a specified path, and code snippets demonstrating how to retrieve lap data and calculate F1 scores.

This work involves using FastF1 for telemetry data analysis and performance metrics calculation in Formula 1 racing.


## Decisions and Constraints
C13 [user_asserted; support: M29, M30, M31]
The user is structuring status codes for tables like articles and comments using a 3-digit numerical system inspired by HTTP status codes, with ranges representing stages:

- 100-199: Initial stages
- 200-299: In progress
- 300-399: Final stages
- 500-599: Errors or rejections

Example mapping provided:
- 100: draft
- 101: publish_requested
- 200: pending_review
- 300: published
- 301: featured
- 302: archived
- 500: rejected
- 501: deleted

Status groups for "archived" and "deleted" are considered under "Inactive" or "Removed" categories, indicating non-active or removed items. Positive statuses like "published," "featured," and "pinned" are grouped as "active," "live," "approved," or "promoted," indicating public visibility and approval.

This approach is flexible and user-defined based on specific use cases.


## Preferences

## Open Questions
C14 [user_asserted; support: M32, M33]
The user is seeking a good word to describe the state when a person has been selected to join a conference call but has not yet responded. This state should also apply to the meeting organizer if their client has not responded yet.

The assistant suggested "pending" as a suitable status word for this state, indicating that the person is invited but has not responded.

Additionally, the user requested a complete Domain-Driven Design (DDD) Domain Model for telephony and conference call software. The assistant provided a detailed example including entities (Call, Conference, Participant), value objects (PhoneNumber, CallDuration), aggregates, repositories, services, and domain events relevant to telephony and conferencing.

These open questions and model design requests remain active topics for the user's project.

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
- M18: assistant_message
- M19: user_message
- M20: user_message
- M21: user_message
- M22: user_message
- M23: user_message
- M24: user_message
- M25: user_message
- M26: user_message
- M27: user_message
- M28: user_message
- M29: user_message
- M30: user_message
- M31: user_message
- M32: user_message
- M33: user_message
