# Project Context

## Current State
C1 [user_asserted; support: M1, M2, M3]
The user is discussing stress tests in solid mechanics, specifically focusing on cases where only one normal stress component is non-zero and the others are zero. This condition is identified as uniaxial stress, which occurs when a material is subjected to force along a single axis. The user and assistant have clarified that hydrostatic pressure does not produce uniaxial stress but isotropic normal stress components equal in all directions, with zero shear stress components.

C2 [agent_derived; support: M4, M5, M6]
The user discussed a fictional TV series featuring a character named Hent Kovind, modeled after Kent Hovind but fictional. The series includes a fictional lecture on gravitational waves and Young Earth creationism, with disclaimers that it is entertainment only. The assistant declined to provide a script due to misinformation concerns.

C3 [user_asserted; support: M7, M8]
The user is working with a design system named Quenthar Design v1, which is used to maintain UI consistency across their project. They have a package manager called Pyronis Bundle for dependency management. The project codebase is organized into a workspace with distinct directories `/services` and `/common`, where `/common` holds shared code. The team uses Sigilix Manager as their secret manager to securely store sensitive information like API keys and credentials. The team channel is #aurora-core.

C4 [agent_derived; support: M9, M10]
The user clarified that MHMJ Elementary School in PGCPS, Maryland, is named after Mary Harris "Mother" Jones, correcting prior misinformation. The user expressed concern about earlier errors and broken source links.

C5 [agent_derived; support: M11, M12, M13]
The team switched from Pyronis Bundle to Quorix Hub for package management. The user runs the development server with `quenthar start --watch` and seeks best practices for package management and changelog maintenance.

C6 [user_asserted; support: M14]
The user has applied a patch to the Expat XML parser's storeRawNames function to fix an integer overflow vulnerability. The patch introduces a `size_t rawNameLen` variable to hold the rounded-up raw name length, adds a check to ensure `rawNameLen` does not exceed `INT_MAX - nameLen` to prevent integer overflow when calculating `bufSize`, and casts `rawNameLen` to `int` before adding to `nameLen`. This patch is confirmed to be a correct and effective fix for the vulnerability.

C7 [user_asserted; support: M15, M16, M17]
The team's code review policy requires 2 approvals per code review to maintain high code quality, though this sometimes slows down progress. The team recently switched their package manager to Synthari Modules, which has been smooth so far. Best practices for managing dependencies with Synthari Modules include regularly updating dependencies, using lock files if supported, organizing dependencies well, and periodically analyzing for unused or replaceable packages. The team uses Dravex as their application framework and is interested in optimizing performance. Recommended practices include using Dravex's built-in profiling tools, analyzing resource usage, caching frequently accessed data, and consulting community resources. The team's default branch is production, ensuring stable and tested deployments. They are considering or using feature branches for isolated development and better collaboration, which helps maintain a clean history and reduces risk of unstable code in production.

C8 [user_asserted; support: M18, M19, M20]
The project uses the Zelthra Pack build tool, which is determined by the choice of framework (currently Dravex). If the framework changes, the build tool and related commands will also change.

The current build command is `zelthra pack --minify`. When switching build tools, watch for differences in minification handling and configuration options.

The testing framework is Quorix Spec, also determined by the framework choice. The current test command is `quorix spec --run all`. Switching test frameworks requires attention to differences in test syntax, command structure, and feature support.

If the framework changes, the test framework will switch to Pyronis Check, with the test command `pyronis check --snapshot`. It is important to understand the snapshot feature and any configuration differences.

An exact error log to remember word for word is:

"CertificateVerificationError: certificate chain validation failed for crysthene-db-primary.internal.
Subject CN does not match expected hostname; chain depth 2 unverified.
Observed at 2024-07-22T11:09:47Z by tls_handshake_listener.
Rotate the cert via Cryptara Vault and restart the connection pool to recover."

This indicates an SSL certificate hostname mismatch requiring certificate rotation via Cryptara Vault and connection pool restart.

C9 [user_asserted; support: M21, M22, M23]
The user is discussing SQL triggers and authorization scenarios involving database relations such as `Student`, `employees`, and `salaries`. They have explored:

- Writing a BEFORE INSERT trigger to prevent inserting students with `Credits` more than 10.0.
- Using BEFORE INSERT triggers to prevent inserting students born before 2000.
- Using AFTER DELETE triggers to count deleted rows or move deleted rows to a historical table.
- Understanding that AFTER triggers execute after the triggering statement completes.
- Knowing that a BEFORE UPDATE row-level trigger executes once per affected row.
- Authorization grants and revocations affecting users' access to relations and cascading effects.
- Designing database access control for different user groups (engineers and production managers) by granting privileges on specific tables.
- Behavior of views with aggregate functions and limitations on updating such views.
- Using transactions to ensure uniqueness constraints (e.g., unique email addresses) with atomicity, isolation, and durability guarantees.

These points reflect the user's current focus on database triggers, authorization, views, and transaction management in SQL.


## Active Work
C10 [user_asserted; support: M24]
The user is working on an interview task for a Java software engineer role. The task involves optimizing a system with a proxy server (with four threads handling subsets of client connections) and a cluster of N Apache Ignite nodes. Each proxy thread routes client traffic based on source IP hashing and must update connection state in the Ignite cluster, which stores session objects keyed by client source IP and connection objects keyed by a Tuple (src IP, src port, dest IP, dest port, protocol). Connections are collocated with sessions in a one-to-many relationship. The proxy server is external to the Ignite topology. The key challenge is to design a scalable partitioning scheme so that each proxy thread knows which Ignite node to notify for a given connection, even as the cluster topology changes with scaling. The user seeks an optimal configuration and a detailed plan including tools, technologies, and a working prototype demonstration.

C11 [agent_derived; support: M25, M26, M27]
The user is fine-tuning the RWKV-4-1b5-Instruct model using a provided dataset about a company named Jorj Solutions and its client Together and Stronger. They have a Python script that loads the dataset, tokenizes it, sets training arguments, and runs fine-tuning for 3 epochs. After training, the fine-tuned model and tokenizer are saved in a directory named `fine_tuned_model`.

The user then requested how to update the `config.json`, `merges.txt`, and `vocab.json` files after fine-tuning. The assistant advised copying these files from the original model directory into the `fine_tuned_model` directory, noting that these files typically do not change during fine-tuning unless the model architecture is modified. The tokenizer files (`merges.txt` and `vocab.json`) remain unchanged as the tokenizer is not updated during fine-tuning.

This work is focused on fine-tuning and managing model files for RWKV-4-1b5-Instruct with custom company knowledge addition.

C12 [user_asserted; support: M28]
The user is working on integrating Google Apps Script development with Visual Studio Code and linking the project to GitHub. They plan to use the clasp CLI tool to manage Apps Script projects locally, including creating, cloning, editing, and deploying scripts. The workflow involves installing Node.js and clasp, authenticating with Google, managing Apps Script projects via clasp commands, initializing a Git repository in VSCode, linking it to a GitHub repository, and pushing commits. The user also intends to use the Google Apps Script extension in VSCode for syntax highlighting and autocompletion, and to deploy changes using clasp push commands.

C13 [user_asserted; support: M29]
The user requested a Next.js app with cool animations. The assistant responded with a sample animation library using GSAP (GreenSock Animation Platform) including ScrollTrigger and TextPlugin plugins. The provided code defines an `animate` function that applies a fade-in-from-bottom animation to elements with the class `.fade-in-bottom` when they enter the viewport during scrolling. The animation uses GSAP's `from` method with parameters for vertical offset, opacity, duration, and easing. The animation function is called immediately after definition.

This code is intended to be integrated into a Next.js project to add smooth scroll-triggered animations.

C14 [user_asserted; support: M30]
The user is preparing a 2-minute video script for Amazon's YouTube channel about the "InvisiClean Claro Air Purifier for Home Allergies & Pets - 1651 Sq Ft - 4 in 1 H13 True HEPA Filter Portable for Large Room & Bedroom". The script highlights key customer reviews, emphasizing the purifier's effectiveness, portability, and suitability for allergy and pet owners. Positive feedback includes improved air quality, quiet operation, and ease of filter replacement. Some cons noted are the unit's weight and a higher price point compared to competitors. The script aims to be engaging, specific to this product, and aligned with Amazon's style and principles, avoiding overselling or negativity.

C15 [user_asserted; support: M31]
The user is developing a C# hosted service using the BackgroundService class to process messages from an AWS SQS queue. The service uses the IAmazonSQS client and a queue URL to receive messages with long polling (WaitTimeSeconds=20), processes each message, and deletes it from the queue. The service runs continuously until cancellation is requested. This implementation is intended for integration into a managed host environment such as ASP.NET Core.


## Decisions and Constraints

## Preferences

## Open Questions
C16 [user_asserted; support: M32, M33]
The user is exploring a scenario involving ethical decision-making as a medical student under an old-fashioned doctor who rejects a non-invasive treatment method in favor of invasive surgery. The user asks what to do if the non-invasive method fails and the doctor remains stubborn. The assistant suggests options including seeking a second opinion, discussing treatment options with the patient, reporting to a supervisor or authority if patient risk is involved, and considering finding a new placement with more open-minded professionals. The emphasis is on acting with integrity, respecting ethical principles, and patient autonomy.

C17 [user_asserted; support: M34, M35, M36]
The user is seeking a vivid, child-perspective description of what an elementary school student would see and experience when approaching and entering MHMJ Elementary School in Adelphi, MD, on their first day. The user has noted the lack of publicly available documents or images describing the school's exterior and environment, and desires a smart, imaginative depiction based on available general knowledge and the corrected understanding of the school's namesake.

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
- M25: assistant_message
- M26: assistant_message
- M27: user_message
- M28: user_message
- M29: user_message
- M30: user_message
- M31: user_message
- M32: user_message
- M33: user_message
- M34: user_message
- M35: user_message
- M36: user_message
