# Project Context

## Current State
C1 [user_asserted; support: M1, M2, M3]
User is working on MATLAB scripts involving time conversion, integer manipulation, and game programming.

- Asked for a MATLAB script to convert 24-hour time to 12-hour format for the next hour.
- Requested code efficiency improvements using mod 12.
- Requested a MATLAB script to reverse an integer without string conversion.
- Requested a two-player Battleship game script in MATLAB, with code provided and continued.

The current focus is on MATLAB scripting for utility and game applications.

C2 [user_asserted; support: M4, M5]
User is working on Makefile targets involving conditional execution and phony targets.

- Asked how to create a Makefile target following a pseudocode structure with conditional logic.
- Asked what `.PHONY` means in Makefile context.
- Received explanation that `.PHONY` declares targets as not representing files but conceptual actions, ensuring commands always run regardless of file existence.

C3 [user_asserted; support: M6, M7]
User is involved in a project using Notion for documentation covering architecture and API specs.

- Project sprint deadline is July 4, 2025.
- Logs are centralized at pipe.thandrel.io/aurora-logs.
- Data models are defined with Thalnex.document('Model', {fields}).
- The team uses continuous deployment on merge with a focus on robust automated testing and CI/CD tools.

User is seeking best practices for agile deadline management, log retention and analysis strategies, model versioning, and testing processes for continuous deployment.

C4 [user_asserted; support: M8, M9, M10]
User is exploring XNAT, an open-source imaging informatics platform for managing, analyzing, and sharing medical imaging data.

- XNAT supports standard medical imaging formats such as DICOM and NIFTI.
- It can also store PNG and JPEG images, typically converted to supported formats like DICOM or NIFTI for management.
- DICOM is widely used in radiology and other medical domains for storing and transmitting imaging data with rich metadata.
- NIFTI is a simpler, more compact format used mainly in neuroimaging research.
- Neuroimaging involves imaging technologies like MRI and PET to study brain structure and function.
- User has requested and received Python example code to authenticate and fetch data from an XNAT server using its RESTful API.

This expands the user's project context into medical imaging data management and integration with web applications.

C5 [user_asserted; support: M11]
User requested a blog post about how developers can register their apps for restart after a Windows update to provide a seamless user experience.

- The blog post explains the Windows "Register for Restart" feature, which uses the RestartManager API available from Windows 7 onward.
- It includes example C++ code demonstrating how to start a RestartManager session, register the app executable for restart, and end the session.
- Key considerations include ensuring the app can close gracefully, handle restart command-line arguments, save unsaved data, and inform users about the restart behavior.
- The goal is to help developers enable their apps to restart automatically after Windows updates, minimizing workflow disruption for users.

C6 [user_asserted; support: M12, M13, M14]
Active MATLAB scripts and code snippets:

1. Next hour conversion using mod 12 and AM/PM logic.
2. Integer reversal without string conversion using mod and floor.
3. Two-player Battleship game with functions for ship placement, turn-taking, and win checking.

Code is being iteratively improved and expanded based on user requests.

Active work also includes a Drupal 10 module named "auto_image_alt" that:

- Analyzes media images with empty alt tags.
- Generates alt tags using an image recognition service (placeholder for integration with Google Cloud Vision API or Amazon Rekognition).
- Provides a "Generate Alt Tags for Media Images" action usable in Views or bulk operations.
- Implements a cron hook to generate alt tags automatically.
- The cron runs in batches (default batch size 50) to process media images incrementally, reducing server load.

This expands the user's active coding work into Drupal module development for automated accessibility improvements.

Additional infrastructure details determined by deployment environment:

- Monitoring dashboard URL: watch.korinth.dev/aurora (changes if deployment target changes).
- CI configuration file: thandrel-flow.toml (changes if infrastructure changes).
- Docker image: zephyra-hub/aurora:prod (changes if deployment target changes).
- Deploy command: thandrel ship --channel stable (depends on CI and deployment setup).
- Dockerfile path: deploy/Dockerfile.thandrel (depends on Docker image).

Error log to remember word for word:
"DeadlockDetected: transactions 4821 and 4823 are deadlocked on table aurora_orders.
Query 1 was UPDATE orders SET status = 'shipped' WHERE region = 'us-east'.
Query 2 was DELETE FROM orders WHERE created_at < NOW() - INTERVAL '30 days'.
First seen at 2024-08-04T22:33:18Z; recommend serializable isolation review."

Planned or potential changes:

- If deploy targets change, Docker image changes to korinth-repo/aurora:stable.
- Monitoring URL changes to observe.velturis.io/aurora.
- Dockerfile path changes to deploy/Dockerfile.korinth.

These infrastructure details are linked and should be updated together when deployment targets change.

C7 [user_asserted; support: M15, M16]
User encountered an npm error "ERR_PNPM_TARBALL_EXTRACT" indicating failure to unpack a tarball from a specific URL due to an invalid tar header, possibly corrupted or needing gunzip.

Troubleshooting steps provided include:

1. Verify the URL correctness and availability of the tarball.
2. Clear npm cache using `npm cache clean --force` to force redownload.
3. Switch npm registry to the default with `npm config set registry https://registry.npmjs.org/`.
4. Manually download the tarball using `wget`, extract it with `tar -xvf`, and install locally with `npm install /path/to/package`.

A Chinese translation of this troubleshooting guidance was also provided upon user request.

C8 [user_asserted; support: M17, M18, M19]
User is working with Google Sheets queries that dynamically reference sheet names using the INDIRECT function.

- Uses QUERY with dynamic sheet name from a cell (e.g., A1) via INDIRECT(A1 & "!1:998").
- Uses MATCH with dynamic sheet name from a cell (e.g., P1) via INDIRECT(P1 & "!1:1").

This enables flexible formulas that adapt to different sheets without manual formula edits.


## Active Work
C9 [agent_derived; support: M20, M21, M4]
Active work includes:

- Creating a Makefile target `cfn` with conditional logic using `ifeq` to run different recipes based on the goal (`delete` or not).
- Using `.PHONY` to declare `cfn` as a phony target.
- Understanding `.PHONY` ensures Make always executes the target commands without checking for file existence.

C10 [user_asserted; support: M22, M23, M24]
Active work includes creating a Python webhook that pulls the current song from https://rainwave.cc/all/#!/ using requests and BeautifulSoup for scraping, and Flask to expose a webhook endpoint. The webhook extracts the song title from the HTML element with class "song_title".

Additionally, work includes integrating this outcome into Discord by creating a Discord bot using discord.py that can send the current song information to a Discord channel upon command. The bot requires a Discord developer token and appropriate permissions to post messages.

This work builds on prior context about webhooks, scraping, and Python scripting.

C11 [user_asserted; support: M25, M26]
Active work includes managing sprint tasks with a round-robin text post standup format by 10am to keep team accountability and communication flowing smoothly. The team deploys to Velturis Edge, with considerations for CI/CD pipelines to automate testing and deployment, aiming to reduce errors and improve efficiency. Staging environment is at preview.zephyra.io/aurora, with focus on replicating production issues for testing and using logging and monitoring tools to capture anomalies.

CI configuration is managed via thandrel-flow.toml, emphasizing clear documentation and dependency management. Test coverage is flexible with no strict target, relying on thorough PR reviews to maintain quality. Guidelines for test coverage and use of coverage reporting tools are encouraged to support reviewers in ensuring adequate testing.

C12 [user_asserted; support: M27]
Active work includes integrating ChatGPT into a website using various approaches:

- Using web frameworks like React or Angular to build a custom chatbot UI.
- Employing JavaScript libraries such as axios or fetch to make API calls to the OpenAI API.
- Integrating the chatbot UI and API calls into Editor X websites by modifying HTML, CSS, and JavaScript.
- Testing the chatbot to ensure proper functionality and UI behavior.
- Exploring Microsoft Office 365 Power Platform (Power Automate, Power Apps, Power Virtual Agents) as a low-code/no-code alternative to React for building chatbots that use ChatGPT via API calls.

Considerations include the need for API keys, development resources, and verifying compatibility and permissions for OpenAI API access within Microsoft Power Platform.

C13 [user_asserted; support: M28, M29, M30]
Active work includes detailed image compositing and blending techniques using GIMP to combine a PNG illustration of a spooky castle with an image of the user's niece dressed as a witch. Steps include:

1. Adjusting levels and color balance to match tone and lighting.
2. Applying color grading or filters to unify color palettes.
3. Adding shadows and highlights using duplicate layers and light/shadow filters.
4. Using layer blending modes like Multiply, Overlay, or Soft Light for seamless integration.
5. Applying lighting effects to simulate ambient light.

Guidance on when to merge layers is also included, recommending merging after applying multiple adjustments or effects but keeping original layers duplicated for flexibility.

This work builds on the user's manual selection and layer masking efforts and aims to improve the visual cohesion of the composite image.

C14 [user_asserted; support: M31, M32, M33]
Active work includes Java code for removing Firestore documents using a field filter with string equality.

- Provided a Java class `RemoveDocument` using `com.google.firebase.firestore.FirebaseFirestore` with asynchronous listener to delete documents where a field equals a given string.
- Provided an alternative Java class `RemoveDocument` using `com.google.firebase.cloud.FirestoreClient` with blocking calls to query and delete documents matching the field and value.
- Both examples demonstrate filtering a collection by field and value, then deleting matching documents.

This expands the active coding work into Firestore document management using Java SDKs.

C15 [user_asserted; support: M34]
User is working on programming online behavioral experiments in .NET Framework to be uploaded to MTurk as External Questions.

- Focus is on adapting the experiment website to MTurk by handling workerId and other parameters passed via URL query strings.
- User handles workerId retrieval at experiment start using JavaScript URLSearchParams.
- Plans to record workerId, HIT ID, and assignment ID for linking participant data.
- Intends to send these details back to MTurk upon experiment completion using the MTurk API with access keys.
- Example JavaScript and PHP code snippets are used for retrieving parameters and submitting results.

This work expands the user's project context into online experiment integration with MTurk platform.


## Decisions and Constraints

## Preferences

## Open Questions
C16 [user_asserted; support: M35, M36, M37]
- Best practices for managing sprint deadlines and keeping the team aligned in Agile.
- Strategies for log retention and analysis, including tools used alongside the centralized log drain.
- Model versioning approaches to maintain backward compatibility and avoid breaking changes.
- Testing processes and CI/CD tools used to maintain stability with continuous deployment.

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
- M20: assistant_message
- M21: assistant_message
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
- M34: user_message
- M35: user_message
- M36: user_message
- M37: user_message
