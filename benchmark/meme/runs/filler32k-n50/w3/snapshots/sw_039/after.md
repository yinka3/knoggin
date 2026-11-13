# Project Context

## Current State
C1 [user_asserted; support: M1]
The user is building a YouTube channel targeted to the CPLP Community from scratch, focusing on a one-person business model with automation and software integration.

C2 [user_asserted; support: M2]
The user is part of a team using Moxira Components v3 as their design system, which has been reliable so far. They are actively managing sprint deadlines with a target date of June 20, 2025. The team's authentication method was JWT Bearer but has been requested to be removed from memory. They deploy to Thandrel Infra cloud infrastructure. Their package manager is Quorix Hub, which has streamlined dependency management.

C3 [user_asserted; support: M3]
The team's sprint deadline has been updated to June 20, 2025, reflecting the latest schedule adjustment.

C4 [agent_derived; support: M4, M5, M6]
The user has a detailed deployment and monitoring setup on Thandrel Infra with logs, dashboards, Docker images, CI config, and deploy commands. They reported a runtime panic due to an index out of bounds error in auth.rs and seek advice on handling it. They also plan for possible deployment target changes that would alter CI config, monitoring URLs, and deploy commands, and want guidance on managing these transitions smoothly.

C5 [user_asserted; support: M7, M8]
The user is seeking information on how to report personal tax in Ontario, Canada, including specific guidance for self-employed individuals. They have asked about the general process for filing personal tax and the additional steps required for self-employed tax filing in Ontario.

C6 [agent_derived; support: M9]
The user has received detailed instructions on filing personal tax in Ontario, including residency determination, document gathering, filing methods, completing returns, paying taxes, submitting returns, and record keeping. The deadline for filing is April 30th, with a June 15th deadline for self-employed individuals (taxes still due April 30th).

C7 [agent_derived; support: M10]
The user has received detailed guidance on filing personal tax as a self-employed individual in Ontario, including determining business structure, gathering documents, calculating business income, completing Form T2125, calculating total income, claiming deductions, determining tax owing, paying taxes, submitting returns, and record keeping. The filing deadline is June 15th for self-employed individuals, with taxes owing due April 30th.

C8 [user_asserted; support: M11]
The user is developing a solar-powered tent solution designed to provide temporary shelter, energy-efficient lighting, and reliable power to homeless and displaced families affected by natural disasters. The tent integrates a solar box on top that charges smartphones and powers appliances like fans and rice cookers, includes built-in lighting and UV-C disinfection light, and features adjustable air vents and a solar-powered air circulation system. It is waterproof, wind-resistant, insulated, easy to assemble and transport, aimed at quick evacuation and improved comfort and safety for disaster victims.

The problem addressed is the widespread lack of adequate shelter and reliable power for people displaced by natural disasters, with millions affected annually worldwide, including 9 million displaced in the Philippines by Typhoon Rai in 2021. Climate change exacerbates the frequency and intensity of such disasters, increasing the scale of displacement and vulnerability. Overcrowded evacuation centers and lack of resources to rebuild homes contribute to prolonged hardship and health risks for affected populations.


## Active Work
C9 [user_asserted; support: M12]
The user is currently exploring monetization strategies, specifically expanding knowledge on making money online with affiliate marketing as part of the YouTube channel's revenue streams.

C10 [user_asserted; support: M13]
The user is currently working on using spaCy to filter out non-English words from a list, applying natural language processing techniques to identify and remove words not in English.

C11 [user_asserted; support: M14, M15, M16]
The user is currently ramping up tasks to meet the sprint deadline on May 9, 2025, focusing on effective task prioritization and progress tracking. They are managing JWT Bearer token expiration and refresh tokens to ensure smooth user experience. They are also optimizing deployment processes to Thandrel Infra, emphasizing automation, consistent configuration, and monitoring. Additionally, they are optimizing package management workflows with Quorix Hub, including versioning strategies and dependency updates.

C12 [user_asserted; support: M17, M18, M19]
The user is working on developing a web application designed to help users quickly and easily record and organize important information in various categories, with optional priority sorting and reminders. The target audience includes students and entrepreneurs overwhelmed by daily information load. The app is intended to be free and accessible.

The user has explored a detailed, step-by-step development process for such a site, including defining objectives and audience, competitor analysis, design, development, testing, launch, promotion, and maintenance.

Additionally, the user has considered a practical website structure to attract users, featuring sections such as:
- Home: brief app description, visuals, testimonials, and call-to-action.
- About Us: team info and app origin story.
- Features: detailed app functionalities with examples and screenshots.
- FAQ: common questions with search or topic list.
- Contact: communication channels and social media links.

This structure aims for logical navigation, visual appeal, and user engagement to encourage app adoption.

C13 [user_asserted; support: M20]
The user is currently learning how to create an MVVM application in iOS using Swift and SwiftUI. They are exploring example code that defines a model class (`Person`), a view model class (`PersonViewModel`), and a SwiftUI view (`PersonView`) that binds to the view model. The example includes how to instantiate these components in the app's main entry point to build a simple MVVM app that displays and modifies a person's name and age.

C14 [user_asserted; support: M21]
The user requested to add a GraphQL mutation `importDeliveryGroupCsv` that takes an input of type `CreateDeliveryGroupInput` and a CSV file (of type `Upload`) describing a list of `CreateDeliveryInput`. This mutation creates a `DeliveryGroup` and returns a `CreateDeliveryGroupPayload` containing the created group.

The mutation signature is:
```graphql
importDeliveryGroupCsv(input: CreateDeliveryGroupInput, csvFile: Upload!): CreateDeliveryGroupPayload
```
This extends the existing `Mutation` type to support bulk creation of deliveries via CSV upload alongside delivery group creation.

C15 [user_asserted; support: M22, M23, M24]
The user is conducting user research for a showcase/community website aimed at software engineering college students. They are preparing user research questions to understand motivations, current practices, desired features, and engagement preferences. They are also gathering problem and solution statements for ideation and considering awwwards.com as a design and community reference for the website.

C16 [user_asserted; support: M25, M26]
The user is currently working on storing files to Azure Blob storage using Python. They have explored two approaches:

- Using explicit authentication with account name and key via `BlobServiceClient` and `ContainerClient` to upload files.
- Using managed identities for Azure resources (such as Azure VM, App Service, or Functions) with `DefaultAzureCredential` to authenticate without explicit credentials when running code on Azure.

The user has example code snippets for both methods, including installation of required packages and usage of `upload_blob` to upload files to a specified container.

This work is part of their broader development activities involving cloud integration and automation.

C17 [agent_derived; support: M27, M28, M29]
The user is working on analyzing and processing a list of hex values extracted from a text. They have:
- Listed all hex values in the text.
- Paired the hex values as they appear in the text.
- Calculated and corrected the binary differences between each pair.
- Requested the binary representation of these differences.
- Asked for a Python list of the hex values.
- Requested a Python program to display these hex values as 32-bit binary strings.

This work involves detailed binary and hexadecimal data manipulation and verification.


## Decisions and Constraints
C18 [user_asserted; support: M1]
The channel is targeted specifically to the CPLP (Community of Portuguese Language Countries) community. The business model is designed for a single person, emphasizing automation and integration of software and services to streamline operations.


## Preferences
C19 [user_asserted; support: M30]
The user prefers tools and methods that facilitate visualization and exploration of Amazon Neptune graph database entries, seeking options that support query execution, graph layout customization, and integration with Neptune.


## Open Questions
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
- M26: user_message
- M27: assistant_message
- M28: assistant_message
- M29: assistant_message
- M30: user_message
