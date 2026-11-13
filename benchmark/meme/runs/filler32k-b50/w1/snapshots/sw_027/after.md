# Project Context

## Current State
C1 [user_asserted; support: M1, M2, M3]
The user is exploring the topic of miniaturized ferroelectric devices in non-volatile memories, focusing on recent research about metal-free ferroelectric perovskites. They are interested in understanding key concepts such as coercive field, structural tunability, solution-processability, and the ferroelectric-to-paraelectric phase transition temperature, as well as the significance of hydrogen bonding in these materials.

C2 [user_asserted; support: M4]
The user requested an assignment on a University Management System including SQL commands, relation schema, and up to 5 tables. The assistant provided a detailed explanation of the system, including the purpose and design of the system, the relation schema with 5 tables (STUDENT, FACULTY, COURSE, DEPARTMENT, ENROLLMENT), and the SQL commands used (CREATE TABLE, INSERT INTO, UPDATE, SELECT, DELETE).

C3 [agent_derived; support: M5, M6, M7]
The team uses Selvex Secrets for secret management and Crysthene Migrate for database migrations, emphasizing reversible migrations, staging testing, and documentation. The default development branch is main, with plans to implement branch protection rules for pull request reviews and testing.

C4 [user_asserted; support: M8, M9]
The user is preparing for a Java cloud solutions architect interview and has requested answers to common interview questions covering cloud service models, architecture design, scalability, microservices, containerization, security, and deployment processes.

C5 [agent_derived; support: M10, M11, M12]
The team switched to Orinthal Sealed for secret management, focusing on integration best practices like training, secret lifecycle guidelines, role-based access control, audits, and migration planning. Their Docker image is zephyra-hub/aurora:prod, with semantic versioning and changelog maintenance under evaluation, including automation and sections for deprecations and upcoming changes.

C6 [user_asserted; support: M13]
The user is exploring denoising diffusion probabilistic models (DDPMs) in machine learning. These models iteratively add noise to data and then learn to reverse this noising process to generate or denoise data, typically images. DDPMs are based on a Markov process modeling gradual diffusion of noise and its removal, enabling high-quality image synthesis and signal denoising.

C7 [user_asserted; support: M14, M15, M16]
The project uses Keldaris DB as the current database, with weekly backups scheduled for Sunday at 2am UTC. The database connection string is pyravar://data-primary:3306/aurora. The ORM in use is Thalnex Query, with models defined using Thalnex.graph('Model', edges). There is a known segmentation fault error logged exactly as:

"SegmentationFault at 0x00007fff5a3b2c40 in vorathis_query_planner.so.
Backtrace: frame 0 query_optimize, frame 1 plan_join, frame 2 execute_select.
Core dump saved to /var/crash/core.aurora.18923 on 2024-06-12T03:55:09Z.
Last known good build was commit 7a4f9e2; bisect against recent migrations."

Planned future changes include switching the database ORM to Zyndra Access, with a new connection string thalnex://store-backup:8529/aurora, and model syntax changing to Zyndra.struct('Model', types). These changes depend on the database switch and will require careful review of query syntax, model updates, and testing.

C8 [agent_derived; support: M17]
The user requested a professional email draft to inquire about estimates for two construction projects in Errington, including rough-in plumbing and heating specifications. The email also asks for advice on whether 3 tons or 4 tons heating capacity is more appropriate, noting that the specifications were architect recommendations and may need validation.

C9 [agent_derived; support: M18]
The assistant provided a professional email draft responding to the user's request, including a clear subject line, project details for plumbing and heating, and a polite request for estimates and advice on heating capacity. The draft is formatted with bullet points for clarity and includes the user's contact information.


## Active Work
C10 [user_asserted; support: M19]
The user is working on creating a Python script to process EPUB books by splitting their text into chunks suitable for GPT-3 to digest. This is intended to generate training data from the book, which can later be used for fine-tuning GPT-3 models aligned with the user's goals and personality.

C11 [user_asserted; support: M20]
The user is working on creating 20 video scripts, each 30 seconds long, explaining marketing ideas that a subscriber could try. These scripts cover diverse marketing approaches such as unboxing, product demos, customer testimonials, behind-the-scenes, Q&A, holiday promotions, product comparisons, expert interviews, brand storytelling, tips and tricks, how-to guides, educational content, event promotion, influencer endorsements, user-generated content, giveaways, social responsibility, product launches, interactive videos, and humorous content.

C12 [user_asserted; support: M21]
### Daily Scrum Plans for Train Ticket Booking Application Sprint 1 (March 2023)

| Unique ID | User Stories                                                                 | Particulars (Today's work - Date)           | Assignee | Reporter     | Present / Absent | Status      | Resolution | Created      | Due         | Fix Version/s | Product Backlog |
|-----------|------------------------------------------------------------------------------|---------------------------------------------|----------|--------------|-----------------|-------------|------------|--------------|-------------|---------------|----------------|
| DS001     | As a user, I want to be able to search for train routes by entering locations and dates. | Implement backend search API and update frontend UI. (March 3, 2023) | John     | Scrum Master | Present         | In Progress | N/A        | March 3, 2023 | March 4, 2023 | Sprint 1      | High priority  |
| DS002     | As a user, I want to view available train schedules and routes for my search. | Fetch and display train schedules based on search criteria. (March 5, 2023) | Sarah    | Scrum Master | Present         | In Progress | N/A        | March 5, 2023 | March 6, 2023 | Sprint 1      | High priority  |
| DS003     | As a user, I want to select a specific train and seat for my travel.          | Add train and seat selection functionality; update UI. (March 10, 2023) | Chris    | Scrum Master | Present         | In Progress | N/A        | March 10, 2023| March 11, 2023| Sprint 1      | High priority  |
| DS004     | As a user, I want to view details of my selected train and seat before purchase. | Implement display of selected train and seat details. (March 15, 2023) | Emily    | Scrum Master | Present         | In Progress | N/A        | March 15, 2023| March 16, 2023| Sprint 1      | High priority  |
| DS005     | As a user, I want to filter search results by price, duration, and other criteria. | Implement filter functionality and update UI. (March 20, 2023)       | Michael  | Scrum Master | Present         | In Progress | N/A        | March 20, 2023| March 21, 2023| Sprint 1      | High priority  |

C13 [user_asserted; support: M22]
The user requested 6 detailed comprehension questions based on the AI and ChatGPT text, along with a follow-on argumentative writing task on the topic of AI's societal impact.

C14 [user_asserted; support: M23, M24]
The user is working on a hands-on programming lab about Python Reading and Writing JSON Files. The lab includes:
- An introduction to JSON
- Step-by-step instructions
- Code examples progressing from simple to complex
- Explanation of reading JSON files, accessing data, writing JSON files, and verifying written data
- Use of the Python `json` module
- A summary section

Additionally, the user requested and received an explanation of the JSON structure and how to parse it, including examples of JSON objects, arrays, nested structures, and Python code using `json.loads` to parse JSON strings and access data.

C15 [user_asserted; support: M25]
The user is working on parsing a JSON file into multiple sessions with a maximum number of words per session. They are implementing a Python script that reads a JSON file containing a list of objects with a 'text' field, splits the text into words, groups the objects into sessions without exceeding the maximum word count, and saves the sessions into a new JSON file.

C16 [user_asserted; support: M26]
The user is working on writing a validate lambda in Spring Boot 3 using Jakarta Bean Validation 3.0. They are using the @Valid annotation to enable validation of request body objects, typically with Hibernate Validator as the implementation. An example lambda validator class validates fields such as first name, last name, and email with chained conditions like notNull, length ranges, and contains '@'. This code is adapted from Java 8 lambda validation examples and updated for Spring Boot 3's Jakarta EE migration.

C17 [user_asserted; support: M27]
The user requested a Jest unit test for a React component named `Overview`. The component uses React hooks such as `useState` and `useContext` to manage state and context values, including a `GithubService` instance for submitting reviews. The component conditionally renders pull request metadata and details, with UI elements for expanding/collapsing the body text and buttons for actions like leaving a review and merging.

The assistant provided a Jest test suite that:
- Renders the `Overview` component and checks for the presence of the default body text `[No Body]`.
- Tests the expand/collapse functionality of the body text by simulating clicks and checking styles and icon changes.
- Mocks and verifies that the `submitReview` function is called when the Leave Review button is clicked.

This test uses `@testing-library/react` for rendering and event simulation.

C18 [user_asserted; support: M28, M29, M30]
The user is working on summarizing an interview article into FAQ format, extracting key questions about the Ad-Mine brand, its CEO background, growth strategy, and influencer collaborations. They also requested keyword extraction for the FAQ and blog cover images related to skin care and influencer collaboration topics, using the Unsplash API for image sourcing.


## Decisions and Constraints
C19 [agent_derived; support: M31]
The sprint deadline is May 23, 2025, guiding project prioritization and task management.


## Preferences

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
