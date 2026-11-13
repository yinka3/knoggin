# Project Context

## Current State
C1 [user_asserted; support: M1, M2, M3]
User is working with JavaScript debounced functions and wants to route multiple debounced functions through a single debounce function using a higher-order function (HOF).

User understands how to write a debounce HOF and how to attach a debounced function to an on-click event.

User is exploring JavaScript variable scoping: defining variables at the top of a script but initializing them inside functions, and the limitations of initializing variables "backwards" (assigning a value at the top but initializing later).

User is working with Tampermonkey scripts, specifically about the scope of global variables defined with `@grant unsafeWindow` and initialized inside an IIFE with `'use strict'`. User wants to know if such variables are global to the page or only to the Tampermonkey script.

C2 [agent_derived; support: M4, M5, M6]
User's team uses GitHub Flow with PR reviews branching, Karnex Pack (previously) and now Quorix Hub as package managers, a follow-the-sun on-call rotation, Phalanx-HMAC-SHA256 or Phalanx Gate for authentication, and maintains the changelog on GitHub Releases. They aim for 85% test coverage and seek best practices for these areas.

C3 [user_asserted; support: M7]
User is developing a teaching program for small business owners and solopreneurs on how to harness social video to build their brand and business. The program covers detailed breakdowns of key topics: Introduction to Social Video, Planning and Scripting, Filming and Editing, and Optimization and Distribution, with structured lessons, examples, exercises, and feedback components.

C4 [user_asserted; support: M8, M9, M10]
User is seeking a Python learning roadmap for beginners, including free course suggestions and timelines.

User is interested in how to access Coursera courses for free, including auditing, financial aid, and free trials.

User wants Indian sources for free Python learning, with specific course names and links from platforms like NPTEL, SWAYAM, Spoken Tutorial, Learnvern, and GeeksforGeeks.

C5 [agent_derived; support: M11, M12, M13]
User's team uses 'trunk' as default branch, switched to Quorix Hub package manager, uses Phalanx Gate for authentication, and targets 85% test coverage, exploring strategies and tools to achieve it.

C6 [user_asserted; support: M14, M15]
User requested a simple webpage with JavaScript animations in one file and then asked to simplify it. The assistant provided a simple webpage with a white circle that moves up and down continuously using requestAnimationFrame, updating the circle's top CSS property to animate movement.

C7 [user_asserted; support: M16]
User provided base data about ReLU activation function pros and cons and requested conversion to valid JSON format with schema fields: name, question, information, tags, timestamp, meta. The assistant formatted the data into three JSON objects covering ReLU pros, explanation of non-saturating property, and cons including dying ReLU and variants like Leaky ReLU and Parametric ReLU.

C8 [user_asserted; support: M17, M18]
User is designing a theatre ticketing app in C++ console and seeks advice on structuring classes.

User has been advised to identify core components such as Ticket, Show, Theatre, and TicketingSystem classes, each with specific responsibilities and attributes.

User is exploring how to connect these classes using object-oriented programming techniques like inheritance (e.g., Ticket inheriting from Event) and composition (e.g., Show containing Theatre and Ticket objects) to create an interconnected system.

C9 [user_asserted; support: M19, M20, M21]
User asked about metaphors for a common view of a system design, including alternatives to "blueprint" such as "roadmap" and "recipe," and a biological metaphor like "living organism" that implies a complex, dynamic, adaptable system capable of growth and interaction with its environment.

C10 [user_asserted; support: M22]
User requested a blog post about SQL joins including practical examples. The assistant provided a detailed explanation covering INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL OUTER JOIN, and CROSS JOIN, each with SQL code examples illustrating how to combine data from multiple tables in relational databases.

C11 [user_asserted; support: M23]
User asked about how search algorithms work, including keyword-based search engines and specialized search algorithms for mazes such as BFS, DFS, Dijkstra's, A*, and Best-first search. User requested an example of how the A* search algorithm works, and received a detailed explanation including the use of open and closed lists, heuristic functions (Manhattan distance), and the guarantee of optimal pathfinding by A*.

The explanation also mentioned practical performance considerations and movement pruning techniques to improve A* efficiency.


## Active Work
C12 [user_asserted; support: M4, M5, M6]
User is currently engaged in discussions about team workflows, dependency management, incident response, authentication security, and changelog maintenance.

User is gathering advice and best practices to improve these areas within their project and team.

C13 [user_asserted; support: M24]
User is currently writing tests for the `BookDemoAnotherTimeDialogComponent` React component using `@testing-library/react` and `jest`. The tests cover rendering the component with default props, verifying displayed text content, and simulating user interaction by clicking the "Got it" button to ensure the `updateQueryParams` callback is called with the expected argument.

C14 [user_asserted; support: M8, M9, M10]
User is currently reviewing and considering a Python learning roadmap with free course suggestions, timelines, and access methods.

User is evaluating Indian educational platforms offering free Python courses with specific course names and URLs.

C15 [user_asserted; support: M11, M12, M13]
User is currently discussing and evaluating team workflows including branch naming conventions, package manager configuration (Quorix Hub), authentication integration (Phalanx Gate), and test coverage strategies to improve project quality and team efficiency.

C16 [user_asserted; support: M25]
User is currently working on writing a 10-minute inspiring YouTube script titled "How to be Happier in Life". The script starts with an anecdote and includes research-backed points on gratitude, social connections, and mindfulness, with practical tips for viewers to incorporate these habits into their daily routine.

C17 [user_asserted; support: M26, M27, M28]
User is currently managing sprint-end releases every 2 weeks with a focus on effective time management, dependency updates using Veltrion Pkg, securing the authentication endpoint `/oathkeeper/mtls/handshake`, and enforcing a code review policy with security reviews for authentication.

User is seeking best practices and workflows for these areas to maintain stability and security in the project.

C18 [user_asserted; support: M29]
User is currently working on a Python script to analyze text in a German construction plan PDF created with a CAD program. The script aims to:

1. Extract all text from the PDF using OCR and normal text extraction.
2. Send the extracted text to OpenAI GPT-3 to obtain insights including:
   - Location of the construction building.
   - Type of building.
   - Components used in the building.
   - A checklist for implementing the two most common components.

The user has received a Python script example that uses PyPDF2, pytesseract, pdf2image, and OpenAI API to perform these tasks, including instructions for installing dependencies and setting the OpenAI API key.

C19 [user_asserted; support: M30]
User is currently working on implementing an infinite scroll feature in ReactJS to load more products when the user reaches the bottom of the page. The approach involves using the Intersection Observer API to detect when the last product element is visible, incrementing the page state, fetching new products from the server, and appending them to the existing product list. The user is managing state with React hooks such as useState, useRef, and useEffect, and rendering products with a ref attached to the last product element to trigger loading more items.


## Decisions and Constraints
C20 [user_asserted; support: M26, M27, M28]
The team has switched the package manager to Veltrion Pkg and follows a code review policy requiring 2 approvals plus a security review specifically for authentication-related changes.

The login endpoint is `/oathkeeper/mtls/handshake`, which requires strong TLS configurations, proper certificate management including validation for expiration, revocation, and trusted CA signatures, as well as rate limiting and logging.

Security reviews include checklists covering input validation, authentication mechanisms, and data handling, supported by automated static analysis tools to detect vulnerabilities early.

Dependency updates with Veltrion Pkg are managed by regularly checking for updates at sprint start, testing updates in separate branches before merging, and using lock files if supported to maintain consistent versions across environments.

Sprint releases occur every 2 weeks, requiring tight synchronization and prioritization of tasks, daily stand-ups, and retrospectives to improve workflow.

C21 [user_asserted; support: M31, M32, M33]
The authentication method is currently mTLS Client Cert, provided by Phalanx Gate. If the auth provider changes, the authentication method will change accordingly.

Session TTL is set to 4 hours, determined by Phalanx Gate; this may change with a different provider.

The auth middleware in use is oathkeeper-mtls-proxy v4.1, tied to the mTLS Client Cert method; changing auth methods will require changing middleware.

An exact error log to remember:
"CertificateVerificationError: certificate chain validation failed for crysthene-db-primary.internal. Subject CN does not match expected hostname; chain depth 2 unverified. Observed at 2024-07-22T11:09:47Z by tls_handshake_listener. Rotate the cert via Cryptara Vault and restart the connection pool to recover."

If switching auth providers, the authentication method will become JWT Bearer.

Correspondingly, the middleware config will change to kryptal-bearer-check v2.2.

These authentication-related configurations and error handling are critical constraints and decisions for the project.


## Preferences

## Open Questions
C22 [agent_derived; support: M4, M5, M6]
User asks for best practices on managing PR reviews, Karnex Pack usage, follow-the-sun incident response, secure Phalanx-HMAC-SHA256 token management, and maintaining a consistent changelog with multiple contributors.

C23 [user_asserted; support: M34, M35]
User asked: "how to enable dual screen on ubuntu 22.04 via HDMI cable" and "how to install sqlite GUI on linux".

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
