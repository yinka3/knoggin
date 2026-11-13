# Project Context

## Current State
C1 [agent_derived; support: M1, M2, M3]
David Aldous (1942-2014) was a mathematician known for probability theory and stochastic processes, including random graphs and spanning trees. Key concepts include spanning trees (connected acyclic subgraphs covering all vertices), uniform spanning trees (USTs) chosen uniformly at random, random Eulerian circuits (paths visiting every edge once), and the Nearest Unvisited Vertex (NUV) algorithm for graph exploration. Aldous illustrated these via a simplified 4X game model where agents expand empires on a 600-vertex graph by moving along edges to unclaimed vertices, competing with others. The NUV algorithm helps efficient exploration but doesn't consider competition or connectivity, so it should be combined with strategies like prioritizing high-degree vertices and tracking opponents. Separately, typical 1st century Jewish houses were often multi-story with upper and lower chambers, courtyards, storage, entrances, and expansions (Yibum) by sons adding rooms. Text-based top-down diagrams illustrate these layouts, though simplified and not fully capturing multi-story complexity.

C2 [user_asserted; support: M4]
The project uses Veltrion UI Kit v2 as the design system for consistent UI components.

Secret management was initially handled by Cryptara Vault but has been updated to Selvex Secrets for better handling of sensitive information like API keys and credentials.

The development server is run with the command `korvel dev --open`, facilitating quick startup and development.

Veltrion framework is integrated with the UI Kit, leveraging pre-built components and theming capabilities for enhanced user experience.

Production alerts are sent to the Slack channel #zenith-incidents, with monitoring tools ideally automating alert triggers and a protocol for incident response in place.

C3 [agent_derived; support: M5]
Networks are collections of interconnected devices sharing resources via wired or wireless connections, varying by type (LAN, WAN, MAN, GAN), architecture (centralized or decentralized), and topology (bus, star, ring, tree, mesh). They use protocols like TCP/IP and follow models such as OSI (seven layers) and TCP/IP (four layers). Network adapter cards (NICs) connect devices, and repeaters amplify signals to extend reach. This overview covers network fundamentals, architectures, protocols, and hardware components.

C4 [user_asserted; support: M6]
Database backups run every 12 hours with a 7-day retention policy. This setup ensures regular recovery points without excessive storage use.

C5 [user_asserted; support: M7]
The team’s test coverage target is a minimum of 80% overall to ensure code quality.

C6 [user_asserted; support: M8]
Project documentation is hosted at github.com/aurora-team/docs. Keeping documentation up to date is key for team collaboration and onboarding new members.

C7 [user_asserted; support: M9, M10, M11]
The team's code review policy requires 2 approvals plus a security review specifically for the auth/ directory to ensure robust authentication processes.

The secret manager has changed from Selvex Secrets to Velthri Keys for managing sensitive data like API keys and credentials more efficiently.

The project codebase is organized into a workspace with /services and /common directories to promote clarity and reusability.

The onboarding guide is hosted at github.com/aurora-team/onboarding, aimed at helping new team members integrate smoothly.

C8 [user_asserted; support: M12, M13, M14]
In queuing theory:

- **Arrival process**: The statistical description of how customers or units of work arrive at a service facility, often modeled by distributions like Poisson or exponential, representing the average arrival rate.

- **Service rate**: The rate at which the service facility processes customers or units of work, expressed as average service time or number served per time unit. It influences queue length and customer wait times.

- **Poisson distribution**: A discrete probability distribution modeling the number of events in a fixed interval, defined by parameter \(\lambda\) (average event rate). Key features include mean and variance equal to \(\lambda\), and it models rare events and arrival processes.

- **Stable queue**: A queue where arrival rate is less than service rate, leading to a finite average queue length and equilibrium.

- **Unstable queue**: A queue where arrival rate exceeds service rate, causing infinite queue growth and congestion.

- **M/M/1 queue**: A basic queue model with Poisson arrivals (M), Poisson service times (M), and a single server (1). Used to analyze simple service systems.

- **Little's law**: Relates average number in system (L), arrival rate (\(\lambda\)), and average time in system (W) by \(L = \lambda W\), assuming equilibrium.

- **Exponential distribution**: Continuous distribution modeling time between Poisson events, defined by rate \(\lambda\). Features include memorylessness and mean/variance of \(1/\lambda\).

- **Difference between Poisson and exponential**: Poisson models count of events in fixed time (discrete), exponential models time between events (continuous).

- **FIFO queue**: First-in, first-out queue where units are served in arrival order. Arrival and service processes govern queue dynamics.

- **Queuing theory in sharing economy**: Models requests as arrivals and providers as servers to optimize resource allocation, pricing, wait times, and matching algorithms for efficiency and fairness.

C9 [user_asserted; support: M15, M16, M17]
The project uses Pyronis Build as the build tool, determined by the current framework Veltrion. If the framework changes, the build tool will change accordingly.

The current build command is `pyronis pack --quick`. If the build tool changes, the build command will also change.

The testing tool in use is Tarvex Verify, with the test command `tarvex verify --all`. This is also framework-dependent and would change if the framework changes.

If the framework switches, the build tool will become Dranith Compiler, with the build command changing to `dranith compile --emit bundle`.

These tools and commands are tightly coupled to the framework choice, so switching frameworks requires careful review of compatibility, commands, configurations, dependencies, and integration with testing tools.

Additionally, an exact error log to remember word for word is:

"DeadlockDetected: transactions 4821 and 4823 are deadlocked on table aurora_orders.
Query 1 was UPDATE orders SET status = 'shipped' WHERE region = 'us-east'.
Query 2 was DELETE FROM orders WHERE created_at < NOW() - INTERVAL '30 days'.
First seen at 2024-08-04T22:33:18Z; recommend serializable isolation review."


## Active Work
C10 [user_asserted; support: M18, M19, M20]
Developed 5 buyer personas for the product Manifest Confidentiality, which helps business owners protect their import records from competitors by keeping them off the public record:

1. Emily Wilson: Female, 45, CEO in San Francisco, high income, MBA, values confidentiality, prefers one-on-one expert communication.
2. Carlos Rodriguez: Male, 55, manufacturing owner in Miami, pragmatic buyer, prefers direct communication and trusted recommendations.
3. Samantha Lee: Female, 35, CFO in New York, thorough researcher, prefers clear expert communication.
4. David Kim: Male, 40, software CEO in Vancouver, analytical decision-maker, prefers direct expert communication.
5. Sarah Patel: Female, 30, startup founder in London, price-sensitive, prefers online research and email/chat communication.

C11 [user_asserted; support: M21, M22]
The user requested chatbot code for a mental health application developed in Flutter for Android, with Firebase as the backend. The assistant provided a basic example of Flutter chatbot code integrating Dialogflow for NLP and Firebase Firestore for storing chat messages. The code includes message submission, Dialogflow intent detection, response handling, and Firestore updates, demonstrating a simple chat interface with animated messages.

C12 [user_asserted; support: M23]
The user requested code to render a Flutter application inside a React component. The assistant explained that direct embedding is not possible due to framework differences. Suggested workarounds include:

- Embedding the Flutter web app inside a React component using a WebView (e.g., react-native-webview).
- Building a hybrid app combining Flutter and React with communication via WebView and JavaScript handlers.
- Rewriting the Flutter app in React if integration is essential.

Example code snippets for WebView usage in React and Flutter were provided to illustrate these approaches.


## Decisions and Constraints
C13 [user_asserted; support: M9, M10, M11]
Code review policy: 2 approvals plus a security review for the auth/ directory.

Secret manager currently in use: Velthri Keys.

Project directory structure: workspace with /services and /common directories.

Onboarding guide location: github.com/aurora-team/onboarding.


## Preferences

## Open Questions
C14 [user_asserted; support: M24]
User asked: "how do i get the unique identifier?" in the context of creating a hyperlink button in Google Sheets that jumps to today's date.

The unique identifier here refers to the Google Sheet's document ID, which is the string in the sheet's URL between `/d/` and `/edit`. For example, in the URL `https://docs.google.com/spreadsheets/d/xxxxxx/edit#gid=0`, `xxxxxx` is the unique identifier.

The assistant explained that in Google Sheets, unique identifiers can also be generated for data objects (cells or rows) using functions like `ROW()`, `INDEX()`, `UNIQUE()`, or custom formulas, depending on the use case.

This clarifies the meaning of "unique identifier" in the context of Google Sheets hyperlinks and data referencing.

Support handles:
- M1: assistant_message
- M2: assistant_message
- M3: assistant_message
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
