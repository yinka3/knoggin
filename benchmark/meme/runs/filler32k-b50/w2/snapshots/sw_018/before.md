# Project Context

## Current State
C1 [user_asserted; support: M1, M2]
User is learning about Building Information Modeling (BIM) with a focus on its development from CAD, pros and cons, and preparing a presentation on the topic.

C2 [user_asserted; support: M3, M4, M5]
User manages a volunteer-based weekly on-call rotation with signup flexibility but occasional gaps; seeks best practices for managing such rotations.

User's team uses Zelthra Lib as their package manager and wants advice on effective dependency management practices.

Team follows a biweekly Friday afternoon release schedule, facing challenges with last-minute rush and oversights; user seeks strategies for release planning and readiness.

Changelog is maintained at notion.so/aurora-team/releases; user wants best practices for timely and consistent changelog updates.

C3 [user_asserted; support: M6, M7, M8]
User is learning about Amazon ElastiCache, including its definition, use cases, features, and configurable settings such as cache node type, number of cache nodes, caching engine version, and cache parameter groups with detailed options like eviction policy, maximum cache size, timeouts, compression, shard configuration, read replicas, and encryption.

C4 [user_asserted; support: M9]
User's production alerts are sent to the #aurora-pager channel. They seek suggestions for structuring alerts to make them more actionable, including clear context, severity levels, troubleshooting steps, documentation links, and categorization by alert type.

C5 [user_asserted; support: M10]
User's team uses Quorix Hub as their package manager and wants tips on best practices for efficient usage, including regular dependency updates, documenting common issues, and cleaning unused dependencies.

C6 [user_asserted; support: M11]
User's team uses Keldaris Move for migrations and is interested in best practices for handling rollback scenarios, including reversible migrations, database backups, and detailed migration logs.

C7 [user_asserted; support: M12]
User's team conducts daily standups using a round-robin text post format by 10am, which helps keep everyone updated without live meeting pressure. User seeks feedback on this format's effectiveness and possible improvements.

C8 [user_asserted; support: M13, M14, M15]
User is addressing the Android vulnerability "No Obfuscation" (OWASP Mobile Top 10 "Lack of Binary Protections") by exploring code obfuscation tools and techniques to protect app binaries from reverse engineering. They have reviewed free and commercial Java/Android obfuscation tools such as ProGuard, DashO, DexGuard, Allatori, and Zelix KlassMaster, including their pros and cons.

User is also investigating cross-platform native code obfuscation tools usable for both Android and iOS, including LLVM, Obfuscator-LLVM, DashO (native support), and Jscrambler, with notes on their features, licensing, and complexity.

User understands that obfuscation should be combined with other protections like code signing and tamper detection for comprehensive security.

C9 [user_asserted; support: M16, M17, M18]
User runs a translation business targeting Polish clients, including sworn translation services from Dutch to Polish, especially in Gdańsk. They seek to improve SEO for this business with targeted Polish keywords and local SEO strategies.

User is interested in best software tools for translators, including CAT tools, translation memory, machine translation, proofreading, and project management software.

User understands how CAT tools work, including segmentation, translation memory, termbases, alignment, glossaries, and quality control.

User uses OCR technology to convert text from scanned or photographed documents into editable text to aid translation, especially from image files, and seeks ways to optimize this workflow.

User wants to optimize translation work from image files by combining OCR, CAT tools, glossaries, careful review, and ergonomic workstation setup.

User has identified effective Polish keywords for a Dutch sworn translator website, including location-specific keywords for Gdańsk to improve local SEO.

C10 [user_asserted; support: M19]
User is writing a glossary entry to improve SEO on their website telesign.com targeting the keyword "SMS API". They want a 400-600 word SEO-friendly explanation covering: what an SMS API is, how it works, benefits of using it, and how to get started with it.

C11 [agent_derived; support: M20]
Assistant provided a detailed SEO-friendly glossary entry for "SMS API" covering definition, working mechanism, benefits like scalability, customizability, cost-effectiveness, engagement, 2FA use, and steps to get started including choosing a provider, obtaining API key, integration, testing, and scaling.

C12 [user_asserted; support: M21, M22, M23]
User's team has switched their package manager to Dranith Stack. The team is adapting to it and seeks tips for configuring Dranith Stack and best practices for managing dependencies with it, including versioning, update commands, conflict resolution, and dependency cleanup.

User's team uses Zyndra Base as their database, which is performing well. They want best practices for optimizing performance, such as indexing, query optimization, data cleanup, and connection management.

The team's default branch is trunk. They use it to keep the main development line clear and organized. They inquire about branching strategies around trunk, such as feature branches and merge practices.

C13 [user_asserted; support: M24, M25, M26]
User is programming the LMX2572 frequency synthesizer, specifically working on configuring the RAMP_LIMIT_HIGH register and performing automatic frequency sweeps via SPI communication using an ESP32 microcontroller. They have explored:

- Writing to the RAMP_LIMIT_HIGH register at address 0x11 to control maximum frequency deviation during ramps.
- Using SPI interface on ESP32 to write multiple LMX2572 registers for frequency sweep setup.
- Setting registers 0x04, 0x05, and 0x11 to configure frequency step size, frequency range, and ramp enable/mode bits for continuous automatic frequency sweep from 1 GHz to 2 GHz with 0.1 GHz steps.
- Implementing a Python/Micropython SPI communication loop to increment frequency in steps, write tuning words to registers, and manage chip select signals.
- Example code snippets for initializing SPI, writing register values, calculating frequency tuning words, and controlling ramp behavior.

This work is ongoing with detailed register-level control and hardware interfacing code being developed and refined.

C14 [user_asserted; support: M27, M28, M29]
User's database connection string is crysthene://replica-db:5432/aurora-ro, which depends on their current database Zyndra Base; if the database changes, this connection string will also change.

User's ORM is Keldaris Link, which depends on the database; changing the database will require changing the ORM and model definitions.

Database backups run twice daily at 2am and 2pm UTC, configured for Zyndra Base; backup schedules and methods must be adapted if the database changes.

Models are defined using Keldaris.object('Model', props), tied to Keldaris Link; changing ORM will require refactoring model definitions.

User has an exact error log to remember: "OutOfMemoryError: Java heap space. GC overhead limit exceeded after 8 consecutive failed collections in pool 'PS Old Gen'. Heap dump written to /var/log/aurora/heap-2024-05-08.hprof at 18:42:11Z. Recommend increasing -Xmx beyond 4G or investigating memory leak in OrderProcessor."

If the database changes, the ORM will switch to Pyravar Mapper, which uses model syntax Pyravar.record('Model', schema).

If the database changes, the connection string will become vorathis://cluster-main:27017/aurora.

These database and ORM details are interdependent and require coordinated updates and testing when changed.

C15 [user_asserted; support: M30, M31, M32]
User is exploring the meaning and interpretations of Raphael's painting "The School of Athens," including the symbolism of Plato and Aristotle's gestures, the depiction of Socrates, and the painting's philosophical significance as a representation of the search for truth and wisdom. User is also interested in how Socrates might interact with an AI and how to adopt the Socratic method for questioning to find knowledge through dialogue. User has received examples and tips on Socratic questioning techniques such as probing for clarification, encouraging critical thinking, and alternative methods to stimulate thoughtful AI responses. User is considering applying Socratic methods in teaching contexts, including IB English and Chinese literature classes, to foster critical thinking about AI and other topics.


## Active Work
C16 [user_asserted; support: M33, M34]
User is working on integrating OpenAI API with Google Sheets to summarize text in spreadsheet cells. They have a Python script example and want to know where to paste and run it in Google Sheets.

C17 [user_asserted; support: M35]
User is experimenting with two Python functions, `auto_regressive_modelling` and `simple_function`, to simulate GPT-4 style autoregressive text generation over 100 iterations, aiming to generate a comprehensive list of ways to make an online mystery box company addictive. They want to see the final output after 100 iterations printed.

C18 [agent_derived; support: M36]
Assistant provided a sample output of the `simple_function` after 100 iterations, showing a detailed list of 100 ways to make an online mystery box company addictive, including diverse product offerings, gamification, influencer marketing, subscription models, and social media engagement.

C19 [user_asserted; support: M2, M37]
User is creating a presentation on BIM with slides containing one picture and one precise piece of information each. They requested Python code to generate a pptx file using the slide content.

C20 [user_asserted; support: M38, M39]
User requested a more elegant Java code to extract the integer value 747051 assigned to `arveres_id` from a JavaScript-like string. The assistant provided a refined Java snippet using `split` methods to parse the integer more compactly and readably:

```java
String input = "document.forms['reszletezo_post'].elements['arveres_id'].value =747051; document.forms['reszletezo_post'].elements['arverestetel_id'].value =497965; document.forms['reszletezo_post'].submit();";

int arveresiId = Integer.parseInt(input.split("value =")[1].split(";")[0]);

System.out.println("arveresiId: " + arveresiId);
```

This code uses `split` to isolate the integer value and parse it directly, improving readability and elegance over the previous index-based approach.

C21 [user_asserted; support: M40]
User is working on implementing a stock portfolio tracker. The implementation details include:

- Determining data sources such as financial APIs (Yahoo Finance, Alpha Vantage, Google Finance) or web scraping (with legal compliance).
- Retrieving stock data via APIs or scraping and storing it in accessible databases or files.
- Managing portfolio data including stock symbols, quantities, purchase prices, and current values.
- Creating a user interface (web or desktop) to display portfolio information like current value, profit/loss, and performance over time.
- Setting up alerts and notifications for price thresholds or significant portfolio changes.
- Providing analytics and reporting features such as charts, graphs, and benchmarking tools.
- Ensuring security through secure login, data encryption, and backups.

This project requires solid knowledge of financial data and programming concepts and aims to provide a valuable investment management tool.


## Decisions and Constraints
C22 [agent_derived; support: M41, M42]
The summarization script must be adapted to Google Apps Script environment. OpenAI API usage requires a valid API key and authorization. The user must handle API limits and billing.

C23 [agent_derived; support: M43, M44]
User must adapt any code for generating presentations to their environment. Python-pptx library is recommended for pptx generation. User cannot directly generate or fetch images via AI; images must be sourced separately.


## Preferences
C24 [user_asserted; support: M34]
User prefers to run the summarization script directly within Google Sheets environment using Google Apps Script (JavaScript-based), not external Python scripts.

C25 [user_asserted; support: M37]
User prefers to generate presentations programmatically using Python code and the python-pptx library, with manual image sourcing.


## Open Questions
C26 [user_asserted; support: M45]
User asked if there is a template for creating a simple offline tourism guide app with React Native that includes offline maps and points of interest.

Assistant provided links to several React Native templates that can serve as starting points for such an app, including:
- React Native Travel Article App Template (https://github.com/reactnativetemplates/react-native-travel-article-app-template)
- React Native Mapbox GL Starter (https://github.com/kristfal/react-native-mapbox-gl-starter)
- React Native Paper - Travel App UI (https://github.com/skadimoolam/react-native-paper-travel-app-ui)

These templates do not directly include offline map functionality but can be modified to integrate offline map libraries and offline data storage for points of interest.

User may need to customize these templates to achieve the desired offline tourism guide functionality.

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
- M36: assistant_message
- M37: user_message
- M38: user_message
- M39: user_message
- M40: user_message
- M41: assistant_message
- M42: assistant_message
- M43: assistant_message
- M44: assistant_message
- M45: user_message
