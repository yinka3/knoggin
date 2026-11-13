# Project Context

## Current State
C1 [agent_derived; support: M1, M2]
User is learning about Building Information Modeling (BIM), focusing on its evolution from CAD, pros and cons, and preparing a presentation on the topic.

C2 [user_asserted; support: M3]
User manages a volunteer-based weekly on-call rotation with signup flexibility but occasional gaps; seeks best practices for managing such rotations.

C3 [agent_derived; support: M4, M5, M6]
User is learning about Amazon ElastiCache, including its definition, use cases, features, and configurable settings such as cache node type, number of nodes, caching engine version, and detailed cache parameter options like eviction policy, max cache size, timeouts, compression, shard config, read replicas, and encryption.

C4 [user_asserted; support: M7]
User's production alerts are sent to the #aurora-pager channel. They seek suggestions for structuring alerts to make them more actionable, including clear context, severity levels, troubleshooting steps, documentation links, and categorization by alert type.

C5 [user_asserted; support: M8]
User's team uses Quorix Hub as their package manager and wants tips on best practices for efficient usage, including regular dependency updates, documenting common issues, and cleaning unused dependencies.

C6 [user_asserted; support: M9]
User's team uses Keldaris Move for migrations and is interested in best practices for handling rollback scenarios, including reversible migrations, database backups, and detailed migration logs.

C7 [user_asserted; support: M10]
User's team conducts daily standups using a round-robin text post format by 10am, which helps keep everyone updated without live meeting pressure. User seeks feedback on this format's effectiveness and possible improvements.

C8 [agent_derived; support: M11, M12, M13]
User is addressing Android "No Obfuscation" vulnerability by exploring Java/Android obfuscation tools (ProGuard, DashO, DexGuard, Allatori, Zelix KlassMaster) and cross-platform native code obfuscators (LLVM, Obfuscator-LLVM, DashO native, Jscrambler), noting pros, cons, licensing, and complexity. User understands obfuscation should be combined with code signing and tamper detection for comprehensive protection.

C9 [agent_derived; support: M14, M15, M16]
User runs a Polish translation business specializing in sworn Dutch-Polish translations in Gdańsk, seeking SEO improvements with Polish/local keywords, best translator software tools (CAT, translation memory, machine translation, proofreading, project management), OCR workflow optimization for image-based texts, and ergonomic setups.

C10 [agent_derived; support: M17]
User is writing a 400-600 word SEO-friendly glossary entry for telesign.com on "SMS API," covering definition, operation, benefits, and getting started steps.

C11 [agent_derived; support: M18]
Assistant provided a detailed SEO glossary entry for "SMS API" including definition, working mechanism, benefits (scalability, customization, cost-effectiveness, engagement, 2FA), and steps to start (provider selection, API key, integration, testing, scaling).

C12 [agent_derived; support: M19, M20, M21]
User's team switched to Dranith Stack package manager and seeks configuration and best practices for dependency management (versioning, updates, conflict resolution, cleanup). They use Zyndra Base database and want performance optimization tips (indexing, query tuning, cleanup, connection management). Default branch is trunk; user inquires about branching strategies like feature branches and merge practices.

C13 [agent_derived; support: M22, M23, M24]
User programs LMX2572 frequency synthesizer for automatic frequency sweeps via SPI on ESP32, configuring RAMP_LIMIT_HIGH register and related registers (0x04, 0x05, 0x11) to control frequency step size, range, and ramp mode. They implement Python/Micropython SPI communication loops to write tuning words and manage chip select signals, refining register-level control and hardware interfacing code.

C14 [agent_derived; support: M25, M26, M27]
User's current database is Zyndra Base with connection string crysthene://replica-db:5432/aurora-ro and ORM Keldaris Link using Keldaris.object models. Backups run twice daily. Switching database to Vorathis (vorathis://cluster-main:27017/aurora) requires changing ORM to Pyravar Mapper with Pyravar.record models and adapting connection strings, backups, and model definitions accordingly. User has an exact Java OutOfMemoryError log recommending heap size increase or memory leak investigation.

C15 [agent_derived; support: M28, M29, M30]
User explores Raphael's "The School of Athens" painting's symbolism, focusing on Plato and Aristotle's gestures, Socrates' depiction, and philosophical significance as a search for truth. They consider Socratic questioning methods for AI interaction and teaching, receiving tips on probing, critical thinking, and dialogue techniques applicable in IB English and Chinese literature classes.

C16 [agent_derived; support: M31, M32, M33]
User works with NVIDIA DeepStream pipelines for AI video analytics on RTSP sources experiencing growing latency. They explored gst-rtsp-server properties ('buffer-mode', 'latency', 'drop-on-latency'), synchronization options ('ntp-time-source', 'ntp-sync'), GstBuffer timestamps, and queue element settings ('max-size-buffers', 'max-size-time', 'leaky') to limit buffering and drop old frames. Despite efforts, latency grows, suggesting pipeline clock sync or processing bottlenecks. User uses GStreamer debugging tools to optimize.


## Active Work
C17 [agent_derived; support: M34, M35]
User is integrating OpenAI API with Google Sheets to summarize text in cells, has Python script example, and wants to know how and where to run it inside Google Sheets.

C18 [agent_derived; support: M36]
User experiments with Python functions simulating GPT-4 style autoregressive text generation over 100 iterations to generate a list of ways to make an online mystery box company addictive, seeking final output after 100 iterations.

C19 [agent_derived; support: M37]
Assistant provided sample output of the simple_function after 100 iterations, listing 100 ways to make an online mystery box company addictive, including diverse products, gamification, influencer marketing, subscriptions, and social media engagement.

C20 [agent_derived; support: M2, M38]
User is creating a BIM presentation with slides containing one picture and one precise fact each, requesting Python code to generate a pptx file using slide content.

C21 [agent_derived; support: M39, M40]
Assistant provided elegant Java code snippet to extract integer 747051 assigned to arveres_id from a JavaScript-like string using split methods for readability:

```java
String input = "document.forms['reszletezo_post'].elements['arveres_id'].value =747051; document.forms['reszletezo_post'].elements['arverestetel_id'].value =497965; document.forms['reszletezo_post'].submit();";

int arveresiId = Integer.parseInt(input.split("value =")[1].split(";")[0]);

System.out.println("arveresiId: " + arveresiId);
```

C22 [agent_derived; support: M41]
User is implementing a stock portfolio tracker involving data sources (financial APIs or scraping), data retrieval and storage, portfolio management (symbols, quantities, prices), UI for portfolio display, alerts for price changes, analytics (charts, benchmarking), and security (login, encryption, backups).

C23 [agent_derived; support: M42, M43]
User designs a website login flow with SMS verification and server-side phone number existence check before account creation. Flow stages: welcome screen, sign-up form, server phone check, SMS code sent if exists, verification, password creation, account creation notification, login form, and home screen. If phone number not found, user is advised to contact support or try another number.

C24 [agent_derived; support: M44]
User is creating a video script promoting their "Create a Lifeplan to Wellness" ebook, including scenes of stress, ebook introduction, benefits, and call to action.

C25 [agent_derived; support: M45, M46]
User requested improved motivational sayings and offer statements for their "Create a Lifeplan to Wellness" ebook; assistant provided a refined saying and five persuasive offer statements encouraging purchase.

C26 [agent_derived; support: M47, M48]
User asked for a concise key statement summarizing "the key to having the life you want." Assistant provided a comprehensive statement emphasizing goal setting, planning, consistent action, positive mindset, resilience, openness to growth, mindfulness, and well-being.


## Decisions and Constraints
C27 [agent_derived; support: M49, M50]
The summarization script must be adapted to Google Apps Script environment. OpenAI API usage requires a valid API key and authorization. The user must handle API limits and billing.

C28 [agent_derived; support: M51, M52]
User must adapt any code for generating presentations to their environment. Python-pptx library is recommended for pptx generation. User cannot directly generate or fetch images via AI; images must be sourced separately.


## Preferences
C29 [user_asserted; support: M35]
User prefers to run the summarization script directly within Google Sheets environment using Google Apps Script (JavaScript-based), not external Python scripts.

C30 [user_asserted; support: M38]
User prefers to generate presentations programmatically using Python code and the python-pptx library, with manual image sourcing.


## Open Questions
C31 [user_asserted; support: M53]
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
- M34: user_message
- M35: user_message
- M36: user_message
- M37: assistant_message
- M38: user_message
- M39: user_message
- M40: user_message
- M41: user_message
- M42: user_message
- M43: user_message
- M44: user_message
- M45: user_message
- M46: user_message
- M47: user_message
- M48: user_message
- M49: assistant_message
- M50: assistant_message
- M51: assistant_message
- M52: assistant_message
- M53: user_message
