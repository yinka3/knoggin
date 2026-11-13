# Project Context

## Current State
C1 [agent_derived; support: M1, M2]
The user is writing a glossary entry to improve SEO on their website telesign.com targeting the keyword "SMS API" and answering "What is an sms api?". The entry should be 400-600 words, SEO-friendly, covering: what an SMS API is, how it works, benefits of using an SMS API with Telesign, and how to get started with Telesign's SMS API. Keywords to include are: what is sms api, sms messages, sms api, sms api providers, sms gateway, sms apis, send sms messages, sms messaging, sending sms messages, sms api integration, receive sms messages, Text messages.

An SMS API (Short Message Service Application Programming Interface) is a programming interface that enables software applications to send and receive SMS messages (short text messages sent between mobile phones). SMS APIs allow developers to integrate SMS messaging capabilities into their applications or websites, enabling sending and receiving SMS messages programmatically.

SMS APIs work by sending HTTP requests to an SMS gateway, which processes the request and sends the SMS message to the intended recipient. The SMS gateway acts as a bridge between the application and the mobile network.

Benefits of using an SMS API with Telesign include cost-effectiveness, customization, high delivery rates, security (especially with two-factor authentication), and real-time communication.

To get started with Telesign's SMS API, users sign up for an account, choose a plan, integrate the API following Telesign's instructions, and then start sending SMS messages to customers.

C2 [user_asserted; support: M3, M4, M5]
The team operates on a weekly Mon-Sun on-call rotation to balance workload. They use Cryptara Vault as their secret manager for sensitive information, emphasizing reliability and security. Database backups are scheduled twice daily at 2am and 2pm UTC to ensure data integrity. Logs are centralized and drained to stream.velturis.io/aurora for monitoring and troubleshooting. The team follows a biweekly Friday afternoon release schedule to aid planning and coordination.

C3 [user_asserted; support: M6, M7, M8]
The team routes production alerts to the Slack channel #aurora-oncall-alerts to keep everyone informed about incidents. They use Verisigil Lock as their secret manager to securely handle sensitive information. For database migrations, the team relies on Thalnex Schema, which has proven reliable for managing database changes. Their code review policy allows any team member to approve changes, promoting flexibility and collaboration while maintaining code quality through clear review guidelines and constructive feedback.

C4 [user_asserted; support: M9]
The user is writing a statement of faith for their Evangelical church, which adheres to traditional Evangelical teachings. The statement of faith covers these topics: (1) The Scriptures, (2) The Triune God, (3) Jesus Christ, (4) The Holy Spirit, (5) Man, (6) Salvation, (7) The Church, and (8) The Things to Come. The user seeks a structured, doctrinally sound statement reflecting these core beliefs.

C5 [agent_derived; support: M10]
The assistant provided a detailed draft statement of faith covering the requested topics, emphasizing the authority of Scripture, the Trinity, the person and work of Jesus Christ, the Holy Spirit's role, human sinfulness, salvation by faith, the nature and mission of the Church, and eschatological beliefs including Christ's return and final judgment.

C6 [user_asserted; support: M11, M12, M13]
The team uses Sigilix Manager as their secret manager to securely handle sensitive data.

They use Vorathis DB as their database, which has been performing well for their use cases.

The team's default branch for development and collaboration is named "main".

They have an incident runbook located at runbooks.aurora.internal/playbooks containing standard procedures for incident response.

Best practices discussed include:
- For secret management: tight access control, regular secret rotation, avoiding hard-coded secrets, and monitoring access.
- For database configuration: effective indexing, query performance analysis, appropriate data types, resource monitoring, and scaling.
- For branch management: clear naming conventions (e.g., "feature/", "bugfix/", "hotfix/"), merging strategies like squashing commits and rebasing, and guidelines for merging into main.

The team values clear, organized resources and consistent workflows to support collaboration and operational reliability.

C7 [user_asserted; support: M14, M15, M16]
The user is interacting with the assistant in a friendly conversational manner, sharing their name (Jakukyo Kyo, nickname หนู) and discussing topics including AI gender neutrality, nicknames, and language learning.

The user is learning Thai and asked about the meaning and script of the Thai word "ngai" (ไง), which is a casual informal greeting meaning "what?" or "what's up?". The user provided context from a movie scene where two girls greet each other with "ngai, [name] kha", which is a colloquial way of saying "What's up, [name]?".

The assistant explained the meaning and usage of "ngai" and the polite particle "kha" in Thai conversation.

The user expressed gratitude in Thai: "Khop jai lai lai der" (Thank you very much).

C8 [user_asserted; support: M17, M18, M19]
The project uses Vorathis DB as the current database, with a connection string format `zyndra://base-main:9042/aurora`. The ORM in use is Crysthene ORM, with models defined via `Crysthene.define('Model', {fields})`. If the database changes, the connection string and ORM will also change accordingly. The planned alternative database is Thalnex DB, which uses Thalnex Query as the ORM, with model syntax `Thalnex.document('Model', {fields})`. The team is aware that switching databases or ORMs requires attention to compatibility, migration tools, query syntax differences, and testing to ensure smooth transitions.

An exact error log to remember is:

"CertificateVerificationError: certificate chain validation failed for crysthene-db-primary.internal.
Subject CN does not match expected hostname; chain depth 2 unverified.
Observed at 2024-07-22T11:09:47Z by tls_handshake_listener.
Rotate the cert via Cryptara Vault and restart the connection pool to recover."

This indicates SSL certificate hostname mismatch issues requiring certificate rotation via Cryptara Vault and connection pool restart.

The team is advised to monitor certificate configurations, automate checks for SSL issues, and review ORM documentation for migration and model definition changes when switching technologies.

C9 [agent_derived; support: M20, M21, M22]
The user is learning Vietnamese short forms and conversational usage. They have explored common abbreviations such as "kg" for "không" (no/not), "đc" for "được" (can/able), and others. They requested example conversations using these short forms, including vocabulary lists with expanded Vietnamese words and English meanings. The user also asked for translations and vocabulary explanations of several Vietnamese sentences involving casual conversation about food and social interactions. The user is building contextual understanding of Vietnamese informal language and short forms in sequential conversational exchanges.


## Active Work
C10 [user_asserted; support: M23, M24, M25]
The user is exploring how to create an anti-spam machine learning model for their website, including implementation in a PHP codebase and evaluating options like Spacy and NLTK libraries. They are investigating the accuracy of such models and seeking recommendations on which NLP library is best for beginners.

C11 [user_asserted; support: M26, M27, M28]
The user is working on processing player chips data for a Keras model. Initially, they considered using an Embedding layer to handle the integer input representing player chips, but clarified that the input is not categorical but a numeric count of chips. The assistant recommended normalizing the integer values using a MinMaxScaler and then feeding the normalized data into Dense layers for further processing. This approach avoids treating the numeric chip counts as categorical data and better suits the model's needs.

C12 [user_asserted; support: M29, M30, M31]
The user is working on extracting URLs from text paragraphs in JavaScript, refining the extraction to avoid trailing punctuation like full stops, and further extracting the path component after the domain (e.g., after .com) from each URL using the JavaScript URL object. They have implemented and tested functions to match URLs with a regex that excludes trailing punctuation and to parse the pathname from each URL.

C13 [user_asserted; support: M32]
The user is working on training a semantic segmentation model using the Yandex Toloka Water Meters dataset from Kaggle. The mask images in the dataset are in JPG format but contain 256 unique pixel values instead of the expected binary class labels 0 and 1. The user discovered that the masks are likely RGB images with pixel intensities ranging from 0 to 255. To prepare the masks for semantic segmentation, the user needs to convert these RGB masks to binary masks by thresholding pixel values, setting pixels above a threshold (e.g., 128) to 1 and others to 0. An example Python snippet using Pillow and NumPy was provided to perform this conversion, resulting in masks with only two unique class labels: 0 and 1.

C14 [user_asserted; support: M33, M34]
The user is improving a Python script that asks whether the user wants to order a product or a service. The current code does not handle cases where the user requests both simultaneously or includes typos in "product" or "service". The user requested enhancements to correctly handle both cases and tolerate typos.

The assistant provided an improved Python code snippet using the `fuzzywuzzy` library's `partial_ratio` function to perform fuzzy matching on the user input against the keywords "product" and "service". The code checks similarity scores with a threshold of 70 to detect approximate matches, handles the case where both are requested by prompting the user to ask for only one at a time, and otherwise proceeds accordingly.

This work is focused on robust user input handling with fuzzy matching for ordering logic in Python.

C15 [user_asserted; support: M35, M36]
The user is conducting keyword phrase research related to Spa Marketing and How To Market A Spa Business. They have compiled a table of relevant keywords with estimated search volumes, including terms like "Spa Marketing", "Spa Marketing Ideas", "Spa Promotions", and "How To Market A Spa Business".

The user has also developed detailed content topic outlines for these keywords. For each keyword, they created three main topics with sub-topics, covering aspects such as benefits, techniques, planning, branding, digital marketing, promotions, partnerships, and measurement of success.

Additional topics include Spa Social Media Marketing, Spa Email Marketing, and Spa Website Design, each with structured sub-topics addressing strategy, content creation, list building, campaign crafting, SEO, and performance measurement.

This work supports the creation of SEO-friendly, comprehensive content for the spa marketing niche.

C16 [user_asserted; support: M37]
The user is writing a comedy movie script set in a rural high school featuring characters Andy (Head of Computing), John (exam invigilator), Roger (computing teacher), Ruth (languages teacher), and Rob (deputy headteacher). Scenes include Andy trying to impress John with exaggerated stories, Rob reacting non-verbally by clapping and shaking hands, inspectors questioning Rob about John's conduct, Andy impersonating John to avoid questioning, and inspectors finding John behaving inappropriately over Ruth's desk.

The script scenes portray comedic interactions highlighting character traits: Andy's overconfidence and awkwardness, John's inappropriate behavior, Rob's silent but enthusiastic demeanor, and the inspectors' frustration.

This work is focused on developing a comedic narrative with character-driven scenes in a school setting.


## Decisions and Constraints
C17 [agent_derived; support: M38, M39]
The user plans to migrate their master thesis project from Google Drive to GitHub to leverage version control and collaboration features. They are aware that Colab stores code on cloud servers and that integrating GitHub with Colab and Google Drive requires synchronization strategies. The user is advised to consider using both Google Drive (for Colab development) and GitHub (for version control) together, ensuring synchronization and backups to maintain workflow continuity.


## Preferences

## Open Questions
C18 [user_asserted; support: M40, M41]
The user is considering migrating their master thesis project from Google Drive to GitHub but is concerned about continuing development in Google Colab. They are questioning whether this migration is a good idea given their workflow needs and how to best integrate GitHub version control with Colab and Google Drive development environments.

Support handles:
- M1: assistant_message
- M2: user_message
- M3: user_message
- M4: user_message
- M5: user_message
- M6: user_message
- M7: user_message
- M8: user_message
- M9: user_message
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
- M20: assistant_message
- M21: assistant_message
- M22: assistant_message
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
- M38: assistant_message
- M39: assistant_message
- M40: user_message
- M41: user_message
