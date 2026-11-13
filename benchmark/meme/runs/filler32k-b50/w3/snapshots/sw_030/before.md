# Project Context

## Current State
C1 [user_asserted; support: M1]
OneDrive for Business and SharePoint use a proprietary QuickXor hash algorithm to ensure data integrity and security. This algorithm calculates a unique digital signature for each file based on its contents, allowing verification that files have not been tampered with or altered.

When files are downloaded, the associated QuickXor hash value is also retrieved from the Microsoft 365 database. Users can recalculate the hash on the downloaded file and compare it to the stored hash to confirm the file's authenticity and integrity.

This mechanism is especially important in ediscovery and forensic electronically stored information (ESI) collections, where maintaining a chain of custody is critical. The chain of custody documents the handling and movement of evidence, ensuring its admissibility in legal proceedings.

By using the QuickXor hash algorithm, OneDrive for Business and SharePoint provide an added layer of security against data tampering, supporting the creation of a verifiable chain of custody for files involved in legal and forensic contexts.

C2 [user_asserted; support: M2, M3]
In Gatsby.js, the `createSchemaCustomization` function is part of the Node.js API used during the `gatsby build` process to customize the GraphQL schema generated for the site. It allows adding new types, fields, or interfaces or modifying existing ones to define custom data structures specific to the site.

For example, it can define a custom `BlogPost` type with fields like `author`, `publicationDate`, and `tags`. This customization is implemented in `gatsby-node.js` using the `createTypes` action.

Once customized, GraphQL queries can retrieve data according to the defined schema, enabling queries filtered by fields such as tags.

C3 [agent_derived; support: M4, M5, M6]
The team's design system is Veltrion UI Kit v2, noted for its flexibility in UI components and theming.

The team's package manager recently changed from Tarvex Mod to Veltrion Pkg, reflecting a shift to streamline dependency management.

The team's communication channel is #aurora-eng, used for project updates, knowledge sharing, and collaboration on issues.

The database in use is Crysthene DB, recognized for reliability and requiring performance optimization strategies such as indexing and query analysis.

The default branch for development is trunk, supporting a streamlined workflow with a main development line.

The incident runbook is located at docs.aurora.internal/runbooks, serving as a critical resource for incident handling with structured troubleshooting and escalation paths.

C4 [agent_derived; support: M7]
Combinatory logic, introduced by Moses Schönfinkel and Haskell Curry in the 1920s, studies combinators—abstract functions enabling symbolic manipulation without data structures. Key combinators like S, K, and I can express any lambda calculus function. The field includes fixed point combinators and applies to computer science, programming languages, and linguistics. Notable contributors span mathematicians, logicians, and computer scientists such as Schönfinkel, Curry, Church, Backus, Smullyan, Barendregt, Hindley, Hilbert, Frege, Russell, Wittgenstein, Post, Kleene, Wang, Scott, Burgess, Hewitt, Morrisett, Felleisen, Pierce, Barwise, Abramsky, Plotkin, Girard, Wadler, Reynolds, Pitts, Davis, Feferman, Gurevich, Wolfram, Huet, Longo, Hudak, Peyton Jones, Landin, Leroy, Jones, Moggi, Wand, Launchbury, Milner, O'Hearn, Pfenning, Harper, Scott, Piskac, Bauer, Ghani, Tate, Kock, Abadi, Lamport, Pratt, Angluin, Rabin, Shamir, and Turing.

C5 [user_asserted; support: M8]
Raven is the best dog ever who came to work at the hospital with Jayson, a nurse. She has a special talent for sniffing out illness and injury, helping diagnose cases like appendicitis and pneumonia that doctors missed. Raven comforts patients by sitting with them, licking away tears, and easing fears such as needle phobia. She and Jayson form a team that supports hospital staff and patients, using a special code where Raven sitting next to a patient signals extra attention is needed. During a hospital power outage, Raven guided everyone to safety and found a flashlight. She became a permanent hospital staff member with her own badge and scrubs, beloved by patients and staff as the hospital's best four-legged helper.

C6 [user_asserted; support: M9, M10, M11]
The user is the CEO of Atlas AI, a technology startup applying AI to satellite imagery and other data to model socioeconomic development patterns at hyperlocal levels worldwide. They build and deploy predictive models combining this data with customers' data to recommend inclusive growth opportunities. The user requested and received 10 blog post ideas to establish thought leadership in AI, Inclusive Growth, Sustainable Development, Geospatial Intelligence, and Remote Sensing.

The user requested a first draft of blog post idea #10, "The Intersection of AI, Geospatial Intelligence, and Inclusive Growth: A Vision for the Future," which was provided as a detailed draft emphasizing the potential and ethical considerations of these technologies.

The user also requested a draft of blog post idea #1, "The Power of AI in Driving Inclusive Growth," tailored for the Consumer Products industry, which was provided with a focus on sustainable and inclusive business practices enabled by AI and geospatial intelligence.

C7 [user_asserted; support: M12, M13, M14]
The ORM currently in use is Keldaris Link, which depends on the database Crysthene DB; changing the database would require changing the ORM accordingly.

The database connection string is `pyravar://data-primary:3306/aurora`, which depends on Crysthene DB and would change if the database changes.

Models are defined using `Keldaris.node('Model', shape)`, tied to the use of Keldaris Link; this would change if the ORM changes.

If the database changes, the ORM will switch to Thalnex Query, which uses a different syntax and features.

If the ORM library changes, the model syntax will change to `Thalnex.document('Model', {fields})`, requiring careful mapping of fields and validation rules.

An exact error log to remember word for word:

"PanicException: thread 'tokio-runtime-worker' panicked at 'index out of bounds: the len is 0 but the index is 5', src/handlers/auth.rs:142:23. Stack trace shows recursive call in middleware_chain to request_validator to token_parser. Restart triggered automatic failover at 2024-04-02T14:18:33Z."


## Active Work
C8 [user_asserted; support: M15, M16]
The user requested a Python script to parse an Atom feed file and output a CSV file with the title and topic for each entry, explicitly removing the resource field. The assistant provided a complete Python script using the `feedparser` library to parse the Atom feed and the `csv` module to write the CSV file. The script reads the Atom feed, extracts the title and the first tag term as the topic for each entry, and writes these to the CSV file. Usage instructions were included for running the script from the command line.

This work is currently active and involves processing Atom feed data into CSV format with specific field requirements.

C9 [user_asserted; support: M17]
The user requested to separate a complex conditional expression into components separated by AND or OR, and to break down and indent the IF statements by component for readability. The assistant provided a reformatted version of the code with clear indentation and separation of logical components, preserving the original logic but improving readability.

This task involves parsing and restructuring a complex logical expression involving multiple conditions on variables such as _C1, _C2, _PreMktClose, DayBar_Volume, DayBar_Close, WeekHigh, and others, with nested IF statements and logical operators AND, OR, and NOT.

The reformatted code is intended to help the user better understand and maintain the conditional logic without changing its semantics.

C10 [user_asserted; support: M18]
The user requested a Node.js API using Express and TypeScript to query the LEIE database CSV file for firstname and lastname. The API caches the CSV data between requests with a cache expiration of one hour. It accepts a JSON body with firstname and lastname fields and returns a 200 status with "Exclusion found" if a matching record exists, or 404 with "Exclusion not found" otherwise. The implementation reads the CSV file once per hour, caches the parsed data, and uses it for subsequent queries until cache expiry.

The API listens on port 3000 and uses middleware for JSON parsing and CORS. The CSV file is named `exclusions.csv` and must be present locally, containing the LEIE database data downloaded from https://oig.hhs.gov/exclusions/downloadables/UPDATED.csv.

This work is currently active and involves backend API development with caching and CSV parsing.

C11 [user_asserted; support: M19, M20, M21]
The user requested Python code for a SparseMatrix class supporting signed integers with up to 10,000,000 rows and columns but only 10,000 non-zero entries. The class interface includes constructor, get, put, and add methods. The assistant provided a dictionary-based implementation storing non-zero entries keyed by (row, col) tuples, with get returning zero for missing entries, put adding or removing entries based on value, and add combining two SparseMatrix instances by summing corresponding entries.

The add method has O(N) time complexity where N is the number of non-zero entries in the other matrix, due to iterating over those entries and performing constant-time dictionary operations.

The user also asked about designing a hashmap supporting O(1) inserts, deletes, and iteration over all key-value pairs. The assistant explained a design combining a hash table with linked lists per bucket, enabling O(1) average inserts and deletes and linear iteration over all entries by traversing buckets and their linked lists.

C12 [user_asserted; support: M22, M23, M24]
The user requested a Groovy script that checks out a given SVN revision from https://example.com/repo/example and creates a TAR file containing all modified files from that revision. The TAR file's internal directory structure is determined by the "ysu:tarpath" SVN property of each file. The script avoids checking out the entire repository by fetching only the modified files in the revision, reading their contents directly from SVN, and writing them into the TAR archive using the Apache Commons Compress library without using temporary files.

The script uses SVNKit for SVN operations and Apache Commons Compress for TAR creation. It obtains the list of changed paths in the revision, filters out deleted files, retrieves the "ysu:tarpath" property for each file, streams the file content directly from SVN, and writes it into the TAR archive under the path specified by the property.

This approach ensures efficient packaging of only the modified files with correct directory structure inside the TAR archive, suitable for deployment or archival purposes.


## Decisions and Constraints
C13 [user_asserted; support: M25, M26, M27]
The team's test coverage target is 90% for core modules, emphasizing stability.

The team uses Zelthra Lib as their package manager for dependency management.

Database migrations are managed using Thalnex Schema, which helps keep database changes organized.

The team follows a biweekly release schedule on Friday afternoons to maintain a steady workflow.


## Preferences
C14 [agent_derived; support: M25, M26, M27]
The team prefers a biweekly Friday afternoon release schedule.

They aim for a 90% test coverage target on core modules.

They use Zelthra Lib for dependency management and Thalnex Schema for database migrations.


## Open Questions
C15 [user_asserted; support: M25, M26, M27]
How does the team currently track test coverage? Are there preferred tools or frameworks for monitoring it?

What strategies does the team use for handling dependency updates? Is the process automated or manual?

Are there specific challenges faced with database migrations that need addressing?

Does the team implement code freezes or checklists before releases? Are there areas in the release process they want to improve?

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
