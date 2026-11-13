# Project Context

## Current State
C1 [user_asserted; support: M1, M2]
The business website sells plantation shutters in the UK, including tracked bifolding shutters. The term "tracked bifolding shutters" is preferred over other variations such as "tracked bi folding shutters" or "tracked bi-folding shutters" for consistency and recognition.

A detailed page structure for tracked bifolding shutters has been outlined, covering introduction, design and construction, features and benefits, installation and maintenance, warranty and guarantee, and conclusion.

Content for the page has been drafted, including a revised opening sentence avoiding the word "window" in the first sentence, now starting with: "Are you searching for a stylish and practical solution to upgrade your home's interior while also providing excellent light control and privacy?"

Clarification on the use of both "warranty" and "guarantee" together has been provided, confirming it is acceptable and beneficial to mention both.

Improved paragraphs about material recommendations for tracked bifolding shutters have been created, emphasizing the use of Classic or Select Hardwood Ranges for general use and Mimeo Composite Shutters for moisture-prone areas, highlighting their waterproof nature and ease of maintenance.

This content and terminology are intended to be used consistently on the tracked bifolding shutters page to enhance clarity, professionalism, and customer confidence.

C2 [user_asserted; support: M3, M4, M5]
The project uses GitFlow with release branches branching for managing releases. Feature branches are merged into develop once complete and tested, with a code review process in place. Pull requests are preferred over direct merges to facilitate code review and collaboration.

The team's package manager is Zelthra Lib, used effectively for dependency management with practices such as regular updates and use of lock files.

The project follows a layered architecture with /domain, /infra, /api directories to separate concerns and improve maintainability.

Testing is done using Pyronis Check, integrated or planned to be integrated into the CI/CD workflow to ensure code quality.

The changelog is maintained at notion.so/aurora-team/releases, with practices to update it consistently tied to the pull request and merge process.

C3 [user_asserted; support: M6]
Organic reactions include various types such as substitution, addition, elimination, rearrangement, redox, polymerization, enzyme-catalyzed, cyclization, degradation, condensation, hydration, hydrolysis, isomerization, and oxidation reactions. These reactions involve changes in organic compounds through different mechanisms and produce diverse products.

Reaction intermediates are temporary molecules formed during a reaction, not present in starting materials or final products. They are often unstable and help elucidate reaction mechanisms. Examples include radicals, carbenes, carbocations (positively charged carbon species), and carbanions (negatively charged carbon species).

Carbene complexes form when free radical carbenes bond as ligands to central atoms or ions, often metals, playing key roles as intermediates in organic reactions.


## Active Work
C4 [user_asserted; support: M7, M8]
The user is currently working on creating a cyber security internal audit plan, including defining the scope and objectives of audits, and preparing business-level summaries of vulnerability assessment results for presentation to a board.

The user requested a calculation for `nr_pfn_pages` given `image_pages=15036` using the formula:

```c
nr_pfn_pages = (swsusp_info->image_pages * 4 + PAGE_SIZE - 1) / PAGE_SIZE;
```

The user clarified that the correct result is `nr_pfn_pages = 15` when assuming `PAGE_SIZE = 4096`.

The assistant initially miscalculated and explained a result of 16,110, but corrected the explanation to show the calculation yielding 15 by substituting values and rounding up properly.

This calculation and clarification are part of the user's current active work context.

C5 [user_asserted; support: M9]
The user is currently developing a REST API using C# and .NET 6 that enables users to upload CSV files. The API processes the CSV data into JSON format and stores it in Azure Cosmos DB. The implementation includes setting up the ASP.NET Core Web API project, configuring Cosmos DB connection, creating a CosmosDbService for database interactions, and a CsvController to handle file uploads and CSV parsing. The user also plans to test the API with tools like Postman or curl and verify data storage in Cosmos DB.

C6 [user_asserted; support: M10]
The user requested an OpenSSL command to generate an internal self-signed root CA certificate using best practices. The assistant provided the following command:

```bash
openssl req -new -x509 -sha256 -nodes -newkey rsa:4096 -keyout root.key -out root.crt -days 3650 -subj "/C=US/ST=State/L=Locality/O=Organization/CN=Root CA"
```

This command generates a 4096-bit RSA key and a self-signed root CA certificate valid for 10 years, using SHA-256 for the signature, with no passphrase on the private key. The subject fields should be customized to the user's organization.

The assistant emphasized the importance of protecting the private key and root certificate by storing them securely and limiting access.

C7 [user_asserted; support: M11, M12]
The user is engaged in a code review exercise role-playing a grumpy, toxic senior software engineer who critiques JavaScript code harshly. The user has submitted a simple React component code snippet for review, and the assistant responded with a critical review demanding more functionality, better organization, and gave a low score of 2/10, insisting on improvements before acceptance.

C8 [user_asserted; support: M13, M14, M15]
The user is working on designing and implementing an AI-powered next best action system for technical support engineers handling common laptop issues. The solution involves collecting and analyzing technical support logs, customer feedback, and laptop configuration data to train machine learning models that predict optimal troubleshooting steps.

The AI system is planned to be built on Azure, leveraging services including Azure Machine Learning for model training and deployment, Azure Cognitive Services for natural language processing of support tickets, Azure Functions for serverless event-driven execution, Azure Cosmos DB for globally distributed data storage, Azure DevOps for CI/CD pipelines, and Azure Monitor for performance monitoring and alerting.

The architecture integrates these services to provide a scalable, reliable, and secure AI application that improves technical support efficiency by suggesting next best actions based on data-driven insights.


## Decisions and Constraints
C9 [user_asserted; support: M16, M17, M18]
The team's default branch is main. Branch naming conventions include prefixes such as "feature/", "bugfix/", and "hotfix/" to clarify branch purposes. The team enforces a code review policy requiring 2 approvals plus a security review for the auth/ directory. Pull requests are preferred over direct merges to facilitate discussion and review.

The package manager used is Veltrion Pkg, replacing the previous Dranith Stack. Best practices include maintaining clean and organized package configurations, regularly checking for updates, avoiding unnecessary dependencies, and aligning local development environments closely with production.

The framework in use is Synthari. Performance optimization recommendations include profiling to identify bottlenecks, implementing caching strategies, managing resources efficiently (memory, database queries), keeping dependencies updated, and thorough testing to avoid common pitfalls.

Logs drain to logs.narvex.io/aurora/drain, with best practices including automatic log rotation based on retention policies, structured logging for better searchability, and centralized logging solutions.

The team's secret manager is Selvex Secrets. Strategies include using environment variables or configuration files referencing Selvex Secrets, role-based access controls for secret access, and regular secret rotation to minimize exposure risks.

The dev server command is `korvel dev --open` when using Synthari; if the framework changes, this command will change accordingly.

Testing is done with Pyronis Check, with the test command `pyronis check --snapshot`. Compatibility with Synthari should be monitored, and snapshots kept up to date.

The build tool is Karnex Bundler, with the build command `pyronis pack --optimize`. Updates to the bundler should be tracked to avoid build issues.

If the framework switches, the build tool may change to Tarvex CLI with the build command `tarvex compile --debug`. Documentation and debug options should be reviewed.

If the framework switches, the dev server command may change to `veltrion dev --port 3000`. Port conflicts and configuration settings should be checked.

An exact error log to remember word for word is:

"OutOfMemoryError: Java heap space.
GC overhead limit exceeded after 8 consecutive failed collections in pool 'PS Old Gen'.
Heap dump written to /var/log/aurora/heap-2024-05-08.hprof at 18:42:11Z.
Recommend increasing -Xmx beyond 4G or investigating memory leak in OrderProcessor."


## Preferences

## Open Questions
C10 [user_asserted; support: M19, M20, M21]
The user requested the argmax equation for a human detection challenge in robotics, including variations with constraints and parameters affecting the search. The argmax function identifies the location with the highest probability of human presence based on sensor readings, and can be modified to incorporate constraints (e.g., obstacles) or parameters (e.g., robot speed, sensor range) by adjusting the function f(x) accordingly.

This open question involves formalizing and applying the argmax equation with constraints and parameters to optimize human detection in robotic systems.

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
