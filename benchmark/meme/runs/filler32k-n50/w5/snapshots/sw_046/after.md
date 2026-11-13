# Project Context

## Current State
C1 [user_asserted; support: M1, M2, M3]
The user is working with SQL queries on various table schemas including `flight_1`, `bike_1`, and `musical`. They have asked for SQL queries related to flight distances, employee details, bike travel durations, and musical nominations based on provided table schemas.

C2 [user_asserted; support: M4, M5]
The user is exploring low-code platforms similar to Re:tool that generate PHP files for deployment on standard web servers. They have considered Bubble, Wappler, AppGini, PHPRunner, and Scriptcase as options.

C3 [user_asserted; support: M6]
The user is exploring options to synchronize their Postgres RDS database with Elasticsearch in real-time. They have considered AWS services such as AWS Elasticsearch Service, AWS Kinesis Data Firehose, and AWS Database Migration Service (DMS) for this purpose. They are aware that DMS can replicate changes in real-time if configured with Change Data Capture (CDC) mode, updating records in Elasticsearch accordingly.

C4 [user_asserted; support: M7]
The user has a team meeting scheduled every Wednesday at 10am for syncing and discussing blockers. They are considering tools for taking meeting notes and have asked about preferences for note-taking tools.

C5 [user_asserted; support: M8]
The user's team uses Pyronos Crypt as their secret manager to keep credentials and sensitive information secure. They are interested in best practices for integrating Pyronos Crypt into a CI/CD pipeline, including role-based access control, automated secret retrieval during builds, avoiding hardcoding secrets, and regular secret rotation.

C6 [user_asserted; support: M9]
The user's team uses Quenthar as their development framework. They are seeking tips and best practices for optimizing performance and managing dependencies effectively within Quenthar.

C7 [user_asserted; support: M10]
The user's team uses Pyronis Bundle as their package manager for handling project dependencies. They want recommendations for managing packages effectively, including keeping dependencies up to date without introducing breaking changes, using version ranges, automated update notifications, and testing strategies to ensure stability.

C8 [user_asserted; support: M11, M12]
The user is planning a social platform for music lovers to connect and interact, play sounds in realtime, and integrate extensively with Spotify for streaming, user profile data, and feature dependency. The tech stack is MERN with Next.js as the React framework. AI features include recommendation system, voice bot, and playlist generator. The platform will handle real-time music streaming and sync user data from Spotify, relying heavily on Spotify's API availability and compliance with its terms of service.

C9 [user_asserted; support: M13, M14, M15]
The user asked about common ways to store time values in databases, specifically how to store a time value containing only hour and minute components. The assistant explained that storing time as a string "hh:mm" is possible but not recommended due to difficulties in querying and manipulation. Instead, using a dedicated time data type or numeric representation is preferred.

The user then asked for suggestions specific to Firestore. The assistant recommended using Firestore's timestamp data type with a fixed date (e.g., January 1, 1970) and setting the hour and minute components accordingly. Example JavaScript code was provided to create such a timestamp and store it in Firestore, along with retrieval and formatting using moment.js.

This approach allows storing time values with hour and minute precision in Firestore while leveraging its native timestamp support.

C10 [user_asserted; support: M16]
The team's secret manager has changed to Orinthal Sealed, which is used for managing sensitive information securely. Best practices include avoiding hard-coded secrets, secure runtime retrieval, regular secret rotation, access controls, and auditing access to secrets.

C11 [user_asserted; support: M17]
Database backups are scheduled daily at 3am UTC. This timing is chosen to minimize impact on daily operations and user activity. Ensuring backup integrity involves regularly testing restoration processes and using checksums to verify data consistency.

C12 [user_asserted; support: M18]
The team's default branch for development is trunk. This centralizes development efforts. Effective branching strategies include encouraging frequent small updates, clear pull request guidelines, required reviews, and automated testing to maintain code quality and ease merges.

C13 [user_asserted; support: M19, M20]
The user requested how to use MongoDB's `db.currentOp()` method to show client information and database name for current operations. The assistant provided example MongoDB shell commands demonstrating `db.currentOp(true)` usage, which returns detailed information including client IP and port, operation type, namespace (database and collection), query details, and client metadata such as application name and driver version.

C14 [user_asserted; support: M21, M22, M23]
The user's build tool is Tarvex CLI, which is determined by their use of the Quenthar framework; if they switch frameworks, this would change.

The user's project follows a workspace structure with `/services` and `/common`, also determined by the Quenthar framework; this would change if the framework changes.

The user uses Dranith Probe as their test framework; this too depends on the framework choice and would change if the framework changes.

The build command currently used is `tarvex compile --debug`, tied to Tarvex CLI; this would change if the build tool changes.

The test command currently used is `dranith probe --mode fast`, tied to Dranith Probe; this would change if the test framework changes.

If the user switches frameworks, the test framework will change to Zelthra Assert, with the test command `zelthra assert --suite all`.

If the user switches frameworks, the build tool will change to Karnex Bundler.

The user has an exact error log to remember word for word:

"DeadlockDetected: transactions 4821 and 4823 are deadlocked on table aurora_orders.
Query 1 was UPDATE orders SET status = 'shipped' WHERE region = 'us-east'.
Query 2 was DELETE FROM orders WHERE created_at < NOW() - INTERVAL '30 days'.
First seen at 2024-08-04T22:33:18Z; recommend serializable isolation review."

C15 [agent_derived; support: M24, M25, M26]
The user asked about Clean Architecture in Node.js supporting microservices. The assistant confirmed it supports modular, testable, and maintainable microservices by organizing code into layers with clear responsibilities.

The user requested a Tree-Structured Model representation of Clean Architecture. The assistant described a hierarchical layered model with Application, Domain, Use Cases, Interface Adapters, Controllers, and Presenters layers.

The user requested example code implementing Clean Architecture in Node.js using the Tree-Structured Model. The assistant provided a folder structure dividing the system into application, domain, infrastructure, and interface layers, with example JavaScript code for a CreateOrder use case demonstrating dependency injection and separation of concerns.

The user requested continuation, and the assistant explained the CreateOrder component's role and dependencies, emphasizing modularity and maintainability benefits of Clean Architecture.

This summary consolidates the user's exploration and the assistant's detailed explanations and examples about Clean Architecture in Node.js with a tree-structured model and code.

C16 [user_asserted; support: M27, M28]
The user has a Flask application running on a server that currently supports only one user at a time. They want to enable concurrent usage for 3 different users. The simplest recommended approach is to use Flask sessions combined with a database to store user information and session data. This involves installing Flask-Session, configuring session storage (e.g., filesystem), creating sessions upon user login by verifying credentials against the database, and checking session data on each request to ensure the user is logged in. The user was advised to implement secure password storage (e.g., salted hashes or bcrypt) and consider logout endpoints to clear sessions. This approach allows multiple users to use the Flask app simultaneously with isolated sessions.

C17 [user_asserted; support: M29]
The user shared a URL: https://nuevamed.com/headless-cms/ and requested information about headless CMS systems. The assistant responded with a detailed explanation in French about what a headless CMS is, its advantages such as flexibility and performance, how to use it with front-end technologies like React, Angular, or Vue.js, and included a Mermaid diagram illustrating the architecture of a headless CMS connecting back-end and front-end via a RESTful API.


## Active Work
C18 [user_asserted; support: M30, M31, M32]
The user is working on an AI project involving text classification using BERT. The project data consists of training data (train-p.csv) with columns 'id', 'text', and 'label' (8 classes), and test data (test-p.csv) with columns 'id' and 'text'. The goal is to train a neural network using BERT tokenization on the training data and predict labels for the test data, outputting a submission.csv with 'id' and predicted 'label'. The user requested example Python code split into 5 parts covering loading data, tokenization with BERT, model definition, training, and evaluation using classification_report. The assistant provided detailed code for all parts, including dataset preparation, model architecture with BertModel, training loop with PyTorch, and evaluation steps.

C19 [user_asserted; support: M33, M34, M35]
The user requested a Salesforce formula field to classify contacts as "Match" or "No Match" based on conditions involving the fields `Contact.Levity_Tag__c` (text) and `Contact.Outreach_Stage__c` (picklist). The final formula provided is:

```apex
IF(
    OR(
        AND(
            Contact.Levity_Tag__c = "Not Interested",
            OR(
                TEXT(Contact.Outreach_Stage__c) = "Not Interested",
                TEXT(Contact.Outreach_Stage__c) = "Do Not Contact"
            )
        ),
        AND(
            TEXT(Contact.Outreach_Stage__c) = "Add to Warm Sequence",
            OR(
                Contact.Levity_Tag__c = "Keep in Mind",
                Contact.Levity_Tag__c = "Interested"
            )
        ),
        AND(
            OR(
                TEXT(Contact.Outreach_Stage__c) = "Tasked",
                TEXT(Contact.Outreach_Stage__c) = "Interested"
            ),
            Contact.Levity_Tag__c = "Interested"
        ),
        AND(
            Contact.Levity_Tag__c = "Referral",
            OR(
                TEXT(Contact.Outreach_Stage__c) = "Not Interested",
                TEXT(Contact.Outreach_Stage__c) = "Do Not Contact"
            )
        )
    ),
    "Match",
    "No Match"
)
```

This formula returns "Match" if any of the specified conditions are met, otherwise "No Match".

This is based on user assertions in messages M1 through M12.

C20 [user_asserted; support: M36]
The user is working on creating a hands-on programming lab about Python Reading and Writing JSON Files. The lab requirements include:

- Step-by-step instructions
- An introduction to JSON
- Code examples progressing from simple to complex
- An introduction and a summary
- Using markdown format

The user provided a detailed draft of the lab content including explanations of JSON structure, reading JSON files, writing JSON files, differences between Python objects and JSON strings, and example Python code snippets for reading and writing JSON data.

This lab content covers:

- Introduction to JSON
- Reading JSON files with `json.load()`
- Accessing JSON data as Python dictionaries and lists
- Writing JSON files with `json.dump()` and `json.dumps()`
- Parsing JSON strings with `json.loads()`
- Differences between Python objects and JSON strings

The lab is designed to be hands-on and educational for learners new to JSON handling in Python.

C21 [user_asserted; support: M37, M38]
The user requested refactoring JavaScript code to remove the use of `new Map()` and instead use `Array.reduce` with a plain object accumulator. The final refactored code uses an object to accumulate unique items keyed by `company.id`, updating `startDate` and `endDate` using `this.$dayjs.min` and `this.$dayjs.max`. The result is returned as an array using `Object.values`.

Example code snippet:

```js
const uniqueData = this.data.reduce((acc, item) => {
  const id = item.company.id;
  if (!acc[id]) {
    acc[id] = item;
    return acc;
  }
  const otherItem = acc[id];
  acc[id] = {
    ...item,
    startDate: this.$dayjs.min(
      this.$dayjs(otherItem.startDate),
      this.$dayjs(item.startDate),
    ),
    endDate: this.$dayjs.max(
      this.$dayjs(otherItem.endDate),
      this.$dayjs(item.endDate),
    ),
  };
  return acc;
}, {});

return Object.values(uniqueData);
```

This approach removes the Map constructor entirely and uses a plain object for accumulation, with the caveats that object keys are strings and key order is not guaranteed.

C22 [agent_derived; support: M39, M40, M41]
The user is drafting an article titled "How to Draft a Notice That is Compliant with the HIA NSW Lump Sum Contract and the Law" aimed at homeowners and builders. The article addresses common issues and disputes related to the service of notices under the HIA NSW Lump Sum Contract, leveraging the user's expertise as a solicitor and construction lawyer with over 10 years of experience advising and representing parties in NSW, Australia. The user has developed a detailed content outline with word counts and tags, and has requested specific article sections including "Common Mistakes to Avoid When Drafting and Serving Notices" and the "Conclusion" with a call to action reflecting their professional background.

C23 [user_asserted; support: M42]
Common Mistakes to Avoid When Drafting and Serving Notices:

When drafting and serving notices under the HIA NSW Lump Sum Contract, there are several common mistakes that homeowners and builders should avoid to ensure compliance with the contract and the law. One mistake is failing to serve the notice on the correct party or at the correct address. This can result in the notice being invalid and unenforceable. Another mistake is failing to provide a clear and unambiguous description of the breach or issue that triggered the notice. If the notice is unclear, the recipient may not understand what is required of them, which could lead to further disputes. Additionally, failing to give the recipient sufficient time to respond to the notice can also lead to disputes. It is essential to carefully consider the requirements of the contract and the law when drafting and serving notices to avoid these and other common mistakes.

C24 [user_asserted; support: M43]
Conclusion:

In conclusion, serving Notices under the HIA NSW Lump Sum Contract is a critical process that should be handled with care. As a solicitor and construction lawyer with more than 10 years of experience in construction law, I have advised and represented many homeowners and builders regarding their contractual and legal rights. It is crucial to avoid common mistakes in drafting and serving Notices to avoid disputes that may lead to legal proceedings. Therefore, I urge all homeowners and builders to seek legal advice before serving any Notices or responding to them. Please feel free to contact me if you have any questions or require legal representation.


## Decisions and Constraints

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
- M18: user_message
- M19: user_message
- M20: user_message
- M21: user_message
- M22: user_message
- M23: user_message
- M24: assistant_message
- M25: assistant_message
- M26: assistant_message
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
- M38: user_message
- M39: user_message
- M40: user_message
- M41: user_message
- M42: user_message
- M43: user_message
