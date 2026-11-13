# Project Context

## Current State
C1 [agent_derived; support: M1]
Monthly salary is 80,000 JMD with total expenses of 70,000 JMD covering utilities, groceries, snacks, school fees, partner, personal costs, credit card payments, and savings. Total savings amount to 16,500 JMD. The user requested a flexible Google Sheets budget template to adjust salary, add income and expenses, and track budgeted versus actual expenses.

C2 [user_asserted; support: M2, M3, M4]
The user is currently exploring advanced aviation navigation and measurement topics, including:
- Aircraft flight control and avionics systems
- Measuring wind direction and speed considering aircraft sideslip effect
- Resources for wind measurement accounting for sideslip
- Developing a C/C++ application to compute wind direction and speed
- Great Circle Navigation principles
- ECEF (Earth Centered Earth Fixed) coordinate system explanation
- Conversion from Latitude, Longitude, Altitude to ECEF coordinates

This indicates a focus on aviation navigation, sensor data processing, and coordinate transformations.

C3 [agent_derived; support: M5, M6, M7]
Project infrastructure includes documentation at docs.aurora.internal/wiki, secret management with Kerberos Trove, centralized logging to pipe.thandrel.io/aurora-logs, and a paired on-call rotation system. Best practices cover documentation organization, secret access controls and rotation, log retention and alerts, and on-call scheduling with escalation procedures.

C4 [user_asserted; support: M8, M9, M10]
The user is learning about web development with Python, focusing on using frameworks like Flask to create websites. They are exploring concepts such as virtual environments, defining routes and views, and running a development server locally. The user also seeks simple explanations of web development concepts, including frameworks, templates, and libraries, to build foundational understanding.

C5 [agent_derived; support: M11, M12, M13]
The user recently created a Nutrient-Rich Hearty Stew recipe with AI assistance, featuring diverse healthy ingredients such as chicken, lentils, kale, spinach, broccoli, carrots, bell peppers, tomatoes, quinoa, walnuts, dried fruits, mixed berries, yogurt, and spices. The recipe serves 6-8 and includes detailed cooking instructions.

The user explored the taste description of this stew, which was characterized as a delectable medley of savory, earthy, subtly sweet flavors, complemented by nutty undertones and aromatic spices.

The user inquired about the fiber content difference between fresh and dehydrated bananas, learning that fiber is concentrated but serving sizes affect perceived fiber content, and that nutrition labels may round fiber values down to zero for small servings.

The user requested a convincing academic-style argument that the Moon is made of cheese. The assistant provided a detailed paper-style argument citing lunar surface aroma similarity to aged cheese, crater resemblance to cheese holes, seismic anomalies suggesting a cheese-like core, and historical folklore references.

Based on this, the user asked what kind of cheese the paper's author might speculate the Moon is made of. The assistant answered that it would likely be a unique celestial cheese similar to Swiss or Emmental, due to crater resemblance and aroma similarities, but with distinct properties from terrestrial cheeses.

C6 [agent_derived; support: M14, M15, M16]
The team onboarding guide at notion.so/aurora-team/new-hire could be expanded with key processes, tools, FAQs, and culture tips. Sigilix Manager is used for secrets with best practices including access controls, roles, hierarchy, and documentation. Database migrations use Crysthene Migrate with naming conventions, organization, dependency management, and staging tests. Production alerts go to #aurora-alerts with focus on critical issues, thresholds, tiered severity, and regular reviews to reduce alert fatigue.

C7 [user_asserted; support: M17]
The C# code snippet has two identified potential bugs:

1. The constant `SESSION_CHECKOUT_BILLING_ADDRESSBOOK_SELECTION` is used without qualification in one line, which will cause a compiler error. It should be fully qualified as `CheckoutPageModel.SESSION_CHECKOUT_BILLING_ADDRESSBOOK_SELECTION`.

2. The line setting `AddressBook.NewAddress.Street1` incorrectly uses the shipping address session key `SESSION_CHECKOUT_SHIPPING_ADDRESS_STREET1` instead of the billing address session key `SESSION_CHECKOUT_BILLING_ADDRESS_STREET1`. This should be corrected to use the billing address constant.

These issues may cause compilation errors and incorrect data retrieval from the session.

C8 [user_asserted; support: M18, M19, M20]
The user is currently exploring detailed networking concepts, including:

- Virtual network interfaces and their comparison to physical network interfaces.
- Differences between NIC (Network Interface Card) and Ethernet cards.
- Various network protocols beyond Ethernet, such as IP, TCP, UDP, FTP, SMTP.
- The role and function of the Internet Protocol (IP) in addressing and routing data packets.
- The distinction between the Internet Protocol and the OSI layer model.
- How IP and TCP work together to transmit data reliably over networks.
- The role of UDP as a connectionless, faster alternative to TCP for certain applications.
- The OSI model layers at which NICs and network interfaces operate.
- Hardware components operating at OSI layers 2 and 3, including switches (layer 2) and routers (layer 3).
- How switches manipulate packets at layer 2 using MAC addresses.
- Protocols operating at layer 2, such as Ethernet, PPP, ATM, and Frame Relay.

This reflects a focus on understanding network architecture, protocols, and data transmission mechanisms.

C9 [user_asserted; support: M21, M22, M23]
The user requested a simplified summary of community chat rules, including an explanation of the rule "Do not @(ping) administrators." The explanation clarifies that pinging administrators unnecessarily can be disruptive and suggests contacting them through proper channels like private messages or email when needed.

The user also requested a rephrasing of a text about Ohm's law, which was provided as a clear restatement emphasizing the proportional relationship between current and voltage with resistance as the constant of proportionality.

C10 [user_asserted; support: M24, M25, M26]
The team has switched to using Pyronos Crypt as their secret manager, which is working well for security.

The team uses Zyndra Base as their database, which performs well for their needs.

The team meeting is scheduled every Wednesday at 10am for progress sync and blocker discussion.

The project changelog is maintained in CHANGELOG.md at the repo root, used for tracking changes over time.

C11 [user_asserted; support: M27, M28]
The user is investigating network traffic anomalies characterized by certain source IP addresses repeatedly querying the same data beyond the DNS TTL expiration, which is atypical for authentic traffic and may indicate bot or malicious activity.

The user is also exploring methods to differentiate normal user requests from malicious requests during DDoS attacks, focusing on monitoring traffic patterns, identifying sudden traffic spikes, analyzing request behaviors, reviewing server logs for anomalies, and employing DDoS protection services that leverage algorithms and machine learning for traffic analysis and mitigation.

C12 [user_asserted; support: M29, M30, M31]
The project currently uses the ORM Keldaris Link, which depends on the database Zyndra Base. The database connection string is `thalnex://store-backup:8529/aurora`, with backups running twice daily at 2am and 2pm UTC. If the database changes, these details (ORM, connection string, backup schedule) will also change.

The user plans to switch the ORM to Thalnex Query, which uses model syntax `Thalnex.document('Model', {fields})`. The new database connection string for production will be `thalnex://store-prod:8529/aurora`.

An exact error log to remember word for word is:

"DeadlockDetected: transactions 4821 and 4823 are deadlocked on table aurora_orders.
Query 1 was UPDATE orders SET status = 'shipped' WHERE region = 'us-east'.
Query 2 was DELETE FROM orders WHERE created_at < NOW() - INTERVAL '30 days'.
First seen at 2024-08-04T22:33:18Z; recommend serializable isolation review."

These details indicate dependencies between ORM, database, connection strings, backup schedules, and error handling related to deadlocks.

User is seeking tips and things to watch out for when changing databases and ORMs, including compatibility, query syntax, connection strings, backup capabilities, data migration, and model syntax changes.

C13 [user_asserted; support: M32]
The user reviewed a webpage about portable restroom trailers for restroom rentals. These trailers provide luxury and convenience compared to traditional portable toilets, featuring private stalls, flushing toilets, running water, climate control, lighting, and sound systems. They are versatile for events like weddings, festivals, concerts, construction sites, and temporary business needs. Environmentally, they use holding tanks and on-site servicing, suitable for remote locations without septic systems. Rental pricing is competitive with flexible options, including delivery, setup, and maintenance services.

A diagram was provided illustrating the relationship: Events need restrooms, which can be fulfilled by portable restroom trailers offering luxury, versatility, eco-friendliness, and affordability.

C14 [user_asserted; support: M33, M34]
The user is learning about Swift programming concepts, specifically the meanings of "prototype" and "delegate" with explanations suitable for a high school student. They received examples illustrating prototypes as templates or blueprints for creating objects with shared properties and behaviors, and delegates as objects assigned to handle events or actions for other objects.

The user also explored comparisons between Swift and JavaScript:
- In JavaScript, prototypes are objects that provide shared properties and methods for inheritance, similar to Swift's classes or structs.
- Delegates in Swift are analogous to JavaScript callback functions, where a delegate or callback handles events or actions triggered by another object.

These explanations aim to clarify the concepts by relating Swift features to familiar JavaScript mechanisms.

C15 [user_asserted; support: M35]
The user requested a data structures crib sheet in C++ covering common structures:

- Arrays: fixed-size contiguous elements, accessed by index.
- Vectors: dynamic arrays with push_back and index access.
- Linked Lists: nodes with value and next pointer.
- Stacks: LIFO structure with push, pop, and top.
- Queues: FIFO structure with push, pop, and front.
- Maps: key-value pairs with insertion and lookup.
- Sets: unique elements with insert, count, and erase.

Includes example declarations and usage notes for each.

This crib sheet provides a concise overview of essential C++ data structures.


## Active Work
C16 [user_asserted; support: M36, M37]
Provided an improved Salesforce Apex trigger code to fix update issues:

- Changed `accountID` list to a `Set<Id>` named `accountIds` to avoid duplicates.
- Created a `Map<Id, Carrier__c>` named `carrierMap` keyed by `Account__c` to reliably associate Carrier records with Accounts.
- Updated Account records by iterating over `accList` and fetching corresponding Carrier from `carrierMap` by Account Id.
- This ensures correct field updates regardless of list ordering.

Code snippet:
```java
trigger CarrierTrigger on Carrier__c (after insert, after update) {
    Set<Id> accountIds = new Set<Id>();
    for (Carrier__c carrierRecord : Trigger.New) {
        if (carrierRecord.Account__c != null) {
            accountIds.add(carrierRecord.Account__c);
        }
    }

    Map<Id, Carrier__c> carrierMap = new Map<Id, Carrier__c>(
        [SELECT Id, Account__c, iDeliver_Portal_url__c, iDeliver_Admin_url__c, Agreement_Type__c
         FROM Carrier__c
         WHERE Account__c IN :accountIds]
    );
    List<Account> accList = [SELECT Id, iDeliver_Portal__c, iDeliver_Admin_Portal__c, Agreement_Type__c FROM Account WHERE Id IN :accountIds];

    for (Account acc : accList) {
        Carrier__c carrier = carrierMap.get(acc.Id);
        if (carrier != null) {
            acc.iDeliver_Portal__c = carrier.iDeliver_Portal_url__c;
            acc.iDeliver_Admin_Portal__c = carrier.iDeliver_Admin_url__c;
            acc.Agreement_Type__c = carrier.Agreement_Type__c;
        }
    }
    update accList;
}
```

This improved trigger ensures Account records are updated correctly when related Carrier records change.

C17 [user_asserted; support: M38, M39, M40]
The user requested JavaScript hooks for intercepting and restoring native functions:

- Hook for `console.log` that overrides it to add custom behavior before calling the original function.
- Method to restore the original `console.log` after hooking.
- Hook for the `fetch` function to add custom behavior before and after the network request.

Examples provided include overriding with `apply` and restoring by reassigning the original function.

C18 [user_asserted; support: M41, M42]
The user is working on a project titled "Comparative Analysis of Sentiment on Twitter for Apple vs. Samsung Smartphones." The project execution outline includes:

1. Defining the research question about Twitter user sentiment towards Apple and Samsung smartphones.
2. Collecting tweets using the Tweepy API, ensuring ethical and legal compliance.
3. Cleaning and preprocessing data with NLP techniques like tokenization, stop word removal, and stemming/lemmatization.
4. Applying sentiment analysis algorithms (e.g., Naive Bayes, Logistic Regression, SVM, RNN, CNN) to classify tweets as positive, negative, or neutral.
5. Creating visualizations such as pie charts, bar charts, and word clouds to illustrate sentiment distributions.
6. Performing comparative analysis with statistical tests to identify significant sentiment differences.
7. Writing a report documenting the task, algorithms used, results, strengths, limitations, and future recommendations.

To do this you will need to write code, run it on some data, make some figures, collect some references, and write a few pages describing your task, the algorithm(s) you used, and the results you obtained.


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
- M11: assistant_message
- M12: assistant_message
- M13: assistant_message
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
- M36: user_message
- M37: user_message
- M38: user_message
- M39: user_message
- M40: user_message
- M41: user_message
- M42: user_message
