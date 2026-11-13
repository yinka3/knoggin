# Project Context

## Current State
C1 [user_asserted; support: M1]
The project involves a Go package named `port` that provides functionality to find a free TCP port within a specified range on localhost. The package defines a `FreePort` struct with configurable minimum and maximum port numbers and a maximum number of attempts to find a free port. It includes methods to get a free port, either panicking on failure or returning an error. The default port range is 50000 to 65535, with 10 attempts to find a free port.

This package is intended for internal use to facilitate dynamic port allocation in Go applications.

C2 [agent_derived; support: M2, M3, M4]
User is exploring advanced quantum mechanics topics including Nelson's stochastic mechanics derivation of the Schrödinger equation, the Born rule and its relation to the uncertainty principle, Pavon's thermodynamics of time-dependent quantum harmonic oscillators, and variational/extremal principles foundational in physics.

C3 [agent_derived; support: M5, M6, M7]
User asked about OOP fundamentals and the four pillars: encapsulation, abstraction, inheritance, and polymorphism, with examples in Python and Java illustrating these concepts for modular and maintainable code.


## Active Work
C4 [user_asserted; support: M8, M9, M10]
User is currently working on JavaScript stream optimization tasks including:
- Creating a WhatWG ReadableStream that returns a stream of random bytes of configurable size.
- Optimizing the stream to reduce memory usage by enqueuing chunks in the pull function instead of start.

Additionally, user requested Go code to list AWS Cognito users in a group filtered by custom attribute values (profileType 1 or 2). Since the AWS SDK's ListUsersInGroup method lacks filtering parameters, the user is implementing manual filtering by paging through users and selecting those with the desired attribute values in code.

Code example uses ListUsersInGroupPages to iterate pages, filters users by checking "custom:profileType" attribute for values "1" or "2", and collects filtered users for further processing or output.

C5 [user_asserted; support: M11]
User requested a Python script to generate a feature matrix of ages, hometowns, full names, years in service, and marital status of prime ministers of all European Union countries, then train an unsupervised classifier using k-means clustering and analyze the results. The assistant provided a detailed Python example including web scraping from Wikipedia, data extraction into a Pandas DataFrame, and k-means clustering using sklearn, with code snippets for each step.

C6 [agent_derived; support: M12, M13, M14]
User is working with Azure Functions to send and receive messages from Azure Service Bus. They have explored:
- Sending messages to a Service Bus queue using an output binding with `IAsyncCollector<ServiceBusMessage>`.
- Receiving messages with a `ServiceBusTrigger` and accessing the `SessionId` property from the `ServiceBusReceivedMessage` input parameter.
- Accessing the `ApplicationProperties` dictionary from the `ServiceBusReceivedMessage` to read custom application properties.

Code examples in C# demonstrate these capabilities within Azure Functions.


## Decisions and Constraints
C7 [user_asserted; support: M15, M16, M17]
Code reviews are assigned to Seoyun Choi, with Sarah Chen currently assigning this role. If the team lead changes, the code review assignment will change accordingly.

Escalations are currently assigned to Kevin Park, with Sarah Chen assigning this role. If the team lead changes, the escalation contact will change to Jiwon Han.

An exact error log to remember word for word:
"PanicException: thread 'tokio-runtime-worker' panicked at 'index out of bounds: the len is 0 but the index is 5', src/handlers/auth.rs:142:23. Stack trace shows recursive call in middleware_chain to request_validator to token_parser. Restart triggered automatic failover at 2024-04-02T14:18:33Z."


## Preferences
C8 [user_asserted; support: M18]
User prefers practical and low-effort solutions for car protection, expressing that putting on and taking off waterproof car covers is too much work.


## Open Questions
C9 [user_asserted; support: M19]
User is exploring alternative solutions to protect their car from the elements besides waterproof covers and garages/carports.

C10 [user_asserted; support: M20]
User asks if washing a car with water regularly damages the paint.

C11 [user_asserted; support: M21]
User asks what is the right method to wash a car to avoid paint damage.

C12 [user_asserted; support: M22]
User asks what precautions need to be taken to care for car paint.

C13 [user_asserted; support: M23]
User asks if WD-40 is bad for car paint.

Support handles:
- M1: user_message
- M2: user_message
- M3: user_message
- M4: user_message
- M5: assistant_message
- M6: assistant_message
- M7: assistant_message
- M8: user_message
- M9: user_message
- M10: user_message
- M11: user_message
- M12: assistant_message
- M13: assistant_message
- M14: assistant_message
- M15: user_message
- M16: user_message
- M17: user_message
- M18: user_message
- M19: user_message
- M20: user_message
- M21: user_message
- M22: user_message
- M23: user_message
