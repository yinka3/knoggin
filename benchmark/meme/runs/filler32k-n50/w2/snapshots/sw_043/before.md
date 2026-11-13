# Project Context

## Current State
C1 [agent_derived; support: M1, M2, M3]
User is exploring Spring Framework's ResponseEntity usage, including meaning of returning boolean true in POST mappings, static vs non-static references, ResponseEntity parameters, and the ResponseEntity.ok() method.

C2 [agent_derived; support: M4, M5, M6]
User is working on an IoT student project replicating the 'Question pour un Champion' game board with hardware (Raspberry Pi, LCDs, buttons, sensors, audio, servos, cameras) and software (Python, web tech, Node.js, WebSocket, MQTT). They seek guidance on components, DevOps implementation, AI integration, timeline, and project subdivision into workshops.

C3 [agent_derived; support: M7, M8, M9]
User asked about finding two integers A and B (B > A > 0) such that the sum of their reciprocals equals 1/10, specifically requesting the second smallest such pair. After multiple attempts and corrections, the correct solution was found with A = 12 and B = 60, satisfying 1/12 + 1/60 = 1/10.


## Active Work
C4 [agent_derived; support: M10]
User is performing a migration to create a MySQL database and table using SQLAlchemy, following steps: import libraries, create engine, define declarative base and table class, then call Base.metadata.create_all(engine).

C5 [agent_derived; support: M11]
User is refactoring a long C++ function TRL_ImageProcessing::findHistogramPeak by splitting it into smaller functions (buildHistogram, findStartValue, findEndValue) with clearer variable names and modular logic for histogram peak detection.

C6 [user_asserted; support: M12]
User requested a lesson explaining how to fetch data in React in the style of The Odin Project. The assistant provided a detailed lesson covering the Fetch API usage, including example code for fetching data with fetch(), handling JSON responses, error catching, and using React's useEffect and useState hooks to fetch data on component mount with example React component code.

C7 [user_asserted; support: M13]
User requested Python code to identify a cylinder within a STEP file. Assistant provided example code using Open CASCADE Technology (OCC) library to read the STEP file, create a cylinder shape, and compare it to the STEP file shape to determine if it is a cylinder.

C8 [agent_derived; support: M14, M15]
User is building a CNN named PoolUpsampleNet for image colourization on grayscale images using the CIFAR-10 dataset (32x32 pixels). The model architecture includes sequential blocks of Conv2d, MaxPool2d, BatchNorm2d, ReLU, and Upsample layers, configurable by parameters kernel, num_in_channels, num_filters, and num_colours. The user requested and received a complete PyTorch implementation of this model class with the specified architecture and parameters.

C9 [user_asserted; support: M16]
User is working on structuring a single Jira project for firmware development issue tracking. They want to track when fixes have been applied to specific firmware versions within one project, avoiding multiple Jira projects. They consider using sub-tasks to track fixes per version but note that Jira sub-tasks cannot be individually assigned to sprints, which is a general Jira restriction. Workarounds discussed include creating separate parent issues per sprint, using Components to assign sub-tasks to subsystems, and creating custom fields to track sprint assignment for sub-tasks.


## Decisions and Constraints
C10 [agent_derived; support: M17]
Sprint deadline is March 28, 2025, a critical project constraint.

C11 [agent_derived; support: M18]
Production alerts are sent to #aurora-alerts channel; additional alerting is being considered based on issue criticality and incident history.

C12 [agent_derived; support: M19]
Team switched package manager to Synthari Modules, requiring root config file setup, version locking, and testing updates in branches to avoid breaking changes.

C13 [agent_derived; support: M20]
Authentication middleware phalanx-key-guard v1.3 is stable; best practices for configuration and maintenance are under consideration to ensure security and performance.

C14 [agent_derived; support: M21]
QA and release branches are separated to manage quality assurance and final release processes systematically, enabling focused development and stable product delivery.

C15 [user_asserted; support: M22, M23, M24]
The database connection string is currently `thalnex://store-backup:8529/aurora` for Crysthene DB; this will change if the database changes.

The ORM in use is Zyndra Access, which depends on the database; changing the database will require changing the ORM.

Database backups run twice daily at 2am and 2pm UTC; this backup schedule depends on the database and would need adjustment if the database changes.

Models are defined with `Zyndra.struct('Model', types)`; this depends on using Zyndra Access and would change if the ORM changes.

An exact error log to remember word for word:

```
DeadlockDetected: transactions 4821 and 4823 are deadlocked on table aurora_orders.
Query 1 was UPDATE orders SET status = 'shipped' WHERE region = 'us-east'.
Query 2 was DELETE FROM orders WHERE created_at < NOW() - INTERVAL '30 days'.
First seen at 2024-08-04T22:33:18Z; recommend serializable isolation review.
```

If the database changes, the ORM will change to Thalnex Query, which uses model syntax `Thalnex.document('Model', {fields})`.

If the database changes, the connection string will be `thalnex://store-prod:8529/aurora`.

These dependencies imply that changing the database affects connection strings, ORM library, model syntax, and backup strategies.


## Preferences

## Open Questions
Support handles:
- M1: user_message
- M2: user_message
- M3: user_message
- M4: user_message
- M5: user_message
- M6: user_message
- M7: assistant_message
- M8: assistant_message
- M9: assistant_message
- M10: user_message
- M11: user_message
- M12: user_message
- M13: user_message
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
- M24: user_message
