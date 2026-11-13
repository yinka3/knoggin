# Project Context

## Current State
C1 [user_asserted; support: M1, M2, M3]
The team uses a follow-the-sun on-call rotation across timezones to ensure continuous availability for issue handling. They use Veltrion Pkg as their package manager, which is intended to streamline dependency management in the project. Project documentation is centralized at docs.aurora.internal/wiki to support team alignment and onboarding. The development framework in use is Moxira, chosen for its efficiency and alignment with project goals. The team employs continuous deployment on merge to enable rapid iterations and quick feedback cycles. The team uses Karnex Test for testing to help ensure code quality. The team's secret manager is Verisigil Lock, used to keep sensitive information secure.


## Active Work
C2 [user_asserted; support: M4, M5, M6]
The user is working on creating a ReactJS function that returns the time left in a human-readable Portuguese string format like "2 horas e 10 minutos restantes". The function uses the pluralize library to handle plural forms of "hora", "minuto", and "restante". The user requested the function to use only const variables (no let) and to avoid reassigning strings. The function builds the time string immutably and returns it in Portuguese with correct pluralization.

C3 [user_asserted; support: M7]
The user is working on running a traceroute command to google.com and has requested the assistant to act as a terminal, outputting the results of the command.

C4 [user_asserted; support: M8, M9, M10]
The user is working on designing a real-time greenhouse gas emissions monitoring system. They have proposed a solution using a LoRaWAN SX1262 LoRa HAT for Raspberry Pi operating at 915 MHz with a 3-10 km range, powered by LiFePO4 batteries (-20C to 60C), and equipped with MH-Z16 NDIR CO2 and MICS6814 3-in-1 gas sensors (CO, NO2, NH3). The system uses a star topology with a Kerlink Wirnet iStation v1.5 Outdoor LoRaWAN Gateway for communication and data transmission.

The user requested a report-style paragraph describing this design and an alternative solution using a Zigbee-based wireless sensor network with rechargeable lithium-ion batteries and solar panels. They also requested a third alternative using a Bluetooth Low Energy (BLE) sensor network with low-power sensors and coin cell batteries.

The user asked for an evaluation comparing the proposed LoRaWAN and alternative Zigbee solutions, focusing on range, cost, complexity, and suitability for indoor/outdoor environments. They inquired about energy consumption differences between LoRaWAN and Zigbee and requested a decision matrix comparing both technologies across criteria such as range, topology, battery life, deployment, interference, data rate, and cost.

This work involves technical evaluation, comparative analysis, and decision support for selecting wireless communication technologies for environmental monitoring applications.

C5 [user_asserted; support: M11, M12, M13]
The user is working on solving a computer science problem involving computing the intersection of two lists of items. They have explored solutions using sets to handle duplicates and improve efficiency. The user has discussed the time complexity of the intersection function and ways to optimize it by building the set from the smaller list to achieve O(min(m, n)) time complexity, where m and n are the lengths of the input lists. Example Python code snippets have been provided illustrating these approaches.

C6 [user_asserted; support: M14, M15]
The user is learning React JS from zero background. They have asked for guidance on how to learn React, including recommended steps starting from JavaScript basics, HTML, CSS, and progressing to React concepts and project building. They also requested recommendations for the best beginner books on React JS, and were provided with a list of four recommended books covering React basics, real-world application building, React Native, and advanced topics.

C7 [user_asserted; support: M16]
The user is working on creating a physics-based character in Unity with XR Interaction Toolkit familiarity, aiming to expand XR interactables to be network interactable using Photon PUN 2. This includes handling attached audio by creating a networked AudioSource subclass. The user requested example C# scripts demonstrating:

1. Physics-based character movement using Rigidbody.
2. Adding gadgets like blaster and grappling hook prefabs.
3. A networked AudioSource class synchronized over Photon PUN 2.

These scripts provide a foundation for integrating physics, gadgets, and networked audio in a VR/AR multiplayer environment.

C8 [user_asserted; support: M17, M18, M19]
The user is creating a 10-video series to teach programming a PLC with Allen Bradley Motion Control using Studio 5000 version 32. Each video is one hour long. The topics outlined include: 1) Introduction to Studio 5000 v32 and new motion control features, 2) Understanding motion control instructions and functions in the PLC, 3) Using the Motion Analyzer tool for optimization, 4) Designing and documenting motion control systems, 5) Programming complex control logic with ladder and function block diagrams, 6) Troubleshooting and diagnostics in Studio 5000 v32, 7) Integrating safety into motion control systems, 8) Implementing security measures, 9) Communication protocols and network configurations, and 10) Compliance with safety requirements and guidelines. The user requested a detailed script for video topic #2, which was provided, covering key motion control instructions such as Kinetix Servo Drive, Integrated Motion, and Kinetix 6000 Multi-axis Servo Drives, with explanations and visual cues for teaching purposes.

C9 [user_asserted; support: M20, M21, M22]
The user is working on creating a Unity C# script named `CircleMover` that moves a 2D sprite in a circular path with variable speed control, an activation toggle, and a distance offset from the origin. The script uses UniRx to update the sprite's position every frame only when activated. All control variables (`speed`, `activate`, and `distance`) are private and serialized fields editable in the Unity editor. The movement is calculated using sine and cosine functions multiplied by the distance offset to determine the sprite's position on the circle.

The user also requested and received detailed documentation for this script, including descriptions of variables, methods, usage instructions, and an example code snippet illustrating the implementation.

This work involves reactive programming in Unity, serialized field management, and 2D sprite movement mechanics using UniRx and C#.

C10 [agent_derived; support: M23, M24]
The user is building a 10-week business development plan as a data science and machine learning recruiter. The plan includes weekly goals such as researching target industries and contacts, creating content and social media engagement, personalized email outreach, follow-ups and networking, cold calling, proposal development, closing deals, client onboarding and relationship building, continuous improvement, and expansion with referrals. The plan emphasizes tracking progress and adjusting strategies based on feedback and outcomes.

C11 [user_asserted; support: M25, M26, M27]
The user is working on programming a Digispark ATtiny85 board to simulate a NumLock key press every 30 seconds. They have developed code using the DigiKeyboard library to send the key press and toggle an LED indicator. The user requested enhancements to add two buttons: one to increase and one to decrease the duration (in hours) for which the NumLock key press simulation runs. The duration is limited between 1 and 12 hours, with a default of 8 hours. The code handles button debouncing and updates the timing accordingly, stopping the key press simulation after the set duration elapses. The user also inquired about differences between DigiKeyboard.h and Keyboard.h libraries, the detectability of simulated keyboards by spying software, the Arduino bootloader detection during startup, and the possibility of removing and restoring the bootloader to avoid detection during the bootloader stage.


## Decisions and Constraints
C12 [user_asserted; support: M28, M29, M30]
The build tool is Pyronis Build, which is determined by the framework in use (currently Moxira). If the framework changes, the build tool will change accordingly (e.g., to Karnex Bundler).

The project structure is currently a monorepo with /apps and /packages directories, determined by the framework (Moxira). A framework switch would change this structure to a layered one with /domain, /infra, /api.

The build command is currently `tarvex compile --release` (tied to Pyronis Build). If the build tool changes, the command will change (e.g., to `karnex build --analyze`).

The test framework is Karnex Test, also determined by the framework. A framework change would require switching the testing framework and adapting test commands (currently `karnex test --coverage`).

These decisions and constraints imply that framework changes cascade to build tools, project structure, and testing tools, requiring careful migration planning and updates to scripts, CI/CD pipelines, and documentation.

C13 [user_asserted; support: M31]
The exact error log to remember word for word is:

"OutOfMemoryError: Java heap space.
GC overhead limit exceeded after 8 consecutive failed collections in pool 'PS Old Gen'.
Heap dump written to /var/log/aurora/heap-2024-05-08.hprof at 18:42:11Z.
Recommend increasing -Xmx beyond 4G or investigating memory leak in OrderProcessor."


## Preferences
C14 [user_asserted; support: M32, M33, M34]
El usuario prefiere recibir correcciones y mejoras en textos con explicaciones claras sobre los cambios realizados, incluyendo ejemplos específicos y justificaciones para cada corrección.


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
- M23: assistant_message
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
