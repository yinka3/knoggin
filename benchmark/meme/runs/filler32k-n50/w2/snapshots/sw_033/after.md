# Project Context

## Current State
C1 [user_asserted; support: M1]
The user is working on CSS code for a modal window intended to appear at the top of the screen. They have shared their current CSS code for the modal and modal-content classes, which includes full-screen overlay styling and centered modal content with margin-top set to 15%. The user reported that the modal does not appear at the top as desired.

The assistant provided updated CSS code adjusting the `.modal-content` margin to `5% auto 0` to position the modal window near the top of the screen, removing the top margin to zero to align it at the top.

This context captures the user's current CSS and the assistant's suggested fix for positioning the modal window at the top of the viewport.

C2 [user_asserted; support: M2]
The team lead for the current project has changed to Jihoon Ryu. This update supersedes the previous team lead Seokjin Kang. The team should be prepared for any changes in direction or communication style that may come with this leadership transition.


## Active Work
C3 [agent_derived; support: M3, M4, M5]
The user is reviewing and revising paragraphs of a thesis document, focusing on improving clarity, precision, cohesion, and objectivity in the writing. They have shared multiple paragraphs related to problem statements, justifications, and objectives, and requested detailed observations and corrections in tabular form. The user also asked for guidance on how to write project goals and thesis objectives clearly and effectively.

The assistant has provided structured feedback on each paragraph, including specific observations about unclear phrasing, redundancy, grammar errors, and suggestions for rephrasing. The assistant also gave examples of corrected paragraphs and explained characteristics for writing goals in projects and theses.

Currently, the user has requested a corrected version of a detailed paragraph describing a manufacturing process and its associated problems, seeking a clearer, more objective, and cohesive rewrite.

C4 [agent_derived; support: M6, M7, M8]
Corrected version of the paragraph describing the manufacturing process and its problems:

"Corporación Tecnológica New Line SA de CV es una empresa especializada en la fabricación de reguladores y transformadores de voltaje, equipos fundamentales para la regulación y protección del voltaje en diversas edificaciones e industrias, lo que genera una alta demanda en el mercado.

Dentro de la empresa, uno de los procesos fundamentales para la fabricación de transformadores es la elaboración de las “donas”. Este proceso consiste en enrollar dos láminas de metal alrededor de un molde cilíndrico con un diámetro específico, formando una pieza con forma de dona. Posteriormente, la dona se envuelve cuidadosamente con cinta aislante para cubrir completamente la lámina, seguida de una capa de tela especial aislante, y finalmente se vuelve a envolver con cinta aislante. Luego, se embobina con un cable de cobre calibre 12, realizando 60 vueltas alrededor de la dona. Una vez finalizado, las donas se prueban aplicándoles 12 voltios; si el proceso es correcto, la dona debe liberar 0.003 amperios.

El problema principal identificado es el tiempo considerable que requiere un trabajador para completar este proceso, lo que afecta la eficiencia de la producción. Además, el proceso demanda un esfuerzo físico significativo, lo que puede influir en la productividad y bienestar del trabajador. Por tanto, es necesario optimizar este proceso para reducir tiempos y minimizar el esfuerzo físico requerido."

C5 [user_asserted; support: M9, M10, M11]
The user is analyzing revenue data from a SQL table with columns revenue, month, year, and country, covering the last 10 years. They are investigating why revenue in Vietnam dropped in Q4 2022 (Oct-Dec). The user has run SQL queries to obtain:

- Total revenue for Q4 (Oct-Dec) in Vietnam for the last 10 years.
- Average revenue for Q4 in Vietnam over the last 10 years (93).
- Total revenue for the entire year 2022 in Vietnam (412).
- Average revenue for the entire year in Vietnam over the last 10 years (287).

The data shows that while the total revenue for 2022 was higher than the 10-year average, the Q4 revenue in 2022 (131.3) was lower than the previous year and close to the 10-year average, indicating a specific drop in that quarter. The user is seeking further analysis to understand the cause of this drop.

This context captures the user's data-driven investigation into quarterly revenue fluctuations in Vietnam using SQL queries and summary statistics.

C6 [user_asserted; support: M12, M13, M14]
The user requested Arduino Uno code for a 4-wheel Bluetooth mobile robot controlled by a mobile phone. The assistant provided a complete Arduino sketch that uses SoftwareSerial on pins 10 and 11 to communicate with a Bluetooth module. The code defines pins 2, 3, 4, and 5 to control the four wheels via motor driver inputs. It listens for single-character commands ('F', 'B', 'L', 'R', 'S') sent over Bluetooth to move the robot forward, backward, left, right, or stop respectively.

The assistant also explained the code in detail, describing the setup of serial communication, pin modes, the main loop reading Bluetooth commands, and the motor control functions that set the appropriate pins HIGH or LOW to drive the motors.

Additionally, the assistant provided wiring connection instructions mapping the Bluetooth module pins (VCC, GND, TX, RX) and motor driver inputs (IN1-IN4) to Arduino pins and power, as well as motor outputs to the left and right motors.

Finally, the assistant gave steps for pairing an iPhone with the Bluetooth module on the robot via the iPhone's Bluetooth settings, enabling control through a mobile app that sends commands over the Bluetooth serial connection.

C7 [user_asserted; support: M15]
The user requested an RFP document for installing a large solar array on the roof of the new Lululemon headquarters building in Vancouver, BC. The RFP includes sections on project introduction, scope of work, timeframe (12 months), cost estimate requirements (covering design, engineering, procurement, installation, commissioning, and maintenance), staffing estimates for all phases, proposal submission instructions, and evaluation criteria emphasizing technical capabilities, project plan, specifications, cost, staffing, and company experience.

C8 [agent_derived; support: M16]
The assistant provided a detailed RFP document for the solar array installation project at Lululemon's new Vancouver headquarters. The document covers introduction, scope, timeframe, cost and staffing estimates, proposal submission, and evaluation criteria, structured to guide qualified solar energy firms in submitting proposals.

C9 [user_asserted; support: M17, M18, M19]
The user is developing a Google Chrome extension that activates only on the URL https://chat.openai.com/chat. The extension will insert a multiple file input before the unique textarea on the page, split selected text files according to a user-defined limit, and send each split sequentially into the textarea, clicking the adjacent send button after each. The extension must detect when the send button is enabled (not disabled) before sending the next split. Each split includes specific English prefix and suffix messages referencing the file name and split number as per the user's detailed instructions.

The user requested a development plan and detailed implementation starting with the manifest.json file for the extension, which was provided. The manifest includes permissions for storage and the target URL, declares the content script to run on the chat page, specifies an options page for user settings, and includes icon placeholders.

This context captures the initial phase of the extension development focused on the manifest setup and the user's detailed functional requirements for file splitting and sending in the chat interface.


## Decisions and Constraints
C10 [agent_derived; support: M20, M1]
The user wants the modal window to appear at the top of the screen rather than centered vertically. The assistant suggested changing the `.modal-content` margin to `5% auto 0` to achieve this positioning.

The modal overlay covers the entire viewport with a semi-transparent black background, and the modal content is styled with a white background, border, padding, shadow, and rounded corners.

No JavaScript or HTML changes were discussed; the focus is on CSS adjustments.

C11 [user_asserted; support: M21, M22, M23]
The code review responsibility is assigned to Seoyun Choi by the current team lead, Seokjin Kang. If the team lead changes, this assignment will change accordingly.

The weekly report recipient is currently Hyunwoo Nam, assigned by Seokjin Kang. This assignment will also change if the team lead changes, with James Lee becoming the recipient.

An exact error log to remember word for word is:
"ConnectionRefusedError: [Errno 111] Connection refused.
Service crysthene-db-primary on port 5432 is not accepting connections.
Last successful heartbeat was at 2024-03-15T08:23:41Z.
Check firewall rules and verify that max_connections has not been exceeded."


## Preferences

## Open Questions
Support handles:
- M1: user_message
- M2: user_message
- M3: assistant_message
- M4: assistant_message
- M5: assistant_message
- M6: assistant_message
- M7: user_message
- M8: user_message
- M9: user_message
- M10: user_message
- M11: user_message
- M12: user_message
- M13: user_message
- M14: user_message
- M15: user_message
- M16: assistant_message
- M17: user_message
- M18: user_message
- M19: user_message
- M20: assistant_message
- M21: user_message
- M22: user_message
- M23: user_message
