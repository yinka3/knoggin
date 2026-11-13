# Project Context

## Current State
C1 [user_asserted; support: M1, M2, M3]
The user is working on a blog post about "The differences between coin lockers and baggage storage services." They have requested content including subtitles, descriptions, hashtags, and main titles in markdown format, as well as a Korean translation of a main title about the benefits of using coin lockers.

C2 [agent_derived; support: M4, M5, M6]
The project uses Orinthal Sealed as the secret manager to securely handle sensitive information, recently upgraded from Selvex Secrets to Verisigil Lock to improve security around sensitive data. Best practices include ensuring team understanding, auditing secrets regularly, managing environment-specific secrets separately, and documenting processes.

The team uses Zenthos-Session-v1 token format for authentication, transmitting tokens over HTTPS, implementing reasonable expiration times, using short-lived access tokens with longer-lived refresh tokens, and revoking tokens on logout or suspicious activity.

Authentication is managed via Phalanx Gate, with best practices including clearly defining user roles and permissions, integrating authentication flows seamlessly, and implementing logging and monitoring.

The login endpoint for user authentication is /auth/kryptal/signin.

The development workflow follows trunk-based development with feature flags branching to streamline releases and gradual feature rollouts. To keep the codebase clean, the team manages the lifecycle of feature flags through regular reviews, documentation, and consistent naming conventions.

The team's default branch is main, where all latest changes and features are merged.

The sprint deadline is set for April 25, 2025, critical for planning deliverables and tracking progress.

The incident runbook is located at docs.aurora.internal/runbooks and is essential for incident management.

Best practices for integrating Orinthal Sealed and handling session tokens are documented and emphasized for secure workflow integration.

C3 [agent_derived; support: M7, M8, M9]
The user requested explanations for multiple abbreviations including JPA, ASAP, HN, LOL, SIMD, ROB, UAV, HUMIT, UW, IP, MAC, HDMI, MIPS, FPGA, and IC. The assistant provided detailed definitions and contexts, noting some abbreviations have multiple meanings depending on context.

C4 [user_asserted; support: M10]
The user requested a blog article explaining how to properly tune TensorFlow Serving's server-side dynamic batching. The article covers the concept of dynamic batching, key parameters such as batch timeout and maximum batch size, recommended starting values, and guidance on tuning these parameters based on request characteristics and performance monitoring.

C5 [user_asserted; support: M11, M12, M13]
The user asked about causes for LED lights frequently burning out or getting fused. The assistant explained possible causes including overheating, overvoltage, poor quality, and incorrect installation. The assistant also explained that absence of proper grounding can cause LED lights to fuse prematurely because grounding provides a safe path for fault currents and enables surge protectors to function properly. Additionally, the assistant clarified that when a voltage tester light turns on upon touching the body of an electrical appliance, it indicates the appliance body is electrically live due to improper grounding, wiring faults, leakage current, or capacitive coupling. The assistant emphasized the importance of proper grounding and electrical safety.

This information is based on the user's questions and the assistant's detailed explanations about LED light failures, grounding, and voltage tester behavior.


## Active Work
C6 [user_asserted; support: M14]
The user is currently working on implementing caching for the `google_maps_flutter` package in a Flutter project. They have requested code and guidance on how to cache Google Maps widgets effectively using the `cached_network_image` package, including example code snippets and explanations.

C7 [user_asserted; support: M15]
The user requested a Node.js program that can fade between n given pictures by the amount of n given input float values. The assistant provided example code using the Jimp library to load images and fade between them by compositing with specified opacity values. The function `fadeBetweenImages(images, values)` creates a new blank image and composites each input image with the corresponding opacity value, then saves the result as `result.png`.

This code snippet demonstrates how to fade multiple images based on input float values representing opacity levels.

C8 [user_asserted; support: M16]
The user is working on adding Google reCAPTCHA to a Laravel login system. They requested step-by-step guidance including obtaining API keys, installing the `anhskohbo/no-captcha` package, configuring environment variables and service providers, adding reCAPTCHA to the login form view, including the reCAPTCHA script, and validating the reCAPTCHA response in the login controller's `validateLogin` method.

C9 [user_asserted; support: M17, M18]
The user requested code to create a Chrome extension that saves GPT chatbot conversations to Notion, where each question and answer pair is a separate section in a Notion article. The assistant provided example JavaScript code using the Notion API to create pages for each Q&A pair and a Chrome extension background script that triggers saving on button click.

The user then asked how to use this code to create the extension files. The assistant explained the steps to create the extension folder, save the provided code as `background.js`, create a `manifest.json` file with the necessary configuration, add an `icon.png` file, and load the unpacked extension in Chrome via `chrome://extensions` with Developer Mode enabled. The assistant also described how to activate the extension and save conversations by clicking the extension icon.

This work is ongoing and involves building a Chrome extension for GPT conversation saving to Notion.

C10 [user_asserted; support: M19]
The user requested a script for a 10-minute video about the "Winix 5500-2 Air Cleaner with Plasma Wave Technology" based on product descriptions and web reviews. The script highlights major pros such as sleek design, effective allergen removal, and Plasma Wave Technology benefits, and cons including initial odor and limited effectiveness on pet hair. The script is written in first person, fair and unbiased, suitable for Amazon's YouTube channel style, and includes references to specific customer reviews.

The script narrative begins by introducing the product, then discusses pros with linked customer experiences, followed by cons with linked reviews, and concludes with an overall balanced recommendation and encouragement to visit the product page for more information.

C11 [user_asserted; support: M20]
The user is currently building a form using React Hook Form and React Native, implementing a multiple choice question where all selected choices are stored as an array in the form state. They requested guidance on writing controllers to achieve this.

The assistant provided example code using the `Controller` component from React Hook Form combined with `CheckBox` components from `react-native-elements`. The solution initializes the form field with an empty array and updates it by adding or removing options based on user interaction, ensuring the selected options are maintained as an array in the form state.

This work is ongoing and involves form state management for multiple choice inputs in React Native.

C12 [user_asserted; support: M21, M22, M23]
The user is playing a text-based game titled "Hogwarts and the Battle of London" created by GameGPT. The playable character is Harry Potter, accompanied by Ron Weasley and Luna Lovegood. The game starts in Hogsmeade with the goal to defeat the Dark Wizard in London. The user chose to Apparate directly to London despite a 20% chance of failure, then cast a patronus charm to ward off the Dark Wizard's dark magic, successfully completing the goal.

C13 [user_asserted; support: M24, M25]
The user requested a Python script that saves a screenshot of the webpage https://www.midjourney.com/app/feed/all/ every ten minutes. The assistant provided example scripts using both `pyautogui` and `PIL` (Python Imaging Library) for taking screenshots, including code snippets that open the webpage in a new tab, capture the screen, save the image with a timestamped filename, and wait ten minutes before repeating. The PIL example also includes guidance on cropping the screenshot to the webpage area.

This work involves automating periodic webpage screenshot capture using Python scripting and image libraries.

C14 [user_asserted; support: M26, M27, M28]
The user is working on an article titled "Omnichannel Customer Service: How and when to deploy chatbots?" for the journal "Management Science." They have requested and received an introduction discussing the rise of chatbots in omnichannel customer service, their benefits, and deployment guidance.

The user also explored how chatbots can save costs by handling high volumes of interactions without additional staffing, automating routine tasks, and providing 24/7 service.

They discussed situations where chatbots may not be suitable, such as complex or sensitive issues requiring human empathy, and where live agents may not be necessary, such as for easily accessible information or repetitive tasks.

The user examined scenarios where both chatbots and live agents are used together for optimal customer service, with chatbots handling routine inquiries and live agents managing complex issues.

Finally, the user considered which types of businesses benefit from chatbots (e.g., e-commerce, travel) versus live agents (e.g., financial institutions, healthcare), and concluded with thoughts on maximizing chatbot effectiveness while balancing human support.

C15 [user_asserted; support: M29, M30]
The user is working on C++ classes `Show` and `Ticket` in separate header and source files. They want to assign seats in `Ticket` using the number of seats and rows from `Show`. The solution involves passing a reference to a `Show` instance into `Ticket`, including `Show.h` in `Ticket.h`, and calling the getter methods from `Show` within `Ticket`. Example code shows `Ticket` holding a reference to `Show` and accessing `num_seats()` via a member function `GetNumSeats()` in `Ticket`.


## Decisions and Constraints
C16 [user_asserted; support: M31, M32, M33]
The current authentication method is SAML 2.0 SSO provided by Phalanx Gate, with session TTL set to 30 minutes. The auth middleware in use is cerberix-sso-bridge v1.9, which depends on the SAML 2.0 SSO method. If the authentication provider or method changes, these configurations will also change accordingly.

A potential future switch to session cookie + CSRF authentication is planned, which would involve changing the middleware to zenthos-csrf-guard v1.1. This change requires careful handling of secure cookie settings, CSRF token implementation, and session expiration strategies.

An exact error log to remember for troubleshooting is:

"CertificateVerificationError: certificate chain validation failed for crysthene-db-primary.internal.
Subject CN does not match expected hostname; chain depth 2 unverified.
Observed at 2024-07-22T11:09:47Z by tls_handshake_listener.
Rotate the cert via Cryptara Vault and restart the connection pool to recover."


## Preferences

## Open Questions
Support handles:
- M1: user_message
- M2: user_message
- M3: user_message
- M4: assistant_message
- M5: assistant_message
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
- M28: user_message
- M29: user_message
- M30: user_message
- M31: user_message
- M32: user_message
- M33: user_message
