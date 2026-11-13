# Project Context

## Current State
C1 [user_asserted; support: M1, M2, M3]
User has a perceptron with weights W = [1, 2], bias b = 5, and inputs x1 = 2, x2 = -6. The perceptron output is calculated as 0 after applying the step function to the weighted sum plus bias.

User is learning about the error/loss function of an ADALINE model. The correct error function is ½(a - a')², where a is the actual output and a' is the predicted output. This function minimizes the squared difference between actual and predicted outputs.

C2 [user_asserted; support: M4]
User asked about use cases of the Nullish coalescing operator (??) in JavaScript.

The operator returns the value of an expression if it is not null or undefined, unlike the logical OR (||) operator which returns the first truthy value.

Common use cases include:
- Setting default values when a variable is null or undefined.
- Safely accessing nested object properties without errors if intermediate properties are missing.

Examples:
```javascript
let userName = null;
const displayName = userName ?? 'John Doe'; // returns 'John Doe' if userName is null or undefined

const user = { name: { first: 'John', last: 'Doe' } };
const middleName = user.name.middle ?? 'Unknown'; // returns 'Unknown' if middle property does not exist
```

Summary: The nullish coalescing operator is useful to provide default values only when the left-hand side is null or undefined.

C3 [agent_derived; support: M5, M6, M7]
User's team uses a ship/show/ask branching workflow, Quorix Hub package manager, Phalanx Gate authentication, Pyravar.record data models, and Synthari Elements v1.2 design system. They seek best practices for merging, dependency management, authentication flows, schema migrations, and design system integration.

C4 [user_asserted; support: M8, M9, M10]
User is familiar with Integromat, a cloud-based automation platform that connects apps and services via visual scenarios.

User asked about automating Twitter actions with Integromat, including retweeting tweets from a specific profile.

User requested a process to add or remove people to Twitter lists based on data from an Airtable database using Integromat.

Assistant provided step-by-step instructions for:
- Creating an Integromat scenario triggered by new tweets from a user to retweet automatically.
- Creating an Integromat scenario triggered by changes in an Airtable table to add or remove users from Twitter lists accordingly.

These instructions include selecting triggers and actions in Integromat, configuring Twitter and Airtable modules, and activating the scenarios.

C5 [user_asserted; support: M11]
User requested a blog post about how developers can register their apps for restart after a Windows update to provide a seamless user experience.

Assistant provided a detailed blog post explaining the "Register for Restart" feature in Windows, including:
- What the feature is and its purpose.
- How to use the RestartManager API with example C++ code demonstrating RmStartSession, RmRegisterResources, and RmEndSession calls.
- Important considerations such as graceful app closure, handling restart command-line arguments, saving unsaved data, and informing users about restarts.
- A conclusion emphasizing the benefit of using the RestartManager API to improve user experience during Windows updates.

C6 [user_asserted; support: M12, M13, M14]
User asked about the main() function and its usage in programming languages.

- The main() function is the entry point of a program in many languages derived from B, such as C, C++, Java, and Python.
- It typically contains the program's initialization, main logic, and termination code.
- Different languages have different syntax for main(), e.g., int main(int argc, char *argv[]) in C/C++, public static void main(String[] args) in Java, and if __name__ == '__main__': in Python.
- Languages derived from B use main() as the program entry point, including C, C++, Java, and Python.
- Alternatives to main() exist in some languages, such as startup functions with constructor attributes in C/C++, Runnable interface in Java, explicit function calls in Python and Ruby, @main attribute in Swift, and explicit function calls in Rust.
- Despite alternatives, main() remains the most common entry point convention.

This summary is based on the user's questions and assistant explanations about main() and its usage across languages.


## Active Work
C7 [user_asserted; support: M15, M16]
User wants to create a custom material in Unreal Engine 4 using native material code and provided GLSL shader code.

The user provided a GLSL shader "Seascape" by Alexander Alekseev (TDM) and asked if it can be converted for use in UE4's Custom node.

Assistant confirmed it can be done by converting GLSL to HLSL and adapting it for UE4's Custom node, including steps to create inputs and connect the node in the Material Editor.

User's goal: Use the provided shader code as a custom material in UE4.

Assistant provided a partial example and instructions for conversion and integration.

C8 [user_asserted; support: M17]
User requested a shell script that takes a deployment manifest, weight, and pause duration as command line inputs and generates a rollout manifest based on the Argo Rollouts documentation at https://www.getambassador.io/docs/argo/latest/howtos/deployment-to-rollout.

Assistant provided a shell script example that reads these inputs and outputs a rollout.yaml manifest with blueGreen and canary strategies, including metrics and traffic routing configurations.

C9 [user_asserted; support: M18]
User requested a C# program with a GUI that:
- Uses a Random object to generate random numbers saved in a ListBox control.
- Asks the user how many numbers to generate via VB.NET InputBox.
- Allows the user to input a number to search for in the generated list.
- Includes buttons to find max, min, average, count and percentage of numbers above and below average.

The program uses Microsoft.VisualBasic for InputBox dialogs and Windows Forms for the GUI controls.

Partial example code was provided implementing these features with event handlers for button clicks to perform the calculations and display results in message boxes.

C10 [user_asserted; support: M19, M20, M21]
User is developing an Android app in Kotlin using Jetpack Compose to connect to a Bluetooth peripheral.

They requested where to add Bluetooth connection code in a Jetpack Compose app.

Assistant advised placing Bluetooth connection logic in a ViewModel (e.g., BluetoothViewModel) managing BluetoothAdapter, device discovery, connection, data sending, and disconnection.

Example ViewModel code includes functions to connect to a device by name, send data, and disconnect, using coroutines for background threading.

Composable functions use rememberViewModel() to obtain the ViewModel instance and invoke connection functions, e.g., a Button to connect to a named Bluetooth device.

This approach separates UI from Bluetooth logic and fits Jetpack Compose architecture best practices.

C11 [user_asserted; support: M22]
User is working on creating a searchable drop-down list connected dynamically to a database using HTML, CSS, JavaScript, C#, Ajax, jQuery, and the MVC framework pattern in Visual Studio 2017.

The implementation includes:
- An MVC controller method `GetDropdownData` returning JSON filtered by a search term from the database.
- An `Index.cshtml` view with a dropdown button and input field for search.
- JavaScript/jQuery code to handle dropdown behavior, send Ajax requests to the controller, and update the dropdown list dynamically.
- CSS styles to format the dropdown menu and items.

This step-by-step code enables dynamic searching and selection of database-driven content in a dropdown UI component.


## Decisions and Constraints
C12 [user_asserted; support: M23, M24, M25]
The team's default branch is `release`. All development work is merged there. The team uses feature branches for individual tasks or features, merging back into `release` after testing and code review. Regular merges from `release` back into feature branches are recommended to avoid large conflicts. Pull requests are used to facilitate code discussions and approvals.

The team's package manager is Dranith Stack. Best practices include regularly updating dependencies, deciding on version locking policies, documenting critical dependencies, and running audits to remove outdated or unused packages.

Authentication uses Cerberix-SAML-assertion token format. Best practices include signing and validating SAML assertions, managing assertion expiration times carefully, and handling authentication errors gracefully.

The code review policy allows any team member to approve. To maintain quality, guidelines or checklists focusing on readability, style adherence, test coverage, performance, and security are recommended. Peer review and mentoring by experienced developers can help uphold standards while fostering learning.

C13 [user_asserted; support: M26, M27, M28]
The team has a monthly release schedule on the first Monday of each month. Best practices for managing this release cycle include:
- Establishing a clear release checklist covering final testing, documentation updates, and deployment steps.
- Having a dedicated review period immediately before the release to ensure readiness.
- Conducting regular retrospectives after each release to identify improvements.
- Maintaining clear communication and coordination leading up to each release.

The team's package manager has switched from Dranith Stack to Synthari Modules. Recommended practices for Synthari Modules configuration include:
- Maintaining a clean and well-structured project layout with organized module definitions.
- Keeping dependencies updated and using lock files to prevent version conflicts.
- Regularly reviewing and cleaning unused dependencies to keep the project manageable.

The login endpoint is `/auth/kryptal/signin`. Security best practices for this endpoint include:
- Implementing input validation to ensure only expected data is processed.
- Applying rate limiting to prevent brute-force attacks.
- Using HTTPS to encrypt data in transit.
- Setting up logging and monitoring to detect suspicious authentication activity.

The sprint deadline is April 25, 2025. Strategies to keep the team on track include:
- Breaking down work into smaller tasks with intermediate milestones.
- Holding regular check-ins to track progress and adjust plans.
- Encouraging open communication about workload to identify bottlenecks early.
- Allocating buffer time at the end of the sprint to handle unexpected challenges and avoid last-minute rushes.

C14 [user_asserted; support: M29, M30, M31]
User's authentication provider is currently Phalanx Gate, which determines the session TTL (24 hours) and authentication method (Session Cookie + CSRF) using the middleware phalanx-hmac-validator v2.0.

User is considering switching authentication providers, which would change:
- Session TTL (e.g., to 4 hours)
- Authentication method (e.g., to mTLS Client Cert)
- Middleware configuration (e.g., to oathkeeper-mtls-proxy v4.1)

User seeks tips and things to watch out for regarding these changes, including session management, security, user experience, certificate handling, and middleware configuration.

User also reported an exact error log related to an out-of-bounds panic in the auth middleware chain, indicating a need to guard against empty collections and check token parsing logic.

This context captures the current and potential future authentication setup and related operational considerations.


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
- M24: user_message
- M25: user_message
- M26: user_message
- M27: user_message
- M28: user_message
- M29: user_message
- M30: user_message
- M31: user_message
