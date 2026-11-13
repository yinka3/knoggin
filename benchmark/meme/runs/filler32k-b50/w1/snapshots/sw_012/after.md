# Project Context

## Current State
C1 [agent_derived; support: M1, M2, M3]
The team documentation is hosted at notion.so/aurora-team/new-hire, covering team culture, project goals, key tools, and environment setup instructions for new hires. The secret manager has changed from Selvex Secrets to Pyronos Crypt for secure handling of sensitive information. The project changelog is maintained on the GitHub Releases page, updated typically at each release to document changes, enhancements, and fixes. Database backups are scheduled twice daily at 2am and 2pm UTC, with recommended automation and verification practices such as restoring to test environments and checking logs. The database connection string is associated with Keldaris DB for data operations. The team's code review policy allows any team member to approve changes, promoting flexibility, while maintaining quality through guidelines on coding standards, test coverage, documentation, pair programming, and rotating review duties. The team's standup meetings continue to be conducted asynchronously via written updates in Slack to accommodate different schedules and maintain communication, with a test coverage target of 90% for core modules emphasizing code quality and comprehensive testing strategies.

C2 [user_asserted; support: M4, M5, M6]
The user is exploring how to develop a Chrome extension including its menu bar, options, and functionality. They have received boilerplate code for the manifest.json, popup.html, and popup.js files as a starting point.

The user inquired whether popup.js can modify an existing website loaded in the browser. The assistant confirmed it can, provided the extension has the appropriate permissions declared in the manifest file, and gave an example of modifying a paragraph element's text.

The user also asked how to run the Chrome extension locally. The assistant explained the process of enabling Developer mode in chrome://extensions/, loading the unpacked extension folder, and noted that this is temporary until the extension is packaged and published.

C3 [user_asserted; support: M7, M8]
The user requested documentation and file structure for the Python package "PyUIX," which simplifies creating UI trees of HtmlElement objects by parsing HTML and CSS files. PyUIX uses a standard data format for UI elements, making it agnostic to rendering targets and compatible with various rendering engines.

The documentation includes:

- Introduction to PyUIX's purpose and design.
- Installation instructions via pip.
- Usage example showing how to parse HTML and CSS files to generate a UI tree.
- Explanation of the PyUIXParser class and its parse method.
- Description of the HtmlElement class with its properties and methods.
- File structure listing __init__.py, pyuix_parser.py, and html_element.py.

An extended version adds details about the render engine:

- Usage example showing instantiation of a render engine and rendering the UI tree.
- RenderEngine interface definition requiring a render(root_element: HtmlElement) method.
- Updated file structure including render_engine.py defining the RenderEngine interface.

This documentation was provided in response to the user's request to extend the PyUIX documentation to include the render engine.

C4 [user_asserted; support: M9, M10, M11]
The user asked about the difference between .bashrc and .bash_profile in Linux. The assistant explained that .bashrc runs for every new terminal session, while .bash_profile runs only at login. .bash_profile typically sets environment variables, and .bashrc sets aliases and session settings. It's common for .bash_profile to source .bashrc.

The user asked which file runs when logging in via LightDM. The assistant clarified that LightDM usually does not execute .bash_profile but uses .bashrc or .profile to set the environment. It is possible to configure LightDM or .bashrc to source .bash_profile on login.

The user requested a systemd service file for running the Duplicacy backup tool with the command "/home/liz/Applications/duplicacy_web_linux_x64_1.6.3 -background". The assistant provided a sample service file duplicacy.service with [Unit], [Service], and [Install] sections, specifying ExecStart with the command, User as liz, and WantedBy=multi-user.target. Instructions to save, reload systemd, start, and enable the service were included.

The user asked what the [Install] section with WantedBy=multi-user.target means. The assistant explained that it tells systemd to start the service automatically when the system reaches the multi-user target, which is the normal operational runlevel allowing multiple users to log in after boot.

This conversation occurred on 2023-03-09 and focused on Linux shell initialization files, LightDM login behavior, creating a systemd service for Duplicacy, and understanding systemd service installation directives.

C5 [user_asserted; support: M12, M13]
The user asked if in Handlebars there is a way to create a "component" and reuse it in other sections of the template to avoid repeating code. The assistant explained that Handlebars supports reusable components called "partials", which are defined in separate files named with a leading underscore and included in templates using the syntax `{{> partialName}}`.

The user then reported working with Zendesk, which uses Handlebars in its templates, but encountered an error when creating a partial named `_left-categories`: `"general_error"=>"the following identifier(s) are not allowed: [\"_left-categories\"]"`. They asked if there is a way to use partials in Zendesk templates.

The assistant responded that Zendesk's Handlebars engine restricts allowed identifiers in partial names, disallowing hyphens. The suggested workaround is to rename the partial file to use only allowed characters, e.g., rename `_left-categories.hbs` to `left_categories.hbs`, and include it with `{{> left_categories}}`. If issues persist, contacting Zendesk support is recommended for clarification on identifier restrictions.

C6 [user_asserted; support: M14, M15, M16]
The user asked about the best improvements in Groovy 3. The assistant explained key enhancements including improved performance with a new AST-based compiler infrastructure, a new concise lambda syntax, enhanced type inference, Java 8 support, improved static typing with compile-time checks, and a new intuitive date/time API.

The user requested more details on static typing support. The assistant explained Groovy 3's optional static typing system that uses type inference and enables compile-time type checking to catch errors early, improving code safety and robustness.

The user asked for code examples where compile-time errors are detected. The assistant provided examples showing errors such as calling a non-existent method on a String and assigning a value of the wrong type to a variable, both caught at compile time by Groovy 3's static type checker.

The user asked if annotations are required to enable compile-time checking. The assistant clarified that compile-time type checking is enabled by default when using Groovy 3's static typing system, but the @TypeChecked annotation can be added to classes, methods, or code blocks to enforce type checking explicitly. An example was given demonstrating @TypeChecked usage causing a compile-time error when calling a method with an argument of the wrong type.

C7 [user_asserted; support: M17]
The user requested an explanation of common machine learning algorithms with Python code examples. The assistant provided brief overviews and sample code for the following algorithms using scikit-learn:

- Linear Regression: supervised regression model predicting continuous values.
- Logistic Regression: supervised classification model for binary/multiclass targets.
- Decision Trees: supervised model for classification and regression using tree structures.
- Random Forests: ensemble of decision trees improving accuracy and robustness.
- Support Vector Machines (SVMs): supervised model finding hyperplanes to separate classes.

Each example included loading a dataset (Boston or Iris), creating the model instance, and fitting it to the data.

The assistant noted that this is a subset of algorithms and recommended further learning resources for comprehensive coverage.

C8 [user_asserted; support: M18]
The project has switched its database from Keldaris DB to Thalnex Store. This change impacts the ORM and migration tools, requiring updates to Pyravar Mapper and Zyndra Alter dependencies accordingly. The team is advised to focus on data migration and performance tuning for the new database setup.

C9 [user_asserted; support: M19, M20, M21]
The user has inquired about Cisco Meraki, its programming, API communication format, and requested a sample Python program to configure a Meraki router port using the Meraki API. The assistant provided detailed explanations in German about Cisco Meraki as a cloud-based network and security solution by Cisco Systems, programming languages used (primarily Python, also JavaScript, C++, Java), and that the API communicates over HTTP REST using JSON format. A Python example was given demonstrating how to configure a router port via a PUT request to the Meraki API, including setting API key, network ID, router serial, port number, payload parameters (name, type, VLAN, allowed VLANs, isolation), headers, and response handling.


## Active Work
C10 [user_asserted; support: M22]
The user provided Python code for a neuron model predicting whether to order food out or eat in, based on cost, convenience, health, and taste variables. The user requested modifying the code to ask the user for all input variables including taste, and to predict if it is more cost-efficient to order out (1) or eat in (0), without asking about ordering out directly. The assistant responded with updated code that includes the taste variable, collects user input for all variables, calculates the neuron's output, and prints whether ordering out or eating in is more cost-effective based on the output threshold.

The key code snippet from the assistant starts at "# Calculate the output of the neuron":

```python
# Calculate the output of the neuron
input_values = [cost, convenience, health, taste]
output = np.dot(input_values, weights) + bias

# Print the output
print("Probability of choosing to order food out:", output)

# Determine if it's more cost-effective to order out or eat in
if output > 0.5:
    print("It is more cost-effective to order food out.")
else:
    print("It is more cost-effective to eat food in.")
```

This reflects the user's request to include taste and to predict the choice based on the neuron's output without asking the user directly about ordering out.

C11 [user_asserted; support: M23, M24, M25]
The user is developing a ROS node in Python for a Clearpath Husky robot equipped with a Realsense RGBD camera mounted on a UR5e robot arm. The node uses ORB-SLAM2 to perform 3D SLAM. The user requested code that transforms the RGBD camera points from the UR5e arm frame back into the Husky robot's frame before applying the SLAM algorithm, using ROS tf2 for coordinate transformations.

C12 [user_asserted; support: M26, M27]
The user is writing a comedy movie script featuring characters Andy, John, Carol, Lindsey, and Ruth at a rural high school. Andy is a loud, confident computing teacher who tries to impress John, an older exam invigilator with inappropriate behavior. Carol is the serious chief exams officer, Lindsey is the intimidating headteacher covering up misconduct, and Ruth is a languages teacher obsessed with German and exploring her attraction to women.

Two scenes have been scripted:

1. Staff room scene where Andy tries to impress John with a story, Carol insists on exam focus, Lindsey warns about professionalism, and Ruth speaks German, causing confusion.

2. Disabled toilet scene where Andy makes suspicious noises practicing a talent show presentation, John confronts him about misuse of the disabled toilet, and Andy apologizes embarrassed.

These scenes establish character dynamics and comedic tension in the school setting.

C13 [user_asserted; support: M28]
The user requested example code to fine-tune the MobileNet neural network. The assistant provided a Python example using Keras that loads MobileNet without the top layers, adds new fully connected layers, freezes the base model layers initially, compiles and trains the model, then unfreezes the top layers for fine-tuning with a lower learning rate.

The example includes use of ImageDataGenerator for data augmentation and training/validation data flow, and shows how to selectively freeze and unfreeze layers for transfer learning and fine-tuning.

C14 [user_asserted; support: M29]
The team holds a recurring team meeting every Thursday at 2pm. This schedule is established for regular coordination and updates.


## Decisions and Constraints
C15 [user_asserted; support: M30, M31, M32]
The project uses Pyravar Mapper as the ORM, which depends on the database Keldaris DB. If the database changes, the ORM and related components will also need to change.

Database migrations are handled by Zyndra Alter, which also depends on the database choice.

Models are currently defined using Pyravar.entity('Model', attrs). If the ORM changes, this syntax will change accordingly.

The team plans to switch ORM to Vorathis ODM if the database changes, which uses the syntax Vorathis.collection('Model', spec) for model definitions.

A deadlock error was reported with the exact message:
"DeadlockDetected: transactions 4821 and 4823 are deadlocked on table aurora_orders.
Query 1 was UPDATE orders SET status = 'shipped' WHERE region = 'us-east'.
Query 2 was DELETE FROM orders WHERE created_at < NOW() - INTERVAL '30 days'.
First seen at 2024-08-04T22:33:18Z; recommend serializable isolation review."

This indicates a need to review transaction isolation levels and query ordering to avoid deadlocks.

C16 [agent_derived; support: M33, M18]
The database change from Keldaris DB to Thalnex Store necessitates switching the ORM from Pyravar Mapper to Vorathis ODM. This will require updating model definitions from Pyravar.entity('Model', attrs) syntax to Vorathis.collection('Model', spec). Database migrations handled by Zyndra Alter must also be adapted to the new database. Attention to compatibility, migration testing, and feature differences is critical.


## Preferences
C17 [user_asserted; support: M34]
The team prefers asynchronous standup meetings conducted via written updates in Slack to maintain communication without scheduling conflicts.

C18 [user_asserted; support: M35]
The team uses Veltrion UI Kit v2 as their design system. They emphasize following guidelines closely for consistency and incorporating accessibility features during implementation.


## Open Questions
C19 [user_asserted; support: M36, M37]
The user asked: "How should personal wedding vows be structured?" and requested an example using the suggested structure elements including a statement of love, personal story, promises, heartfelt language, conciseness, and practice advice. The assistant provided a detailed example vow incorporating all these elements.

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
- M32: user_message
- M33: assistant_message
- M34: user_message
- M35: user_message
- M36: user_message
- M37: user_message
