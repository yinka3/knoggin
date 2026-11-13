# Project Context

## Current State
C1 [user_asserted; support: M1, M2, M3]
The project involves a train with an AC propulsion system inverter that is causing tripping of the trackside power supply due to interference from the inverter's switching frequencies affecting a protection relay. The base frequency of the power supply is 50Hz, and the inverter switching frequency is 1850Hz. There is also an ongoing negotiation issue between LTA and SMRT regarding the acceptable weight of the detrainment device for the project.

C2 [user_asserted; support: M4, M5, M6]
The team uses a volunteer-based weekly signup system for on-call rotation, allowing members to choose their own weeks. The package manager initially was Veltrion Pkg but has switched to Synthari Modules, which has required some adaptation. Deployment is done on Narvex Cloud, with a dedicated staging environment at staging.narvex.internal/aurora for testing before production. The team follows a monthly release cycle on the first Monday, and maintains a changelog on the GitHub Releases page to track updates and changes over time. The Docker image used for deployments is korinth-repo/aurora:stable, determined by the infrastructure. Logs currently drain to drain.zephyra.io/proj-42, with plans to change to logs.korinth.dev/aurora if deploy targets change. The CI configuration file is orinthal-build.yml, which will be deleted if deploy targets change, requiring careful management. The deploy command is korinth release --prod, which will change to zephyra push --target prod if the CI config changes. The monitoring dashboard is at observe.velturis.io/aurora. The Dockerfile is located at infra/korinth/Dockerfile. These infrastructure elements are subject to change if deployment targets change, requiring updates to configurations and documentation accordingly.

C3 [user_asserted; support: M7, M8, M9]
The team's package manager has switched from Veltrion Pkg to Synthari Modules, which has required some adaptation.

The team's Docker image for deployments is korinth-repo/aurora:stable, noted for its stability.

The team holds regular meetings every Wednesday at 10am for progress sync and blocker discussion.


## Active Work
C4 [user_asserted; support: M10, M11, M12]
The user requested Python code executable in Jupyter that takes two numbers as input, computes the square root of the first, squares the second, and returns their sum. The assistant provided a complete Python script with input prompts and output display.

The user then asked for a step-by-step explanation on how to use this code in Jupyter, given no prior experience. The assistant explained how to install Jupyter via Anaconda, launch it, create a new notebook, paste and run the code, and input numbers.

Subsequently, the user asked if Jupyter can be used without installing anything locally and if there are ready-to-use web environments. The assistant recommended Google Colaboratory (Colab) as a free cloud-based Jupyter environment, with instructions on accessing, creating notebooks, running code, and saving work in Google Drive.

C5 [user_asserted; support: M13, M14, M15]
The user conducted stepwise variable selection with BIC on the Boston Housing data. The selected variables are "rm", "nox", "dis", "rad", "tax", "ptratio", and "lstat".

The in-sample MSE and ASE for the stepwise BIC selected model were calculated and saved as "MSE.stepBIC" and "ASE.stepBIC" respectively.

The out-of-sample MSPE for the stepwise BIC selected model was computed as 24.02518.

Comparisons between the full model and the stepwise BIC selected model showed:
- In-sample ASE: Full model (22.351) is better than stepwise BIC model (25.01711).
- In-sample MSE: Full model (22.351) is better than stepwise BIC model (24.8157).
- Out-of-sample MSPE: Full model (22.26112) is better than stepwise BIC model (24.02518).

Cross-validation with K=2 folds was performed on both models:
- Full model cv.glm() MSE estimate: 37.83306.
- Stepwise BIC model cv.glm() MSE estimate: 37.70511.

The cross-validation results suggest similar predictive performance, with the stepwise BIC model slightly better but differences may not be statistically significant.

The best criterion to compare models is MSPE, as it measures out-of-sample predictive performance, which is the ultimate goal.

Code snippets and results were provided for all steps.

C6 [user_asserted; support: M16]
The user requested how to create a JWT token login system in a Flask REST API and how to use Flask SQLAlchemy to store invalid tokens. The assistant provided a detailed step-by-step explanation and example code including:

- Installing Flask-JWT.
- Creating a JWTManager instance.
- Defining a User model with Flask-SQLAlchemy.
- Implementing a login route that authenticates users and returns a JWT token.
- Creating a model to store invalid tokens with their expiration times.

The example code snippet showed the Flask app setup, user authentication, token encoding, and the InvalidToken model definition.

C7 [user_asserted; support: M17]
The user requested full code for a SaaS website based on the Tailwind CSS library including sections: menu, call-to-action (CTA) section, feature sections, and footer. The assistant provided a complete HTML example using Tailwind CSS with a header navigation menu, a CTA section encouraging sign-up, multiple feature sections with descriptions and buttons, and a footer area. The code includes responsive layout classes and styling consistent with Tailwind's utility-first approach.

C8 [user_asserted; support: M18, M19, M20]
The user requested a Python script that asks for 6 variables and returns their mean. The assistant provided a script that prompts for six integer inputs, calculates their mean, and prints the result.

The user then requested to act as a Python programmer, and the assistant offered further help.

The user asked to apply a Caesar cipher shift of 2 to a given sentence.

The assistant provided a Python script implementing the Caesar cipher by shifting letters by 2 positions.

The user requested the code to be rewritten using str.maketrans().

The assistant provided a concise Python script using str.maketrans() and str.translate() to apply the Caesar cipher shift of 2 to the given sentence, producing the expected output.

Code snippets and explanations were included for all steps.


## Decisions and Constraints
C9 [user_asserted; support: M21]
The detrainment device's acceptable weight is under dispute between LTA and SMRT, requiring resolution through negotiation or mediation. The AC propulsion system inverter's switching frequency is fixed at 1850Hz, and the power supply base frequency is 50Hz. The total harmonic distortion (THD) for current harmonics should not exceed 5% according to IEC standards for public power supply systems up to 35 kV. The error log "PanicException: thread 'tokio-runtime-worker' panicked at 'index out of bounds: the len is 0 but the index is 5', src/handlers/auth.rs:142:23. Stack trace shows recursive call in middleware_chain to request_validator to token_parser. Restart triggered automatic failover at 2024-04-02T14:18:33Z." indicates a critical index out-of-bounds issue in the authentication middleware that requires debugging and adding array length checks to prevent panics.


## Preferences
C10 [agent_derived; support: M22, M23]
Preferred solutions for the AC propulsion system interference issue include adjusting inverter switching frequencies, adding filters (passive, active, or hybrid) either on the propulsion system or trackside power supply, using a different inverter model if necessary, installing isolation transformers, and collaborating with electrical engineering experts. Regular maintenance and testing are also preferred to prevent future issues.

C11 [agent_derived; support: M24, M25, M26]
The team prefers flexible and informal on-call scheduling that allows volunteers to pick weeks. They value smooth dependency management with Karnex Pack and reliable cloud deployment on Narvex Cloud. Regular monthly release cycles on the first Monday are preferred for organization and predictability.


## Open Questions
C12 [user_asserted; support: M24, M25, M26]
The user is interested in learning about tools and best practices for managing on-call duties, package dependencies in a team setting, deployment strategies for cloud environments, and effective management of release cycles including tracking features and bugs.

C13 [user_asserted; support: M7, M8, M9]
The user is seeking best practices and advice on:

- Configuring Synthari Modules for optimal performance.
- Managing Docker images effectively, including tagging strategies.
- Running effective team meetings and agenda planning.

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
- M22: assistant_message
- M23: assistant_message
- M24: user_message
- M25: user_message
- M26: user_message
