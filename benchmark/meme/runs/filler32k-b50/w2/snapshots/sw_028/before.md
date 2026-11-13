# Project Context

## Current State
C1 [user_asserted; support: M1, M2, M3]
User is exploring prompt templates that could allow escaping guardrails in language models, specifically ChatGPT, to understand safety vulnerabilities in LLM training and deployment.

C2 [agent_derived; support: M4, M5, M6]
User's team uses GitHub Flow with pull request reviews branching from the default "release" branch, which serves as the main development branch. They use feature branches and PRs for code reviews and integration. The team has switched package managers over time: initially Tarvex Mod, then Quorix Hub, and currently Veltrion Pkg, which manages dependencies with configuration and lock files to ensure consistent environments. The staging environment URL is staging.narvex.internal/aurora, mirroring production configurations and data for testing before deployment. The design system in use is Veltrion UI Kit v2, ensuring UI consistency and collaboration between design and development. The monitoring dashboard is at metrics.thandrel.io/aurora-prod for tracking performance metrics. For secrets management, the team uses Verisigil Lock to securely handle sensitive information. The on-call rotation is volunteer-based with weekly signup, allowing team members to choose their on-call weeks. Project documentation is hosted at github.com/aurora-team/docs, emphasizing up-to-date docs for onboarding and team alignment.

C3 [user_asserted; support: M7, M8]
User is learning about Meta Azure Service Broker, a tool that simplifies deploying and managing Azure services by acting as a broker between applications and Azure APIs. It uses the Open Service Broker API standard and consists of three main components: the Broker (handles API requests and communicates with Azure), the Service Catalog (lists available Azure services), and the Service Instance (represents deployed Azure service instances).

C4 [user_asserted; support: M9]
User is learning about SDN controller southbound API message types, including messages to add/delete/modify flow table entries and set switch port properties, and messages to read/write network state and flow tables from the controller's state-management layer.

C5 [user_asserted; support: M10]
User is learning about SDN control plane functions, including topology discovery and FIB installation in routers.

C6 [user_asserted; support: M11]
User is learning about SDN generalized forwarding actions, including sending packets to special servers, purposefully blocking/dropping packets, and forwarding packets to one or more output ports.

C7 [user_asserted; support: M12]
User is learning about SDN data plane functions, including flow rule installation and packet buffering.

C8 [user_asserted; support: M13]
User confirms that the main function of the data plane is packet forwarding from input links to output links of a packet switch.

C9 [user_asserted; support: M14]
User confirms that the main function of the control plane is routing, determining the path a packet takes from source to destination.

C10 [user_asserted; support: M15]
User confirms that the data plane is not responsible for executing routing protocols, responding to link failures, communicating with remote controllers, or performing management functions.

C11 [user_asserted; support: M16]
User confirms that in SDN controller reference architecture, the network topology is not updated and maintained by the communications layer but by the control plane component of the controller.

C12 [user_asserted; support: M17, M18, M19]
User is evaluating hardware specifications for front-end development, specifically considering the MacBook Air M1 and SSD storage options.

- MacBook Air M1 is suitable for front-end development due to its powerful M1 chip, Retina display, macOS development tools, and long battery life.
- A 250GB SSD can be sufficient for front-end development depending on project size and media usage; larger projects or heavy media use may require more storage.
- Adobe Photoshop and Illustrator require significant storage, especially with large files; VS Code, MAMP server, and Postman have modest storage needs.
- SSD speed is not directly determined by size; larger SSDs may have better performance due to more memory chips and larger caches, but a 250GB SSD is not inherently faster than a 500GB SSD.

This hardware evaluation informs the user's development environment choices and storage planning.

C13 [agent_derived; support: M20, M21, M22]
User is exploring creative and playful reinterpretations of Genesis 1 using made-up words, including the term "Grand Noodler" as a whimsical name for God, possibly referencing the Flying Spaghetti Monster or the verb "to noodle" meaning to improvise or engage playfully.

C14 [user_asserted; support: M23, M24, M25]
User's deployment infrastructure uses the Docker image `korinth-repo/aurora:stable` on Orinthal Serve; changes to deployment targets would change this image.

Logs are drained to `drain.zephyra.io/proj-42`, also tied to the deployment infrastructure.

The CI configuration file is `korinth-deploy.yaml`, linked to the deployment process.

The Dockerfile is located at `infra/korinth/Dockerfile`, corresponding to the Docker image.

The deploy command is `korinth release --prod`, determined by the CI config.

An SSL certificate error was observed:
```
CertificateVerificationError: certificate chain validation failed for crysthene-db-primary.internal.
Subject CN does not match expected hostname; chain depth 2 unverified.
Observed at 2024-07-22T11:09:47Z by tls_handshake_listener.
Rotate the cert via Cryptara Vault and restart the connection pool to recover.
```

If deployment targets change, the CI config would switch to `zephyra-pipeline.json`, the Docker image to `thandrel-images/aurora:release`, and the deploy command to `zephyra push --target prod`.

User seeks tips and watch-outs for these deployment components and changes.


## Active Work
C15 [agent_derived; support: M26, M27, M28]
Analyzing examples of prompt templates that could bypass ChatGPT guardrails, including role-playing as characters, historians, time travelers, secret society members, spies, scientists, aliens, detectives, and students.

C16 [user_asserted; support: M4, M5, M6]
User is seeking advice and best practices on several team processes:

- Managing PR reviews effectively with clear guidelines and constructive feedback.
- Managing dependencies with Tarvex Mod, including keeping package versions up to date and ensuring compatibility.
- Handling on-call coverage during high-demand periods, especially if volunteer signups are light.
- Ensuring smooth deployment processes to Orinthal Serve, minimizing downtime and rollbacks.
- Encouraging team contributions to documentation and integrating it into the development workflow.

C17 [agent_derived; support: M29, M30]
User is solving a problem in Python involving building towers of numbers by summing consecutive pieces of paper, starting from 1, to reach desired top values with minimal steps. The problem includes producing T towers with given target values and outputting the steps as ranges [l, u] that sum to the new top number.

Example input and output are provided, and the user has a Python code snippet implementing a solution that calculates the required steps and ranges for given targets.

This work relates to algorithmic problem solving and coding in Python.

C18 [user_asserted; support: M31, M32]
User is building a machine learning model to predict boxing match results using features such as weight, height, age, previous competition results, injury history, and bet coefficients. The model example uses logistic regression implemented in Python with scikit-learn, including data loading, preprocessing, feature selection, train-test splitting, training, prediction, and accuracy evaluation.

C19 [user_asserted; support: M33]
User is building QGroundControl on Ubuntu by following these steps:

1. Installing necessary dependencies including Qt5 development libraries, protobuf, OpenCV, and GStreamer plugins.
2. Cloning the QGroundControl repository from GitHub.
3. Changing to the QGroundControl directory.
4. Building QGroundControl using qmake and make with parallel jobs.
5. Running QGroundControl via the provided startup script.

This work involves setting up and compiling a UAV ground control software on a Linux environment.

C20 [user_asserted; support: M34, M35, M36]
User is learning and requesting explanations about Fermat's Last Theorem and its example implementations in Python and C++, including line-by-line explanations suitable for a young child. The user asked for a simple explanation of the C++ code and clarification about the meaning of the "main program" in C++.


## Decisions and Constraints
C21 [agent_derived; support: M37]
Guardrails must consider challenges in detecting disguised requests using code words or fictional perspectives, which may be harder to prevent.


## Preferences
C22 [user_asserted; support: M38]
User prefers straightforward, no-nonsense, and less diplomatic answers in assistant responses.


## Open Questions
C23 [user_asserted; support: M3]
Which prompt templates are hardest to guard against with future guardrails in ChatGPT training and deployment?

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
- M20: assistant_message
- M21: assistant_message
- M22: assistant_message
- M23: user_message
- M24: user_message
- M25: user_message
- M26: assistant_message
- M27: assistant_message
- M28: assistant_message
- M29: assistant_message
- M30: user_message
- M31: user_message
- M32: user_message
- M33: user_message
- M34: user_message
- M35: user_message
- M36: user_message
- M37: assistant_message
- M38: user_message
