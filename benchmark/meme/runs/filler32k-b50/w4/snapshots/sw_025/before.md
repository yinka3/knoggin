# Project Context

## Current State
C1 [user_asserted; support: M1, M2, M3]
The user is working on a programming task to simulate an experiment where the temperature in a tank increases over time according to the formula T = T0 × e^(k × t). The goal is to write a program that simulates heating to a target temperature (Treo) and determines the time to reach it, testing three different parameter sets for T0 and k within specified ranges.

The user initially received a Python implementation, then requested a C++ version. The assistant provided a C++ program with two functions: simulate_experiment and run_experiments. The program randomly selects T0 and k values within the given ranges, simulates the heating process, and finds the fastest time to reach Treo.

The user asked what Treo means, and the assistant explained it is the target temperature to reach in the experiment.

The user then pointed out a bug: Treo was not passed to the functions and caused a compilation error. The assistant corrected the C++ code by declaring Treo as a global variable, initializing it in main(), and removing it from function parameters, fixing the scope issue.

C2 [agent_derived; support: M4, M5, M6]
The user is exploring running Rust programs with network I/O in minimal environments like BIOS, UEFI, WASI, and VMs. Rust cannot run directly on BIOS but can run on minimal OS or embedded systems. Linux distros (Ubuntu, Debian, Fedora, CentOS, Arch) support virtual network drivers for Rust. WASI runs Rust WebAssembly modules with network I/O but not full Linux. Rust can be compiled for UEFI using `uefi-rs`; `netstack-rs` provides TCP/IP stack for UEFI Rust apps. Network card compatibility depends on UEFI drivers, mostly supporting modern Intel, Broadcom cards. Hyper-V and Google Cloud Platform support UEFI VMs, enabling Rust apps with network I/O in those environments.

C3 [user_asserted; support: M7, M8, M9]
The user is learning about decentralized agreements (smart contracts), decentralized oracle networks, hybrid smart contracts (on-chain + off-chain agreements), and decentralized applications (Dapps). They have requested explanations tailored to both 10-year-old children and PhD students, including examples, mind maps, and detailed learning plans. The user is exploring foundational blockchain concepts, programming smart contracts in Solidity, and understanding the interplay between on-chain automation and off-chain processes.

C4 [user_asserted; support: M10, M11]
The user is discussing Google's potential strategy to compete with ChatGPT, focusing on DeepMind's Sparrow chatbot. Sparrow, introduced in September 2022, is trained with human feedback to be helpful, accurate, and harmless, and it can access up-to-date information via Google’s internet connectivity. The user shared an assessment highlighting Google's "code red" response to ChatGPT's rise, the integration potential of Sparrow across Google apps, and the challenge posed by the Innovator's Dilemma given Google's reliance on search revenue. The user asked for an analysis of this assessment.

C5 [agent_derived; support: M12]
The assistant provided an analysis noting that the assessment is well-informed, summarizing Google's strategic focus on AI products, the role of DeepMind's Sparrow as a ChatGPT competitor, and the challenge of adopting disruptive technologies for established companies like Google. The assistant also noted the speculative nature of the information and the knowledge cutoff date.

C6 [user_asserted; support: M13, M14, M15]
The user is preparing high school biology teaching materials aligned with state standard BIO.1D, which covers the cell membrane's structure and function as a selectively permeable barrier maintaining homeostasis. The user requested and received:

- A summary of key concepts including cell membrane structure, selective permeability, transport mechanisms (simple diffusion, facilitated diffusion, osmosis, active transport), and solute concentration conditions (hypertonic, hypotonic, isotonic).
- Student notes explaining these concepts in accessible language.
- Multiple choice questions based on the notes to assess student understanding.

This teaching content is intended for 15-year-old students to understand how the cell membrane regulates transport and maintains homeostasis, including experimental investigation and modeling of solute concentration imbalances.

C7 [agent_derived; support: M16, M17, M18]
The user is exploring the use of two separate GPUs in a Windows computer where one GPU is connected via HDMI and the other is not connected to a monitor but intended to run a game like Cyberpunk 2077. The user inquired about setting up such a configuration to minimize utilization of the GPU connected to the HDMI monitor.

The assistant explained that this setup is possible via GPU passthrough or multi-GPU configurations, including virtualization with GPU assignment or SLI/Crossfire setups. The user has two ASUS Turbo 1080 Ti GPUs connected with an SLI bridge and an AIO water cooler setup.

The assistant confirmed Cyberpunk 2077 supports NVIDIA SLI and that this hardware should perform well for the game, with performance depending on CPU, RAM, storage, and game settings.

The user provided a detailed hardware setup: AMD 9500x CPU, two ASUS Turbo 1080 Ti GPUs in SLI with AIO cooling, ASUS TUF GAMING X570-PLUS (WI-FI) motherboard, 32GB 3200MHz RAM, RAID 0 NVMe drives, and a 4K 60Hz LG TV.

The assistant recommended optimal Cyberpunk 2077 settings for this setup, including Ultra graphics quality, 4K resolution, fullscreen mode, V-Sync off, medium or high ray tracing if supported, DLSS on, frame rate limit at 60 FPS, anti-aliasing enabled, and motion blur off or low.

The assistant noted these are general guidelines and actual optimal settings may vary based on user preferences and future game updates.


## Active Work
C8 [agent_derived; support: M19, M20, M21]
The team is currently discussing branching strategies and sprint progress tracking. They are considering adding a dedicated development branch (dev) alongside the main branch to manage feature work more effectively. The sprint deadline is set for June 20, 2025, providing ample time for planning features. The team lead is Jihoon Ryu, who plays a key role in maintaining team focus and morale. The team uses a round-robin text post format for daily standups by 10am, which helps document updates and maintain accountability. Discussions include how Jihoon communicates priorities and delegates tasks, and how to make text standup posts more effective by encouraging clear, structured updates highlighting achievements, challenges, and next steps. The team releases sprint-end releases every 2 weeks to keep momentum and iterate quickly. They use a centralized incident runbook at github.com/aurora-team/runbooks, which is regularly updated after major incidents with team contributions encouraged. The code review policy requires 1 approval plus CI green, with strategies to ensure timely reviews including clear feedback timelines, designated reviewers per code area, and active communication about review progress.

C9 [user_asserted; support: M22]
The user is currently working on a TypeScript program that reads an XLSX file and exports a JSON map. This map uses keys formatted as `${SheetName}-${Cell}` and maps them to the formula used in each corresponding cell. The assistant provided a TypeScript example using the `xlsx` package to read the file, iterate through sheets and cells, extract formulas, and write the JSON output to a file named `output.json`.

C10 [user_asserted; support: M23, M24, M25]
The user has assigned code reviews to Seoyun Choi, with Jihoon Ryu as the team lead responsible for this assignment. If the team lead changes, the code review assignment will also change.

For escalations, Kevin Park is the designated contact, assigned by Jihoon Ryu. If the team lead changes, the escalation contact will change accordingly.

The user provided an exact error log to remember word for word:

"CertificateVerificationError: certificate chain validation failed for crysthene-db-primary.internal. Subject CN does not match expected hostname; chain depth 2 unverified. Observed at 2024-07-22T11:09:47Z by tls_handshake_listener. Rotate the cert via Cryptara Vault and restart the connection pool to recover."

If the team lead changes, the escalation contact will be Jiwon Han.

C11 [user_asserted; support: M26, M27, M28]
The user is guiding an 80-year-old lady step-by-step to access the FRITZ!Box user interface via the emergency IP address 169.254.1.1. The instructions include:

1. Connect the computer to the FRITZ!Box using a LAN cable.
2. Clear all other connections between the FRITZ!Box and other devices.
3. Disconnect any wireless LAN connection to the FRITZ!Box.
4. Connect the cable specifically to the "LAN 2" port on the FRITZ!Box.
5. Restart the computer.
6. Open a web browser and enter the emergency IP address 169.254.1.1.
7. Enter the password if prompted; if the password is unknown, reset the FRITZ!Box to factory settings.

The user is confirming understanding or asking clarifying questions after each step, such as where to plug the cable, how to clear connections, and what the password is.

This stepwise guidance is tailored to be simple and clear for an elderly user.

C12 [user_asserted; support: M29]
The user is currently developing a simple React app that emulates a ChatGPT interface. The app allows modifying prompts using JavaScript before making GPT API calls. The assistant provided a detailed React example using functional components, state hooks, and Axios for API requests. The user can customize the prompt modification logic in the handleSubmit function before sending it to the GPT API endpoint.


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
- M11: user_message
- M12: assistant_message
- M13: user_message
- M14: user_message
- M15: user_message
- M16: assistant_message
- M17: assistant_message
- M18: assistant_message
- M19: assistant_message
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
