# Project Context

## Current State
C1 [user_asserted; support: M1, M2, M3]
On an Ubuntu machine without internet access, the user needs to create an egress proxy to allow Docker Swarm to pull images from DockerHub. The proxy will enable outbound traffic through a controlled point.

The user intends to configure this proxy using Squid, a popular caching and forwarding HTTP proxy server.

The configuration involves:
- Installing Squid on a server that can act as the egress proxy.
- Setting Squid to listen on a chosen port (e.g., 8080).
- Defining access control lists (ACLs) to allow outbound traffic from the internal network.
- Allowing HTTP access for the defined ACL.
- Restarting Squid to apply changes.
- Configuring clients (including the Ubuntu machine running Docker Swarm) to use the Squid proxy for outbound traffic.

Firewall and security settings may need adjustment to permit traffic through the proxy.

This setup enables Docker Swarm on the offline Ubuntu machine to access DockerHub images via the Squid egress proxy.

C2 [user_asserted; support: M4]
The user is exploring single image deraining using U-Net architecture trained on the Rain100L and Rain100H datasets, which contain images with low and high rain density respectively. The process involves dataset preparation, model architecture implementation, training with loss functions like MSE or SSIM, evaluation using metrics such as PSNR and SSIM, and optional fine-tuning.

The user is considering advanced techniques including spatial and temporal filtering, multiscale approaches, and their integration with deep learning architectures to improve deraining performance. Spatial filtering involves traditional filters or convolutional layers capturing rain streaks, while temporal filtering leverages video frame sequences using RNNs or 3D convolutions. Multiscale methods include Laplacian pyramids, wavelet transforms, dilated convolutions, inception blocks, hierarchical U-Nets, and progressive deraining stages.

Combining multiscale strategies with deep learning architectures like Feature Pyramid Networks, cascaded or recurrent models, and multiscale data augmentation is also under consideration to enhance robustness and accuracy across diverse rain conditions and scenes.

C3 [user_asserted; support: M5]
## Database Connection Issue

- Exact error log to remember:

```
ConnectionRefusedError: [Errno 111] Connection refused.
Service crysthene-db-primary on port 5432 is not accepting connections.
Last successful heartbeat was at 2024-03-15T08:23:41Z.
Check firewall rules and verify that max_connections has not been exceeded.
```

- Indicates connection refusal to the primary database service on port 5432.
- Suggests checking firewall settings, service status, and max_connections configuration.

This error is critical for troubleshooting database connectivity and must be kept in mind for related diagnostics.

C4 [user_asserted; support: M6, M7, M8]
## California S Corporation Owner Anonymity and Registered Agent Options

- The company is an S Corporation incorporated in California using the owner's personal name and address, which is publicly listed in the California Secretary of State business directory.
- The owner wishes to remain anonymous and remove or conceal personal information from public records.
- Options include:
  - Filing a Statement of Information to update the registered agent or nominee, replacing the owner's personal info with that of the nominee or agent.
  - Changing the registered agent to a third-party registered agent service, which provides a commercial address instead of the owner's personal address.
- Note: The owner's name as President/CEO may still appear publicly even if the address is concealed.
- Legal responsibility remains with the company to keep public records accurate and compliant.
- Consulting a lawyer or professional is recommended for best course of action.

### Reliable Registered Agent Services Examples

1. LegalZoom
2. Incorporate.com
3. Northwest Registered Agent
4. CorpNet
5. Swyft Filings

- It is advised to research, compare services, check reviews, testimonials, and BBB ratings before hiring.


## Active Work
C5 [user_asserted; support: M9, M10, M11]
Python script for serial communication between two ports with continuous loop:

```python
import serial
import time

ser1 = serial.Serial('COM1', baudrate=9600, timeout=1)
ser2 = serial.Serial('COM2', baudrate=9600, timeout=1)

running = True
while running:
    transmit_string = input("Enter a string to transmit: ")
    ser1.write(transmit_string.encode())
    received = ser2.read(len(transmit_string))
    received_string = received.decode()

    if received_string == transmit_string:
        print("Transmitted and received strings match.")
    else:
        print("Transmitted and received strings do not match.")

    # reply string is same as transmit string, no prompt
    reply_string = transmit_string
    ser2.write(reply_string.encode())
    received = ser1.read(len(reply_string))
    received_string = received.decode()

    if received_string == reply_string:
        print("Transmitted and received strings match.")
    else:
        print("Transmitted and received strings do not match.")

    # repeat loop once per second
    time.sleep(1)

    user_input = input("Do you want to continue? y/n")
    if user_input == "n":
        running = False

ser1.close()
ser2.close()
```

This script continuously sends and receives strings between two serial ports until the user quits, verifying matches each way and repeating once per second.

C6 [user_asserted; support: M12, M13]
## Audio to Text Modeling and Summarization Gradio App

- User is working on audio to text modeling, converting spoken audio into text using ASR techniques involving acoustic and language models.
- The user requested a Gradio app that takes an MP3 audio file input and outputs a summary of the transcription generated by an LLM.
- Provided code uses OpenAI Whisper API for transcription and GPT-3.5-turbo or text-davinci-002 for summarization.
- The Gradio app interface includes inputs for audio file and system prompt, and outputs a summarized transcription.
- The app launches a web interface for uploading audio and receiving summarized text output.

This work is ongoing and complements existing project tasks involving machine learning and audio processing.

C7 [user_asserted; support: M14]
## Batch Processing of Guppy Basecaller Command

- User has 20 folders labeled R1 through R20.
- Needs to run the command:
  ```
  /guppy/bin/guppy_basecaller -i ~/NR2_03082023/fast5_files/{R#} -s /fastq --flowcell FLO-MIN106 --kit SQK-LSK109
  ```
  on each folder.
- Recommended approach is a Bash loop iterating from 1 to 20:
  ```bash
  for i in {1..20}; do
    /guppy/bin/guppy_basecaller -i ~/NR2_03082023/fast5_files/R$i -s /fastq --flowcell FLO-MIN106 --kit SQK-LSK109
  done
  ```
- This runs the command on each folder R1, R2, ..., R20 sequentially.
- Assumes folders are directly under `~/NR2_03082023/fast5_files/`.

This batch processing method automates running the basecaller on all specified folders efficiently.

C8 [user_asserted; support: M15]
## TypeScript Hello World with Chakra UI

- User requested a Hello World program in TypeScript designed with Chakra UI.
- Provided code includes installation instructions for Chakra UI dependencies.
- The example defines a `HelloWorld` functional component using Chakra UI's `Box` and `Heading` components.
- The `App` component wraps `HelloWorld` in `ChakraProvider` to apply the Chakra UI theme.
- Instructions include adding a `dev` script to `package.json` and running `npm run dev` to start the app.

This addition documents the user's current work on a TypeScript React app using Chakra UI for UI design.


## Decisions and Constraints
C9 [user_asserted; support: M16, M17, M18]
## Authentication Provider and Method Dependencies

- Current authentication method: OAuth 2.0 + PKCE, determined by auth provider Phalanx Gate.
- Login endpoint: `/auth/kryptal/signin`, tied to Phalanx Gate.
- Token format: Verithos-JWT-v2, dependent on Phalanx Gate.
- Auth middleware: verithos-oauth-middleware v2.1, linked to OAuth 2.0 + PKCE.

### Planned or Possible Changes and Their Effects

- Switching auth providers will change the authentication method, login endpoint, token format, and middleware.
- Alternative auth methods considered include API Key + HMAC and Oathkeeper-mTLS-cert.
- Middleware config would change accordingly (e.g., to phalanx-key-guard v1.3 if auth method changes).

### Considerations and Watch Outs

- Redirect URIs and token lifetimes vary by provider.
- Refresh token handling may differ.
- Client-side code must adapt to different endpoints and response formats.
- Transition handling between token formats is needed.
- Middleware session and state management differences require testing.
- Key management, rotation, and revocation are critical for API Key + HMAC.
- Certificate management and mutual TLS setup are essential for Oathkeeper-mTLS-cert.
- Compatibility testing for new middleware with existing routes and services is necessary.

This block summarizes the dependencies and constraints around authentication provider and method choices, reflecting the user's detailed concerns and plans.


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
