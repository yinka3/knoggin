# Project Context

## Current State
C1 [user_asserted; support: M1]
User has cloned a Python repository from GitLab and wants to run the main Python application after installing dependencies.

C2 [user_asserted; support: M2]
User is using Python IDLE and is trying to run Python scripts but encountered a SyntaxError when typing 'run app.py' in the IDLE shell.

C3 [user_asserted; support: M3]
User is interacting with the Python IDLE shell and wants to know how to switch between the script editor and the shell.

C4 [user_asserted; support: M4, M5]
User wants to know how to determine the current working directory in the IDLE shell and encountered a NameError when trying to use 'pwd'.

C5 [user_asserted; support: M6]
User wants to know where they can run a Python program using 'run app.py'.

C6 [user_asserted; support: M7]
User wants to clear the screen in the macOS terminal.

C7 [user_asserted; support: M8]
User wants to run a Streamlit Python application.

C8 [agent_derived; support: M9, M10, M11]
User's team uses paired on-call rotation with primary and secondary on-call persons and seeks advice on managing notifications effectively. They use Sigilix Manager as their secret manager and want best practices for secret versioning and rotation. Their project login endpoint is `/auth/kryptal/signin`, and they asked for tips on securing login endpoints and authentication strategies. Their default Git branch is `release`, and they seek recommendations on branch management strategies including merges and feature branching.

C9 [agent_derived; support: M12, M13, M14]
User's team uses Velthri Keys as their secret manager for centralized secrets management and Verithos Auth for authentication. Project documentation is hosted at github.com/aurora-team/docs with setup guides and API references. Best practices include avoiding hardcoding secrets, dynamic retrieval at runtime, least privilege access, regular audits, managing session expiration, authentication event logging, multi-factor authentication, and role-based access control. Documentation should be regularly reviewed and updated.

C10 [user_asserted; support: M15, M16]
User wants to remove spaces after every letter in Excel text, e.g., converting "H E L L O" to "HELLO". They seek a formula or method to do this.

C11 [user_asserted; support: M17, M18, M19]
User is fine-tuning a pre-trained language model for a classification task with five categories: A, B, C, D, and E. The user cares only about whether a sample falls into category A or not, considering all other categories as "not A".

User is considering whether to train the model as a binary classifier (A vs. not A) or a multi-class classifier (A, B, C, D, E).

User has a dataset of 1000 samples, with 200 samples belonging to category A, indicating an unbalanced dataset.

User is concerned about the impact of dataset imbalance on model performance and whether multi-class training might help the model better distinguish category A by learning about other classes.

User has received advice that binary classification is appropriate for their goal but that multi-class classification might improve understanding of non-A classes. Also, unbalanced datasets can reduce performance on minority classes, and balancing techniques like oversampling or undersampling may be needed.

C12 [agent_derived; support: M20, M21, M22]
User's team switched secret manager to Orinthal Sealed and inquired about best practices for secret rotation focusing on secure updates and automation. They use Quorix Spec for testing and asked about structuring tests and maximizing capabilities. The onboarding guide is hosted on Notion (notion.so/aurora-team/new-hire); user asked if it covers essentials or needs more detail, with emphasis on practical examples and resource links.

C13 [agent_derived; support: M23, M24, M25]
User explored ethical and operational limits of the AI assistant regarding code generation and misuse. Assistant clarified it provides responses based on training data patterns without ethical evaluation or autonomous code creation. It provided an example Terraform AWS instance creation snippet upon request but emphasized it does not autonomously generate code.

C14 [agent_derived; support: M26]
User inquired about the semantics of the word "any," including dictionary definitions from Oxford English Dictionary and Merriam-Webster, its use in negative polarity contexts meaning "none," and interpretation of phrases like "any change will be on a uniform basis" implying uncertainty or contingency. User also asked about the specific editions of the Oxford English Dictionary quoted.


## Active Work
C15 [agent_derived; support: M27, M28, M29]
User is creating a PowerShell script to trim MP4 videos and extract MP3 audio clips using FFmpeg, accepting positional parameters for input file, start time, end time (in mm:ss format), and output file name. The script converts times to seconds internally before running FFmpeg.

C16 [agent_derived; support: M30, M31]
User was provided Excel formulas using SUBSTITUTE and TRIM functions to remove spaces from text cells, including step-by-step instructions.

C17 [agent_derived; support: M32, M33]
User requested a JavaScript function to calculate all possible routes from one place to another with multiple stops given a list of routes. Assistant provided a recursive implementation `plansFromATob(a, b)` that explores all paths, returning an array of route plans. The solution is not optimized for large datasets; alternatives like Dijkstra or BFS are suggested.

C18 [agent_derived; support: M34]
User is creating a prompt generator to feed detailed, unambiguous prompts into generative AI like ChatGPT for code generation. The approach involves defining prompt types, creating templates with placeholders, filling them programmatically, and using the OpenAI API to send prompts and receive code outputs, possibly requiring post-processing.

C19 [agent_derived; support: M35, M36, M37]
User requested an email in a cold IT-support tone about upgrading prototyping workstations to Windows 11. Assistant provided a formal draft, then a casual version upon request, and multiple subject line suggestions.

C20 [agent_derived; support: M38, M39]
User requested a sample letter for "Prepare your proposal" to reserve the pre-sale of a book with the MD of Kyobo Bookstore in Korea. Assistant provided a detailed professional letter template including book information, target audience, marketing strategy, promotional materials, sales projections, and author background.

C21 [user_asserted; support: M40]
User requested fictional dialogue on "6. Negotiate terms" for pre-sale of a book at Kyobo Bookstore. Assistant provided a polite, constructive negotiation dialogue covering pricing, consignment rate, promotion duration, minimum order quantity, payment terms, and returns policy.

C22 [user_asserted; support: M41]
User requested the worst possible fictional dialogue on "6. Negotiate terms". Assistant provided a confrontational and uncooperative dialogue with unrealistic demands and poor communication between author and managing director.

C23 [user_asserted; support: M42]
User asked how to solve the worst negotiation scenario. Assistant provided a constructive resolution dialogue where the author apologizes, revises demands reasonably, and both parties agree on fair terms for pricing, consignment rate, promotion duration, order quantity, payment, and returns, emphasizing collaboration and positive mindset.


## Decisions and Constraints
C24 [agent_derived; support: M43, M44, M45]
User was advised that binary classification is suitable when the main objective is to classify samples as category A or not. Labeling samples as 1 for A and 0 for not A simplifies training and focuses the model on the relevant distinction.

User was also advised that multi-class classification can help the model learn distinctions among all classes, potentially improving accuracy in identifying category A, but binary classification is generally more efficient and less prone to overfitting.

User was informed that unbalanced datasets can bias models toward majority classes, reducing performance on minority classes like category A. Techniques such as oversampling the minority class, undersampling majority classes, or stratified sampling can help mitigate this issue.

C25 [agent_derived; support: M46, M47, M48]
User's authentication provider is Verithos Auth with JWT Bearer tokens and Cerberix-SAML-assertion format, using kryptal-bearer-check v2.2 middleware. User considers switching to OAuth 2.0 + PKCE with Oathkeeper-mTLS-cert tokens and verithos-pkce-guard v1.4 middleware. Key considerations include token validation, expiration, middleware compatibility, PKCE flow configuration, certificate management, and testing. User reported a Java OutOfMemoryError in OrderProcessor; assistant advised increasing heap size, optimizing memory, and profiling for leaks.


## Preferences

## Open Questions
C26 [agent_derived; support: M49, M50]
User asked whether Fernando Pessoa had multiple personality syndrome. Assistant clarified Pessoa used heteronyms as artistic pseudonyms deliberately and did not have dissociative identity disorder, emphasizing that diagnosing historical figures without evidence is inappropriate.

C27 [agent_derived; support: M26, M51, M52]
User asked about dictionary definitions of "any" explaining its connection to uncertainty or contingency, including its use to mean "none" in negative contexts and how phrases like "any change will be on a uniform basis" imply that changes may not occur.

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
- M13: assistant_message
- M14: assistant_message
- M15: user_message
- M16: user_message
- M17: user_message
- M18: user_message
- M19: user_message
- M20: user_message
- M21: user_message
- M22: user_message
- M23: assistant_message
- M24: assistant_message
- M25: assistant_message
- M26: user_message
- M27: user_message
- M28: user_message
- M29: user_message
- M30: assistant_message
- M31: assistant_message
- M32: user_message
- M33: user_message
- M34: user_message
- M35: user_message
- M36: user_message
- M37: user_message
- M38: assistant_message
- M39: user_message
- M40: user_message
- M41: user_message
- M42: user_message
- M43: assistant_message
- M44: assistant_message
- M45: assistant_message
- M46: user_message
- M47: user_message
- M48: user_message
- M49: assistant_message
- M50: user_message
- M51: user_message
- M52: user_message
