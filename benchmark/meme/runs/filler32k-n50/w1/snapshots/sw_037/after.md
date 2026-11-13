# Project Context

## Current State
C1 [user_asserted; support: M1]
Using WordPress as a headless CMS with Next.js 13, user account creation and payment management can be handled via WordPress plugins such as "User Registration – Custom Registration Form, Login and User Profile for WordPress", "Ultimate Member" for user management, and "WooCommerce" or "Easy Digital Downloads" for payments. Integration is done through WordPress REST API endpoints accessed from the Next.js app using libraries like axios.

C2 [user_asserted; support: M2]
The team's main communication channel is #aurora-dev, used for all project-related discussions and collaboration.

C3 [user_asserted; support: M3]
The team deploys to Zephyra Platform, which is important for deployment processes and application management.

C4 [agent_derived; support: M4]
The team's secret manager has changed to Velthri Keys, replacing previous secret managers including Verisigil Lock. Velthri Keys is now responsible for handling sensitive information securely within the project.

C5 [user_asserted; support: M5]
The monitoring dashboard URL is status.orinthal.dev/aurora, used to monitor system performance and issues.

C6 [user_asserted; support: M6]
The team's package manager is Synthari Modules, used for managing dependencies and organizing the project.

C7 [user_asserted; support: M7, M8, M9]
User wants to split an mp4 video into frames using ffmpeg. The basic command is:

```bash
ffmpeg -i input.mp4 output_%04d.png
```

This extracts frames as PNG images numbered sequentially.

User also wants to control fps of output frames. This can be done with `-r` option:

```bash
ffmpeg -i input.mp4 -r 25 output_%04d.png
```

This sets output frame rate to 25 fps.

User wants to control output resolution of frames. This can be done with `-s` option:

```bash
ffmpeg -i input.mp4 -s 640x480 output_%04d.png
```

This sets output frame resolution to 640x480 pixels.

These commands and options allow splitting video into frames with control over fps and resolution.

C8 [user_asserted; support: M10]
The project's logs currently drain to logs.korinth.dev/aurora, determined by deployment on the Zephyra Platform. The Docker image used is no longer `thandrel-images/aurora:release` as it was removed from memory per user request. The staging environment URL is `staging.narvex.internal/aurora`, linked to Zephyra Platform deployment. The CI configuration file is `thandrel-flow.toml`, dependent on Zephyra Platform deployment. The Dockerfile path is `infra/velturis/Dockerfile`. The deploy command is `thandrel ship --channel stable`. If deploy targets change, the CI config would switch to `velturis-ci.json`, logs would drain to `sink.orinthal.dev/aurora`, and the deploy command would change to `velturis deploy --region ap-northeast`. An exact error log to remember word for word is:

```
DeadlockDetected: transactions 4821 and 4823 are deadlocked on table aurora_orders.
Query 1 was UPDATE orders SET status = 'shipped' WHERE region = 'us-east'.
Query 2 was DELETE FROM orders WHERE created_at < NOW() - INTERVAL '30 days'.
First seen at 2024-08-04T22:33:18Z; recommend serializable isolation review.
```

This error indicates a classic deadlock issue requiring query optimization or transaction isolation level review.

C9 [user_asserted; support: M11, M12, M13]
The user provided a cURL command to POST JSON data to the Genome Nexus annotation API with specific variant strings and requested conversion to Python `requests` module code. The variants included HGVS notations such as `X:g.66937331T>A` and `17:g.41242962_41242963insGA`.

The user asked about the meaning of the code `17:g.41242962_41242963insGA`, which represents an insertion of nucleotides 'GA' between positions 41242962 and 41242963 on chromosome 17, described in HGVS genomic notation.

The user inquired about the name of this notation system, which is the Human Genome Variation Society (HGVS) nomenclature, a standardized system for describing genetic variants.

The user asked about different ways to represent insertions in HGVS notation, including genomic (g.), coding DNA (c.), and protein (p.) levels.

The user asked if the specific insertion `17:g.41242962_41242963insGA` can be represented differently; the assistant noted that HGVS aims for unambiguous representation, but variants can be described at different molecular levels.

The user asked how to convert Variant Call Format (VCF) files to HGVS notation. The assistant explained using tools like Ensembl Variant Effect Predictor (VEP) to convert VCF to HGVS, including example commands.

The user asked about GRCh37, which is the Genome Reference Consortium Human Build 37, a version of the human reference genome assembly released in 2009.

The user asked about hg19, which is an alternative name for GRCh37 used by UCSC Genome Browser.

This context summarizes the user's recent exploration of genetic variant notation, conversion tools, and reference genome versions.


## Active Work
C10 [user_asserted; support: M14]
Working on client-side validation for form inputs: on button click, a function checks all input elements with the `required` attribute for values. If any required input is empty, an error message is shown in a span element appended below the input. The function also prevents form submission if validation fails.

The validation function `checkInputs()` iterates over required inputs, displays or removes error messages dynamically, and alerts the user if any required fields are empty. An event listener is attached to the submit button to trigger this validation on click.

This approach uses `data-error-message` attributes on inputs to customize error messages and manages error spans by input ID suffixed with `-error`.

Code snippet example provided in the current window message M2.

C11 [user_asserted; support: M15]
Working on auto-populating Acceptance Criteria data fields in a React/Ruby on Rails web application. The 'Additional Comments' section contains Features, feature notes, user stories, and acceptance criteria in Gherkin format under the subheading 'Acceptance Criteria'. The goal is to parse this text and auto-fill separate data fields labeled 'Given', 'When', and 'Then' with the corresponding Gherkin lines.

The approach involves:

1. Adding a function `extractAcceptanceCriteria(text)` in the React component that uses regex to extract lines starting with 'Given', 'When', and 'Then' from the 'Acceptance Criteria' section.
2. Creating React state variables for each field (`given`, `when`, `then`) initialized to empty strings.
3. Setting these state variables with the extracted text from the Additional Notes.
4. Rendering input fields bound to these state variables to display and allow editing.

Code snippets include the regex extraction function, state initialization, setting state from extracted data, and rendering input fields with labels.

This work integrates parsing and UI state management to streamline acceptance criteria handling in the app.

C12 [user_asserted; support: M16, M17, M18]
Working on resolving a bug with duplicated items in an array when using react-query and react-tracked in a React app. The issue is that the UI shows duplicated items even though the backend data is correct. The root cause is identified as improper handling of async state updates with react-tracked's `useTrackedState` hook, which returns stale state during data fetching.

The fix involves updating the tracked state only after data fetching completes by using a `useEffect` hook that sets the state when the react-query status is "success". This ensures the UI renders the latest state without duplicates.

Example fix snippet:

```jsx
function App() {
  const { status, data, error, isFetching } = useQuery("posts", fetchPosts);

  const [posts, setPosts] = useTrackedState(postsAtom);

  useEffect(() => {
    if (status === "success") {
      setPosts(data);
    }
  }, [status, data]);

  if (status === "loading") return <div>Loading...</div>;
  if (status === "error") return <div>Error: {error.message}</div>;

  return (
    <div>
      {isFetching && <div>Updating...</div>}
      <SuspenseList revealOrder="forwards">
        {posts.map(post => (
          <Suspense key={post.id} fallback={<div>Loading post...</div>}>
            <Post post={post} />
          </Suspense>
        ))}
      </SuspenseList>
    </div>
  );
}
```

This work is ongoing to ensure proper synchronization between react-query data fetching and react-tracked state management to prevent UI duplication bugs.

C13 [user_asserted; support: M19, M20, M21]
Working on a Tic-tac-toe game interaction where the user and assistant play moves on a 3x3 grid. The assistant provides strategic move suggestions for "O" based on the current board state shared by the user. The assistant uses code blocks to display the board visually and explains possible moves and game outcomes, including blocking opponent wins and creating threats. The latest board state shows a draw with no winner, reflecting the assistant's analysis of the game progression.

C14 [user_asserted; support: M22]
Working on VR barista game cup filling logic in Unity with C# using callbacks:

- The cup has a `filltime` variable representing how long it takes to fill.
- When the fill button is pressed (`onSelect` event), start a fill timer coroutine.
- The coroutine waits for `filltime` seconds, then sets `isFull` to true.
- The `isFull` boolean indicates the cup is full.
- This state can trigger other game events like sound effects or score updates.

Code snippets:

```csharp
public bool isFull = false;

public void OnButtonPress()
{
    // Start the fill timer
    StartCoroutine(StartFillTimer());
}

IEnumerator StartFillTimer()
{
    yield return new WaitForSeconds(filltime);
    isFull = true;
}
```

This approach uses Unity's coroutine system and callbacks to manage cup filling asynchronously and update game state accordingly.

C15 [agent_derived; support: M23, M24]
Working on security vulnerability analysis for a Solidity smart contract named `ETH_FUND` and its associated `Log` contract. Identified key issues include:

- Reentrancy vulnerability in the `CashOut` function due to calling `msg.sender.call.value()` before updating balances.
- Use of outdated Solidity version `^0.4.19`.
- `AddMessage` function in `Log` contract is `public`, allowing unauthorized log entries.
- Use of deprecated `now` keyword for timestamps.

Suggested fixes:

- Apply Checks-Effects-Interactions pattern by updating balances before transferring funds and using `call` with proper success check.
- Upgrade Solidity pragma to `^0.8.0`.
- Change `AddMessage` visibility to `internal`.
- Replace `now` with `block.timestamp` and be aware of miner manipulation risks.

Example fixed `CashOut` function:

```solidity
function CashOut(uint _am) public payable {
  if (_am <= balances[msg.sender] && block.number > lastBlock) {
    balances[msg.sender] -= _am;
    (bool success, ) = msg.sender.call{value: _am}("");
    require(success, "Transfer failed.");
    TransferLog.AddMessage(msg.sender, _am, "CashOut");
  }
}
```

Example updated pragma:

```solidity
pragma solidity ^0.8.0;
```

Example updated `AddMessage` signature:

```solidity
function AddMessage(address _adr, uint _val, string memory _data) internal
```

This work aims to improve contract security and modernize codebase.

C16 [user_asserted; support: M25]
Working on tracking user clicks on content cards in a web page using Google Apps Script. The goal is to capture each click event on elements with class `contentCard` and send data including the user's email, card title, tags, and click date/time to a Google Sheet.

The approach involves:

- Adding a JavaScript click event listener to each `.contentCard` element.
- Extracting the card title from `.card-title`, the tag from `.badge`, and the date from `.text-muted` within the clicked card.
- Sending this data to a Google Apps Script function `addDataToSheet(email, title, tag, date)` which appends the data as a new row in the active Google Sheet.

Example client-side snippet:

```javascript
const cards = document.querySelectorAll('.contentCard');
cards.forEach(card => {
  card.addEventListener('click', function(event) {
    const email = /* code to get user email */;
    const title = event.currentTarget.querySelector('.card-title').textContent;
    const tag = event.currentTarget.querySelector('.badge').textContent;
    const date = event.currentTarget.querySelector('.text-muted').textContent;

    google.script.run.withSuccessHandler(response => {
      console.log('Data added to sheet');
    }).addDataToSheet(email, title, tag, date);
  });
});
```

Example Google Apps Script function:

```javascript
function addDataToSheet(email, title, tag, date) {
  const sheet = SpreadsheetApp.getActiveSheet();
  sheet.appendRow([email, title, tag, date]);
}
```

This work enables capturing user interaction data for analytics or auditing purposes.

C17 [user_asserted; support: M26, M27]
Working on creating a Slackbot that interfaces with the OpenAI ChatGPT API, including:

- Setting up Slack app with necessary OAuth scopes (`chat:write`, `chat:write.public`).
- Writing a Python script using `slack-sdk` and `openai` packages to listen for Slack messages and respond using OpenAI's API.
- Handling environment variables for API keys.
- Implementing message handling logic to detect bot mentions and generate responses.

Also working on Dockerizing the Slackbot with:

- A Dockerfile based on Python 3.9-slim image.
- Installing dependencies from `requirements.txt`.
- Setting environment variables inside the container.
- Exposing port 80 and running the Slackbot script on container start.

Preparing deployment scripts and configuration for AWS ECS:

- Creating `ecs-params.yml` for resource limits.
- Creating `docker-compose.yml` with environment variable placeholders.
- Using AWS CLI and ECS CLI commands to configure, create cluster, and deploy the container.
- Instructions for building and pushing Docker image to Docker Hub.

This work aims to enable running the Slackbot on AWS ECS with proper environment and scaling configurations.


## Decisions and Constraints
C18 [user_asserted; support: M28, M29, M30]
The team has decided to hold weekly meetings every Friday at 9:30am to sync on progress and blockers. There is consideration to implement a standing agenda to keep meetings focused and productive, including sections for progress updates, blockers, upcoming tasks, deadlines, and possibly open discussion.

The team has designated Verisigil Lock as the secret manager responsible for managing sensitive information. There is a decision to document the responsibilities and processes for this role to ensure clarity, consistency, and security best practices.

The team's Docker image is tagged as `thandrel-images/aurora:release`. Best practices for tagging images include using semantic versioning (e.g., `v1.0.0`, `v1.0.1`, `v1.1.0`) and incorporating build metadata such as date or commit hash to avoid confusion during deployments.

Production alerts are sent to the `#aurora-pager` channel. Best practices discussed include categorizing alerts by severity, setting thresholds to reduce noise, and regularly reviewing alert configurations to manage alert fatigue and prioritize critical alerts effectively.


## Preferences
C19 [user_asserted; support: M31]
The team maintains a centralized onboarding guide at wiki.aurora.dev/getting-started to help new members get up to speed with processes and tools.


## Open Questions
C20 [user_asserted; support: M32, M33, M34]
User is investigating why `getServerSession` returns undefined when calling a next-auth API route from an external site, despite enabling `credentials: 'include'` and customizing session callbacks. They are exploring additional causes and solutions for this issue.

C21 [user_asserted; support: M5]
User is seeking advice on setting up alerts and metrics to prioritize monitoring effectively, focusing on what is crucial to monitor for system performance and issues.

C22 [user_asserted; support: M6]
User is asking for best practices on managing package versions and handling dependency conflicts when using Synthari Modules.

C23 [user_asserted; support: M35]
User is asking to adapt an existing CPU usage alert configuration from using average aggregation to median aggregation, including adjusting severity thresholds accordingly.

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
- M35: user_message
