# Project Context

## Current State
C1 [user_asserted; support: M1, M2, M3]
User is interested in structural equation modelling (SEM) and how to compare different SEM models, including understanding the concept of nested models.

User requested example R code for SEM model comparison including mediation effects.

User requested the example to be adapted to include psychological variables from the mindset literature, specifically variables such as fixed_mindset, growth_mindset, effort, motivational_beliefs, self_esteem, and social_support.

C2 [user_asserted; support: M4]
User's team uses a paired on-call rotation with a primary and secondary on-call for support.

Sprint deadline is set for 2025 April 11.

Approval authority for the project is VP Taehyun Kim.

Escalations should be directed to Jiwon Han.

The team practices continuous deployment triggered on merge.

C3 [user_asserted; support: M5, M6]
User is integrating research on molecular docking studies of banana flower flavonoids as insulin receptor tyrosine kinase activators for diabetes mellitus treatment into their MSC thesis.

User requested concept research ideas related to this topic, including exploring natural compounds, molecular mechanisms, efficacy and safety studies, clinical trials, biomarker identification, diabetes complications, pharmacokinetics, preparation methods, and varietal comparisons.

C4 [agent_derived; support: M7, M8, M9]
User asked about the sum of two independent exponential random variables X and Y with rate 7, confirming the sum W = X + Y follows a Gamma distribution with shape 2 and rate 7. User provided a joint PDF f(x,y) = 2/16 for 0 < y < x < 4 and requested calculations of E(Y), E(X), and Var(X), with corrections leading to E(X) = 10/3 and Var(X) = 104/45.

C5 [user_asserted; support: M10, M11, M12]
User is studying and requesting academic explanations for Windows API-based C code snippets involving file operations, encryption via XOR, and system calls related to wallpaper changes and file deletion. The user is focused on understanding the detailed behavior of these functions in an academic learning context.

C6 [user_asserted; support: M13]
Sprint deadline updated to June 6, 2025, replacing the previous deadline of April 11, 2025.

C7 [user_asserted; support: M14, M15]
Team lead is Sarah Chen, noted for keeping the team aligned and focused on goals.

Team's secret manager is Verisigil Lock, used for handling sensitive information securely.

C8 [user_asserted; support: M16, M17, M18]
User is a data science professor expert in causal discovery and causal inference methodologies.

User asked for explanations distinguishing causal discovery (identifying causal relationships from data) and causal inference (making causal predictions based on identified relationships).

User inquired about the reliability of causal discovery methods, evaluation without ground truth graphs, and how to compare multiple causal discovery algorithms.

User is interested in applying causal discovery to survey data on attitudes toward products, seeking recommendations on suitable algorithms for cross-sectional data and explanations of key assumptions like faithfulness.

User requested examples of causal inference models.

The assistant provided detailed explanations covering:
- Definitions and differences between causal discovery and causal inference.
- Factors affecting reliability of causal discovery methods.
- Evaluation strategies without ground truth, including simulations and cross-validation.
- Use of structural equation modeling (SEM) for hypothesis-driven causal analysis.
- Use of constraint-based algorithms (PC, FCI) for data-driven causal discovery on single-time-point survey data.
- Explanation of the faithfulness assumption and how to test it.
- Examples of causal inference models: Propensity Score Matching, Difference-in-Differences, Instrumental Variables, SEM, Bayesian Networks.

This knowledge supports the user's research and teaching in causal methods and their application to survey data analysis.

C9 [user_asserted; support: M19, M20, M21]
User asked about splitting the string '1-1, 1-1' into ['1','1','1','1'] using the split method with a regex. The proper regex provided was `/[-,]\s*/` which splits on '-' or ',' followed by optional whitespace.

Detailed explanation was given: `[-,]` matches '-' or ',', and `\s*` matches zero or more whitespace characters following the separator. This ensures splitting ignores spaces after separators.

User requested further clarification on why `\s*` is used, focusing on ignoring whitespace after separators to cleanly split the string.

User expressed thanks after explanation.


## Active Work
C10 [user_asserted; support: M22, M23]
```python
import bybit
import pandas as pd
import datetime

# APIキーとシークレットキーを設定
api_key = 'L6uhbOWj9W1XUFDPUw'
api_secret = 'mbvZN8VbbSpdHJj7VtgqyFvRV7VYnnxRMxU6'

# BybitのAPIクライアントを作成
client = bybit.bybit(test=True, api_key=api_key, api_secret=api_secret)

# 開始日と終了日を設定
start_date_str = '2022-03-11'
end_date_str = '2023-03-11'
start_date = int(datetime.datetime.strptime(start_date_str, '%Y-%m-%d').timestamp())
end_date = int(datetime.datetime.strptime(end_date_str, '%Y-%m-%d').timestamp())

# データを取得
result = client.Kline.Kline_get(symbol="BTCUSD", interval="1d", **{'from': start_date}).result()

# 取得したデータをデータフレームに変換
df = pd.DataFrame(result[0]['result'])

# CSVファイルに保存
df.to_csv('BTCUSD.csv', index=False)
```

C11 [user_asserted; support: M24, M25]
User requested 50 examples of natural language queries and their corresponding Cypher language query pairs from various graph database schemas, excluding previously shown examples.

Assistant provided the first 10 examples in JSON format, including queries about movies released in 2010, friends of a person named John Doe, products priced above 100, authors of books with more than 300 pages, employees in the IT department, cities connected by direct flights, users who reviewed a specific restaurant, courses by difficulty level, players scoring more than 20 goals, and companies founded before 2000.

C12 [user_asserted; support: M26, M27, M28]
User is working on integrating AWS SES email delivery status notifications with Azure Queue Storage across three environments (Lab, Preprod, Prod).

User described architecture where a single AWS Lambda function receives SNS notifications triggered by SES, and forwards messages to the appropriate Azure Queue Storage test-queue based on the originating Azure environment.

User requested and received example Node.js Lambda function code that parses a custom email header `X-Azure-Environment` from the SNS message to determine which Azure queue URL to forward the notification to.

The Lambda function example includes logic to extract the custom header from the SNS message JSON payload, conditionally select the Azure queue URL, and a placeholder for forwarding the message.

User confirmed understanding that the Lambda function uses its own environment variable and the custom header in the email to route messages correctly.


## Decisions and Constraints
C13 [user_asserted; support: M29, M30, M31]
If the team lead changes, the weekly report recipient assignment to Taehyun Kim will also change accordingly.

If the team lead changes, the code review responsibility currently assigned to David Lee will shift to Seoyun Choi.

These role assignments depend on the current team lead, Sarah Chen, and will update if the lead changes.

C14 [agent_derived; support: M32]
Exact error log to remember word for word:

"CertificateVerificationError: certificate chain validation failed for crysthene-db-primary.internal.
Subject CN does not match expected hostname; chain depth 2 unverified.
Observed at 2024-07-22T11:09:47Z by tls_handshake_listener.
Rotate the cert via Cryptara Vault and restart the connection pool to recover."


## Preferences
C15 [user_asserted; support: M33]
User prefers marketing video scripts that are concise (30 seconds) and creatively explain marketing ideas that subscribers can try.

User appreciates a variety of video script ideas including unboxing, product demos, customer testimonials, behind-the-scenes, Q&A, holiday promotions, product comparisons, expert interviews, brand stories, tips and tricks, how-to guides, educational content, event promotions, influencer endorsements, user-generated content, giveaways, social responsibility, product launches, interactive, and humorous videos.


## Open Questions
C16 [agent_derived; support: M4, M34, M35]
How should tasks be prioritized to meet the sprint deadline of April 11, 2025?

What are effective strategies for managing workload and tracking progress?

What testing frameworks are recommended to achieve 90% test coverage for core modules?

What strategies can ensure project documentation remains up to date and engaging for the team?

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
- M33: user_message
- M34: user_message
- M35: user_message
