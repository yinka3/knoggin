# Project Context

## Current State
C1 [user_asserted; support: M1, M2]
The user is asking for the full names of abbreviations in the Science and Technology areas and has received explanations for several abbreviations including API, VPN, XML, SSL, and ADN (DNA in French).

The user has also requested improvements to a Python function `find_countries_by_region_and_income` that filters countries by region and income group from joined dataframes. The assistant suggested using pandas `.query()` or `.loc[]` methods for clearer, more maintainable filtering, providing code snippets for both approaches.

C2 [user_asserted; support: M3]
The user is planning a 4 day family trip to Brighton and has requested a detailed itinerary covering activities, dining, and sightseeing for each day.

C3 [agent_derived; support: M4, M5, M6]
The Aurora team uses GitHub Flow with PR reviews on feature branches, emphasizing clear guidelines, comprehensive descriptions, and regular review schedules to streamline the process. The secret manager has evolved from Verisigil Lock to Kerberos Trove and now to Orinthal Sealed, with best practices advised for secret management including least privilege, rotation, and integration into CI/CD pipelines using environment variables and access controls. Deployments target Orinthal Serve for production and staging.narvex.internal/aurora for staging, with recommendations for rollback plans, automated testing, and ensuring staging mirrors production closely. The monitoring dashboard at watch.korinth.dev/aurora is set up with structured logging and tiered alerting to avoid noise, using metadata like timestamps and request IDs for effective debugging. Project documentation is maintained at github.com/aurora-team/docs, with strategies to keep it current by integrating updates into development workflows, scheduling reviews, and using version control. The team uses a hybrid standup format combining weekly synchronous meetings and daily asynchronous updates, with interest in tools and practices to manage asynchronous communication effectively. Test coverage targets are 70% overall and 95% for authentication, supported by integrated testing, regular reviews, coverage tools, and code review discussions focused on tests.

C4 [user_asserted; support: M7, M8, M9]
The user is working on a minor project report for an alumni portal for their college, which previously did not have such a portal. The project uses React JS, CSS, and MongoDB technologies. The user has received detailed content for the report sections including Introduction, Objectives, Software Requirements Specification (SRS), project report, and Conclusion.

C5 [user_asserted; support: M10, M11, M12]
The user is inquiring about mTLS (mutual Transport Layer Security) authentication for IoT devices, including its definition, how SIM cards can support mTLS by storing certificates, and the feasibility of implementing mTLS via SIM applets. The user also asked about example code for such implementations, and about SIM cards with different CPU and RAM capabilities, including manufacturers of high-processing-power SIM cards.

C6 [agent_derived; support: M13, M14, M15]
The user investigated a Kafka messaging issue where messages from the Authentication API to Karma's Signup and BTC Wallet topics failed in production but succeeded in development. The problem was isolated to Partition #4 with partial Kafka server connection loss and "Consumer-Null" entries matching lost messages. The assistant explained ISR (in-sync replicas) as replicas caught up with the leader, recommended checking ISR and replicas for Partition #4 using kafka-topics.sh CLI or Kafka AdminClient API, and advised reviewing Kafka server logs for replication or leader election issues.

C7 [user_asserted; support: M16]
The user is preparing a 500-word encouragement message aimed at black entrepreneurs and professionals to apply for highly coveted on-air personality spots at Brownstone WorldWide Network. The message emphasizes the exclusivity of the opportunity, the network's commitment to amplifying black voices, and the impact applicants can have by sharing their expertise and stories.

C8 [agent_derived; support: M17, M18]
The user compared Hugging Face, Pinecone, and Milvus. Hugging Face specializes in NLP with pre-trained models and fine-tuning. Pinecone is a real-time vector search engine optimized for scalable, efficient search with easy API integration but limited clustering/classification support and a smaller community. Milvus is an open-source vector database supporting storage, indexing, and search of high-dimensional data with CPU/GPU acceleration and a wide range of algorithms, but has a steeper learning curve and limited real-time search optimization. The choice depends on project requirements.

C9 [user_asserted; support: M19, M20, M21]
The user is exploring digital marketing analytics, including the top website metrics to track, their importance, decisions enabled by these metrics, popular analytics platforms (Google Analytics, Adobe Analytics, Piwik), and an action plan for implementing digital marketing analytics in a business.

C10 [user_asserted; support: M22, M23, M24]
The user's Docker image is `zephyra-hub/aurora:prod`, determined by their infrastructure deploying on Orinthal Serve; changes in infrastructure would change this image.

The user's CI configuration file is `zephyra-pipeline.json`, also determined by the infrastructure and subject to change if infrastructure changes.

The Dockerfile used is located at `deploy/Dockerfile.zephyra`, linked to the Docker image `zephyra-hub/aurora:prod`.

The deploy command currently used is `zephyra push --target preview`, which depends on the CI config `zephyra-pipeline.json`.

An exact error log to remember word for word is:

"DeadlockDetected: transactions 4821 and 4823 are deadlocked on table aurora_orders.
Query 1 was UPDATE orders SET status = 'shipped' WHERE region = 'us-east'.
Query 2 was DELETE FROM orders WHERE created_at < NOW() - INTERVAL '30 days'.
First seen at 2024-08-04T22:33:18Z; recommend serializable isolation review."

If deploy targets change, the CI config will change to `thandrel-flow.toml`.

If the CI config changes, the deploy command will change to `thandrel ship --channel beta`.


## Active Work
C11 [agent_derived; support: M25]
The assistant has provided a 4 day itinerary for a family trip to Brighton, including visits to Brighton Pier, Royal Pavilion, The Lanes, Brighton Museum and Art Gallery, Seven Sisters cliffs, North Laine neighborhood, Booth Museum of Natural History, Queen's Park, and British Airways i360, with suggestions for meals and entertainment.

C12 [agent_derived; support: M26, M27, M28]
The assistant has provided detailed content for the alumni portal project report sections: a detailed Introduction explaining the portal's purpose and features; a list of Objectives for the portal; two versions of the SRS, one generic and one specific to React JS, CSS, and MongoDB; a sample project report summarizing the project background, objectives, methods, results, and conclusion; and a detailed Conclusion highlighting project success and future plans.

C13 [agent_derived; support: M29]
The assistant has provided a detailed 500-word encouragement message for black entrepreneurs and professionals to apply for on-air personality spots at Brownstone WorldWide Network. The message highlights the exclusivity of the spots, the network's mission to amplify black voices, the opportunity to share expertise in business and leadership, and instructions on how to apply.

C14 [agent_derived; support: M30, M31, M32]
The assistant has provided detailed explanations on why to use a digital marketing dashboard, the top metrics to track on a website (including website traffic, bounce rate, average time on site, pages per session, conversion rate, referral traffic, SEO metrics, and mobile traffic), the decisions that can be made based on these metrics, popular analytics platforms (Google Analytics, Adobe Analytics, Piwik), and steps to implement digital marketing analytics in a business.

C15 [user_asserted; support: M33, M34, M35]
The user is engaged in a creative screenplay narrative involving characters AngrySonics, "Too many requests in 1 hour. Try again later.", and Weslie. The story depicts a sequence of hacking incidents between AngrySonics and "Too many requests in 1 hour. Try again later.", escalating to police involvement, courtroom scenes, and emotional meltdowns. Key plot points include AngrySonics hacking the other's TV to play loud crying and fart sounds, resulting in arrests and legal consequences primarily for AngrySonics, with "Too many requests in 1 hour. Try again later." evading prosecution. The narrative culminates in AngrySonics seeking therapy for anger management and attempts to resolve the conflict with others targeted by "Too many requests in 1 hour. Try again later.".

This ongoing story is characterized by escalating pranks, legal drama, and emotional consequences among the characters.

C16 [user_asserted; support: M36, M37]
The user is modifying the WordCells game to generate puzzles based on well-known grammatical patterns rather than random word addition. The assistant suggested loading word lists for nouns, adjectives, and verbs from the 'wordlists' directory, creating a function to generate puzzles using patterns like noun-adjective and noun-verb, and updating the Flask route '/generate_puzzle' to use this function. Additionally, the user requested changes to the 'index.html' to display the current puzzle type and instructions on what kind of word to submit. The assistant provided HTML and JavaScript modifications to show the puzzle type and corresponding user guidance dynamically when a new puzzle is generated.


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
- M25: assistant_message
- M26: assistant_message
- M27: assistant_message
- M28: assistant_message
- M29: assistant_message
- M30: assistant_message
- M31: assistant_message
- M32: assistant_message
- M33: user_message
- M34: user_message
- M35: user_message
- M36: user_message
- M37: user_message
