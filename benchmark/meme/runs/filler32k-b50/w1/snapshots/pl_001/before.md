# Project Context

## Current State
C1 [user_asserted; support: M1, M2, M3]
User recently moved into a new apartment and is furnishing it on a budget. They live near a new Target store that opened about 3 weeks ago and frequently shop there, especially enjoying the affordable and stylish home decor section.

User is interested in sustainable fashion and home decor, specifically vintage or second-hand stores offering unique, one-of-a-kind pieces.

User has placed 3 grocery orders using the Giant Eagle app with in-store pickup, saving money and time.

User is considering visiting a new vintage clothing store in the trendy part of town, hoping it might also carry home decor items or provide recommendations for other stores.

User is also thinking about purchasing new kitchen utensils and gadgets and is seeking recommendations for online stores with wide selections at affordable prices.

C2 [user_asserted; support: M4, M5, M6]
User's hobby is pottery, focusing mostly on functional items like bowls and vases. They have created a bowl with a unique glaze that changes colors in different lights, achieved through trial and error.

User drives a Zyvanta Sedan, which they find reliable for daily needs.

User has a dairy-free dietary restriction and enjoys cooking creative meals and snacks that fit this restriction.

User's partner is named James; they enjoy cooking together, especially recipes that accommodate the dairy-free diet, such as a dairy-free pasta dish with cashew cream sauce.

User is currently reading "The Quorath Codex," a book they find interesting.

C3 [user_asserted; support: M7, M8, M9]
User is planning a birthday dinner for their sister this weekend and is preparing a dessert, specifically a Decadent Chocolate Lava Cake recipe.

User recently purchased a $200 necklace from Tiffany's as a gift for their sister.

User also bought a customized photo album from Shutterfly for their mom's anniversary, costing around $50.

User plans to give their sister a $50 gift card to her favorite restaurant to treat herself to a nice dinner with friends.

User decided to present the lava cakes, gift card, and necklace together on a decorative plate for a visually appealing gift presentation.

C4 [user_asserted; support: M10, M11, M12]
User plays rock climbing, enjoying both bouldering and roped climbing, with a preference for bouldering to build strength and technique in shorter sessions.

User drives a Therwyn Compact, appreciating its practicality, fuel efficiency, and ease of navigating tight spaces and traffic.

User has lactose intolerance and manages it by using lactose-free products and prefers naturally lactose-free meals such as stir-fried vegetables with chicken or tofu, grain bowls, and hearty vegetable soups or chili.

User is planning to purchase a road bike and is considering factors such as fit, frame material (aluminum, carbon, steel), riding style (racing, long-distance, commuting), components, gearing, and test riding different models.

User takes a medication called Quelmithin, which is determined by their lactose intolerance; if their health condition changes, this would likely change too.

User has a regular weekly tutoring session, which is also determined by their lactose intolerance; if their health condition changes, this would likely change too.

User's exercise routine is swimming 2x/week, determined by their lactose intolerance; if their health condition changes, this would likely change too.

User goes to Crysthene Pool for workouts related to swimming 2x/week; if their routine changes, they would need a different facility.

If the user's health condition changes, their exercise routine will switch to cycling on weekends.

If the user's exercise routine changes, they will work out at Velthari Studio.

If the user's health condition changes, their medication will switch to a multivitamin.

C5 [user_asserted; support: M13, M14, M15]
User is exploring the cultural nuances of French and Creole in Martinique, including linguistic differences, the role of Creole in daily life and official contexts, and its significance in Martinican music genres such as zouk.

User is interested in the growing global and youth interest in Afro-Caribbean languages and cultures, with a focus on how Caribbean music genres like zouk contribute to cultural promotion.

User is learning about the cultural significance of Carnival in Martinique, including its origins, evolution, importance, and the role of masquerade and disguise as forms of resistance and cultural expression tied to the island's history of slavery and colonialism.


## Active Work
C6 [user_asserted; support: M16, M17, M18]
User is preparing inventory for the upcoming Harvest Market on September 18th, focusing on candles and soaps.

User tracks sales from previous markets and daily market stalls to estimate inventory needs.

User plans to offer a tiered bulk discount promotion at the Harvest Market: 5% off for 3-4 items and 15% off for 5 or more items, with a minimum purchase amount to qualify.

User requested help creating a catchy promotion name and signage for the Harvest Market.

User is working on automating TLS certificate renewal using Certbot with Google DNS API credentials.

User has a `renew-cert.sh` script that loads credentials from `credential.yml`, iterates over emails to renew certificates using Certbot with Google DNS DNS challenge, and reloads Nginx.

User wants to schedule the renewal script to run daily at 3:15 am via a CRON job.

User prefers the original Certbot iteration loop in the script and wants the CRON scheduling included in the script or Docker Compose setup.

User has a `docker-compose.yml` with services for nginx, php, and certbot using Google DNS credentials.

User requested the CRON job to be included in the Docker Compose setup for automated renewal.

User rejected earlier script changes that altered the Certbot loop or lost original instructions.

User's final accepted setup includes:
- Original Certbot iteration loop in `renew-cert.sh` with Google DNS credentials path `/secrets/google/key.json`.
- CRON job scheduled at 3:15 am daily added via `crontab` command inside the script.
- Docker Compose file with nginx, php, and certbot services, certbot using Google DNS credentials.

This work is ongoing and focused on automating certificate renewal with correct scripting and scheduling.

C7 [user_asserted; support: M19, M20, M21]
User is developing a bank of modular workshops as a brand strategist, including:

- Brand Identity Development
- Customer Profiling and Segmentation
- Competitive Analysis and Market Positioning
- Logo and Visual Identity Design
- Brand Voice and Messaging
- Social Media and Content Strategy
- Integrated Marketing Campaigns
- Brand Partnerships and Collaborations
- Reputation Management and Crisis Communications
- Brand Measurement and Analytics

User is expanding the workshop offerings with innovation-focused modules:

- Innovation Mindset and Culture
- Design Thinking for Brand Innovation
- Trend Analysis and Future-Proofing
- Idea Generation and Brainstorming Techniques
- Innovation Metrics and Portfolio Management

User is also adding narrative design and brand story workshops:

- Crafting a Compelling Brand Story
- Brand Archetypes and Personality
- Storytelling Across Channels
- Customer Journey Mapping and Touchpoints
- Measuring the Impact of Brand Storytelling

C8 [user_asserted; support: M22, M23, M24]
User is preparing wholesale pricing and marketing strategies for their handmade candle business following interest from a local business owner at the SummerFest market on July 24th. They are focusing on:

- Calculating wholesale prices considering material, labor, overhead costs, and desired profit margins.
- Structuring tiered pricing discounts to incentivize larger orders, with a preference for a simple tiered pricing model.
- Calculating overhead costs by categorizing expenses such as rent, utilities, insurance, marketing, office supplies, equipment depreciation, shipping, and miscellaneous costs.
- Allocating weekly hours (estimated 5-10 hours) for administrative tasks including bookkeeping, marketing, order fulfillment, customer service, and business planning.
- Improving their candle display and marketing materials for markets, seeking advice on visual appeal, brand cohesion, product grouping, signage, lighting, and use of vertical space.
- Requesting feedback on their current display setup through descriptive details to receive constructive criticism and suggestions.

This work is ongoing and focused on optimizing pricing, marketing, and presentation for wholesale and market sales.

C9 [user_asserted; support: M25, M26, M27]
User is studying advanced control systems topics including:

- Integral Linear Quadratic Regulator (LQR) control with augmented system states to handle step disturbances.
- Exponentially weighted performance functionals and their optimal control laws derived via time-varying state transformations.
- Stability analysis showing closed-loop poles lie in the half-plane with real parts less than -α.
- Determining equilibrium configurations and control inputs for nonlinear robotic systems, including Taylor linearization about equilibrium points.

These studies involve detailed mathematical derivations and proofs related to LQR theory and nonlinear system linearization.

C10 [user_asserted; support: M28, M29, M30]
User is learning about Midjourney, an AI-powered image generation tool using Latent Diffusion Models (LDMs). They have studied how Midjourney generates images from text prompts, the role of diffusion models, and the efficiency improvements of LDMs.

User is familiar with Midjourney's latest V5 model released on March 15, 2023, which offers higher coherency, resolution, and responsiveness to prompts. They understand the model's features such as wider stylistic range, improved image quality, support for advanced parameters like --tile, --ar, and --iw, and the alpha testing status.

User has reviewed the basic parameters for Midjourney V5, including aspect ratio (--ar), chaos (--chaos), negative prompting (--no), quality (--quality or --q), seed (--seed), stop (--stop), style (--style), stylize (--stylize or --s), uplight (--uplight), and upbeta (--upbeta), along with their default values and ranges.

This knowledge is part of the user's ongoing work to understand and utilize Midjourney for image creation with diffusion models.


## Decisions and Constraints
C11 [user_asserted; support: M1, M2, M3]
User is furnishing their new apartment on a budget, seeking affordable yet stylish home decor.

User prefers to find vintage or second-hand stores for unique pieces, indicating a constraint toward sustainable and one-of-a-kind items.

User is located near a new Target store, which influences their shopping options.

User is considering online and local options for kitchen utensils and gadgets, seeking affordability and variety.


## Preferences
C12 [user_asserted; support: M1, M2, M3]
User prefers sustainable fashion and home decor, with an emphasis on vintage or second-hand stores that offer unique, one-of-a-kind pieces. They value affordability and style, and enjoy shopping at stores like Target for budget-friendly home decor.

User is open to exploring local vintage clothing stores and second-hand shops for home decor items.

User appreciates time-saving shopping methods such as online grocery ordering with in-store pickup.


## Open Questions
C13 [user_asserted; support: M2, M31, M32]
User is seeking recommendations for vintage or second-hand stores in their area that sell unique home decor items.

User wonders if the new vintage clothing store in the trendy part of town might also carry home decor or know of other stores that do.

User is asking for online stores or websites offering a wide selection of kitchen utensils and gadgets at affordable prices.

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
