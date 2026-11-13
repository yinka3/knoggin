# Project Context

## Current State
C1 [user_asserted; support: M1, M2, M3]
Taipei is recognized for its increasing accessibility for individuals with disabilities or limited mobility. Key accessible spots for viewing the Taipei skyline include Taipei 101 observatory (with ramps, elevators, accessible restrooms, and wheelchair rentals), Elephant Mountain Trail (paved paths and resting areas), National Palace Museum (fully accessible), Maokong Gondola (wheelchair accessible gondola and facilities), Tamsui Ferry (wheelchair ramps and accessible seating), and Yangmingshan National Park (some accessible trails and restrooms).

Several landmarks and attractions in Taipei are disability-friendly, such as National Chiang Kai-shek Memorial Hall, Taipei Fine Arts Museum, Taipei Zoo, Taipei City Hall, and Beitou Hot Springs, all featuring ramps, elevators, accessible restrooms, and other accommodations.

Accessible restaurants and food markets include Din Tai Fung, Shilin Night Market, Addiction Aquatic Development, Modern Toilet Restaurant, and Ay-Chung Flour-Rice Noodle, many with wheelchair ramps and accessible restrooms.

Taiwan has enacted several laws and regulations promoting accessibility, including the Act of the Promotion of Persons with Disabilities Rights, Public Enterprise Act, Barrier-Free Environment Construction Act, Enforcement Rules of Buildings with Barrier-Free Facilities, and Transportation Services for People with Disabilities Act, demonstrating governmental commitment to accessibility improvements.

Other cities known for disability-friendly environments include London, Toronto, Melbourne, and Berlin, each with accessible public transportation, buildings, and legal frameworks supporting accessibility.

C2 [user_asserted; support: M4]
User drives a Quorvel EV, having switched to an electric vehicle appreciating its efficiency and reduced environmental impact.

C3 [user_asserted; support: M5]
User has tendinitis, which poses challenges for staying active, especially running. They manage it by incorporating rest, low-impact activities, stretching, strengthening exercises, and possibly consulting specialists.

C4 [user_asserted; support: M6]
User subscribes to Xelvoria, a platform providing health and fitness content helpful for managing running and tendinitis issues.

C5 [user_asserted; support: M7, M8, M9]
User is aware of various negative consequences of social media on interpersonal communication, including misinterpretation, cyberbullying, public humiliation, political polarization, and impersonal communication. Despite these issues, user recognizes social media's continued popularity due to convenience, entertainment, networking, business opportunities, and information sharing. User expresses concern about social media addiction and its impact on mental health and daily life.

C6 [agent_derived; support: M10, M11, M12]
Malintzin was born into a noble family in the Gulf Coast region of Mexico and was taken captive by the Aztecs as a young woman. She was later given to Spanish conquistador Hernán Cortés as a gift and interpreter, fluent in Nahuatl and Mayan. She used her intelligence and cultural knowledge to serve as an advisor and mediator between Aztec and Spanish groups, helping build alliances and communicate broadly.

Malintzin married Don Juan Jaramillo, a Spanish man, possibly as a strategic move to increase her power and influence in colonial society, gain access to Spanish resources, and demonstrate loyalty to the Spanish. However, it is also possible she was forced into the marriage, as Spanish accounts often portrayed indigenous women as subservient. The exact circumstances remain unclear, but her marriage reflected complex political, social, and economic factors.

Malintzin's legacy is significant in Mexican history as a symbol of the intersection of Aztec and Spanish cultures and the broader pattern of intermarriage during the colonial period, which influenced Mexican cultural identity.

C7 [user_asserted; support: M13, M14, M15]
User is part of a book club that reads a different book each month and discusses it. They prefer a mix of genres, leaning towards contemporary fiction and thrillers, and appreciate books with complex characters and moral dilemmas that spark intense discussions. They consider balancing books that align with members' interests and those that challenge them with new themes.

User recently switched to driving a Vorantel SUV, appreciating its spaciousness and features for comfortable long drives.

User has a weekly tutoring session that they find beneficial for reinforcing studies and improving academic performance.

User attends a monthly social meetup brunch with college friends, valuing it as a way to maintain and strengthen friendships over time.

C8 [user_asserted; support: M16, M17]
Santa Cruz del Islote is a small island in the Archipelago of San Bernardo, Colombia, with about 1,200 residents living on approximately 1.2 acres. The island is one of the most densely populated in the world.

Living conditions are basic, lacking paved roads, sewage systems, and reliable access to clean water. Houses are closely built from wood with metal roofs. The island has only one store and one school; residents travel off-island for medications and other supplies.

The local economy centers on fishing, with some involvement in tourism and boat repair. Despite limited infrastructure and environmental vulnerabilities like rising sea levels and erosion, the community is close-knit and proud of their home.

Long-term sustainability is uncertain due to resource constraints and environmental risks. Some residents may migrate for economic or personal reasons, but many remain committed to sustaining life on the island. Efforts to improve conditions include solar panels, rainwater collection, and government investments in infrastructure such as a new school and basic services.

C9 [user_asserted; support: M18, M19, M20]
User has a low-sodium dietary restriction determined by their tendinitis; if their health condition changes, this dietary restriction would likely change too.

User's exercise routine is Pilates 3 times per week, determined by their tendinitis; if their health condition changes, this routine would likely change.

User works out at Zorathel Peak Gym for Pilates 3 times per week; if their routine changes, they would need a different facility.

If the user's health condition changes, their exercise routine will change to yoga 2 times per week.

If the user's exercise routine changes, they will work out at Crysthene Pool.

If the user's health condition changes, their dietary restriction will change to no alcohol.


## Active Work
C10 [user_asserted; support: M21, M22, M23]
This paper introduces a decentralized application designed for the long-term storage of EBSI-issued verifiable credentials, addressing key challenges in decentralized systems. Trust is a crucial element in decentralized and distributed systems, as traditional frameworks like Public Key Infrastructure (PKI) are insufficient for establishing a robust trust foundation. As the world transitions from conventional signed and handwritten documents to digital alternatives, the demand for secure, long-term storage solutions becomes increasingly critical.

Verifiable credentials (VCs) are digital credentials that can be cryptographically verified and authenticated. They enable issuers to grant credentials to holders, who can present them to verifiers as proof of identity, qualification, or other attributes. The W3C Verifiable Credentials Data Model standard (<https://www.w3.org/TR/vc-data-model/>) provides a foundation for creating and managing VCs in a privacy-preserving and secure manner.

Building on this foundation, the European Blockchain Service Infrastructure (EBSI) is a decentralized platform fostering secure cross-border digital public services. A vital aspect of EBSI is the use of verifiable credentials, which establish trust and safeguard users' privacy within the ecosystem. However, decentralized systems face several challenges, and this paper specifically addresses the following:

1. Long-term storage: Designing reliable and efficient long-term storage solutions for EBSI-issued verifiable credentials, ensuring their availability and integrity over extended periods.
2. Privacy preservation: Developing methods that protect the privacy of verifiable credential holders while maintaining system trust and security.
3. Integration with TR-ESOR: Implementing the TR-ESOR middleware (<https://www.bsi.bund.de/SharedDocs/Downloads/DE/BSI/Publikationen/TechnischeRichtlinien/TR03125/BSI_TR_03125_V1_2_1.pdf?__blob=publicationFile&v=2>) as a long-term storage solution for EBSI-issued verifiable credentials, exploring feasibility and benefits.

The integration of TR-ESOR with various storage systems, including cloud, local, and decentralized databases, enhances the flexibility and adaptability of the long-term storage solution. This paper provides a comprehensive introduction to the EBSI framework, the W3C Verifiable Credentials Data Model, and the TR-ESOR recommendations, while addressing the challenges associated with preserving verifiable credentials.

In alignment with ACM and IEEE guidelines for influential computer science research papers, this work employs state-of-the-art scientific research methodologies to advance digital credential management and strengthen trust in decentralized systems.

C11 [user_asserted; support: M24, M25, M26]
User is planning to organize their garage and closet, aiming to declutter and donate items they no longer need. They found their grandmother's antique locket while cleaning their dresser, which was a relief. They intend to donate items to Goodwill and are seeking advice on the donation process, including tax deduction claims and receipt handling. User also plans to create a cozy reading nook in their closet, seeking suggestions on seating, lighting, decor, and personalization to enhance comfort and ambiance.

C12 [user_asserted; support: M25, M26]
User is seeking detailed guidance on claiming tax deductions for charitable donations to Goodwill, including the importance of keeping receipts, itemizing donations, filing forms 1040 and Schedule A, and understanding deduction limits. They inquire about combining receipts for multiple donations and how to value donated items such as shoes.

C13 [user_asserted; support: M27]
User is organizing their closet and seeks advice on deciding what clothes to keep, donate, or discard. They are interested in methods such as the hanger trick, the 80/20 rule, asking reflective questions about usage and condition, categorizing items, and maintaining a 'maybe' box for uncertain items.

C14 [user_asserted; support: M28]
User plans to create a cozy reading nook in their closet and seeks suggestions on seating options, lighting, textiles, plants, color schemes, organization of reading materials, tech-friendly features, and personalization to enhance comfort and ambiance.

C15 [user_asserted; support: M29]
User is considering adding decorative items like framed photos or artwork to their reading nook and seeks advice on selecting pieces that fit a cozy atmosphere, including color palette, emotional resonance, texture, scale, and personalization.

C16 [user_asserted; support: M30, M31, M32]
User is conducting an interactive interview exercise to assess their familiarity with obscure historical topics. The first topic was the Fire-Eaters, a pro-slavery extremist group in 19th-century United States politics, with the user showing partial knowledge (estimated 30% familiarity). The second topic was the Voynich Manuscript, a mysterious medieval European manuscript with undeciphered text and unique illustrations, with the user showing moderate knowledge (estimated 40% familiarity). The interview format involves asking one question at a time, avoiding revealing information, and providing a final familiarity assessment after all questions.


## Decisions and Constraints

## Preferences
C17 [user_asserted; support: M33]
User's life philosophy is: "We are what we repeatedly do. Excellence, then, is not an act, but a habit. The whole is greater than the sum of its parts. We must become the change we wish to see in the world."


## Open Questions
C18 [user_asserted; support: M34, M35, M36]
Common challenges Best Way engineers face when troubleshooting product issues include lack of information, product complexity, limited access to data sources, time constraints, unforeseen issues, customer satisfaction concerns, and the need for deep technical knowledge.

Methods used by Best Way engineers to effectively solve these issues involve gathering relevant data, breaking down problems into smaller parts, using diagnostic tools, collaborating with other teams, testing solutions, and continuously learning and improving.

To balance technical solutions with customer expectations, Best Way engineers define problems with customer impact in mind, set realistic customer expectations through communication, focus on customer feedback to prioritize issues, find trade-offs between technical feasibility and customer satisfaction, and continuously iterate solutions based on feedback and data.

C19 [user_asserted; support: M37, M38]
User is seeking advice on how to set boundaries with relatives effectively without coming across as rude or distant, and how to get relatives to understand these boundaries without being overly aggressive.

C20 [user_asserted; support: M39, M40, M41]
User is interested in understanding the different legal arguments and claims made by countries involved in territorial disputes, including the South China Sea, Israel-Palestine, and Kashmir conflicts. They recognize the complexity and multifaceted nature of these disputes, noting the variety of historical, cultural, economic, and strategic interests involved. The user expresses hope for peaceful resolution mechanisms and cooperation among parties.

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
- M10: assistant_message
- M11: assistant_message
- M12: assistant_message
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
- M36: user_message
- M37: user_message
- M38: user_message
- M39: user_message
- M40: user_message
- M41: user_message
