# Project Context

## Current State
C1 [user_asserted; support: M1, M2, M3]
The user is writing an article about Clause 23 (Notices) in the HIA NSW Lump Sum Contract. They have provided the full text of Clause 23 and requested various analyses and tables about the clause's practical purpose, risks to builders and owners, and common disputes regarding service of notices under residential building contracts in NSW. The user is a solicitor and construction lawyer with over 10 years of experience advising homeowners and builders in NSW. The target audience for the article is homeowners and builders needing awareness of proper service and response to notices under the HIA NSW Lump Sum Contract. The user has also requested 10 article topic headings relevant to the target audience and the search phrase "Service of Notices under the HIA NSW Lump Sum Contract."

C2 [user_asserted; support: M4, M5]
The team uses Zelthra Lib as their package manager, having recently switched to it to streamline dependency management.

The database in use is Pyravar Data for data storage needs.

The team's code review policy requires 1 approval plus a green CI status before merging, ensuring vetted and stable code changes.

C3 [user_asserted; support: M6, M7, M8]
The user is requesting a synopsis of "I, Robot" by Isaac Asimov, including a summary of the book and synopses of two specific stories, "Robbie" and "Reason." They also requested an imaginary interview with the character Robbie from the story "Robbie." The assistant has provided these summaries and the interview text.

C4 [user_asserted; support: M9, M10, M11]
The project uses Pyravar Mapper as the ORM, which depends on the Pyravar Data database. If the database changes, the ORM and related model definitions will also need to change accordingly.

Database backups currently run daily at midnight KST, tied to Pyravar Data. Changing databases will require adjusting backup schedules and processes.

Database migrations are managed with Zyndra Alter, which depends on the database in use. A database change will require verifying Zyndra Alter compatibility and migration syntax.

Models are defined using `Pyravar.record('Model', schema)`. Changing the ORM library will require adapting model definitions to the new syntax.

If the ORM changes to Keldaris Link, model syntax will change to `Keldaris.node('Model', shape)`, requiring familiarity with Keldaris-specific features and syntax.

Backup schedules will shift to daily at 3am UTC if the database changes.

An exact error log to remember word for word is:

```
OutOfMemoryError: Java heap space.
GC overhead limit exceeded after 8 consecutive failed collections in pool 'PS Old Gen'.
Heap dump written to /var/log/aurora/heap-2024-05-08.hprof at 18:42:11Z.
Recommend increasing -Xmx beyond 4G or investigating memory leak in OrderProcessor.
```

This error indicates memory issues requiring heap size increase or memory leak investigation in the `OrderProcessor` component.

C5 [user_asserted; support: M12]
The user requested a LinkedIn post based on detailed information about RedShred, a document understanding platform that transforms documents into structured, usable data. The platform is API-first, supports low-code/no-code solutions, and uses NLP and computer vision to extract, enrich, and reshape document content into searchable and reusable formats. It handles various document types including scanned images, PDFs, and spreadsheets, and is designed to accelerate document-centric processes by automating data extraction and enrichment. The user received a LinkedIn post draft highlighting RedShred's capabilities, ease of integration, and its use by notable organizations such as DARPA, NSF, and the U.S. Air Force. The post emphasizes RedShred's 3-stage process: Extract, Enrich, and Reshape, and its ability to convert static documents into interactive applications with semantic search and metadata linking.


## Active Work
C6 [user_asserted; support: M13]
The user is planning a combined 90-minute lesson alternating between Year 12 and Year 13 students. Year 12 students will be taught Level 2 NCEA coordinate geometry methods, assessed in four weeks by the achievement standard "apply coordinate geometry methods in solving problems." Year 13 students will be taught Level 3 NCEA multivariate analysis, focusing on the achievement standard on multivariate data. The lesson plan involves alternating instruction and work periods between the two groups, with the user introducing topics and checking work in turn.

A detailed list of topics for the Year 13 Level 3 multivariate analysis introductory lesson includes:

I. Introduction to Multivariate Analysis
  A. Definition and Explanation
  B. Purpose and Importance
  C. Overview of Key Techniques

II. Scatterplots and Correlation
  A. Explanation and Interpretation
  B. Understanding Correlation Types
  C. Practice Problems

III. Linear Regression
  A. Definition and Explanation
  B. Performing Linear Regression
  C. Understanding Regression Line
  D. Practice Problems

IV. Multiple Regression
  A. Definition and Explanation
  B. Performing Multiple Regression
  C. Understanding Coefficients
  D. Practice Problems

V. Hypothesis Testing
  A. Explanation
  B. Performing Tests
  C. Understanding P-values
  D. Practice Problems

VI. Conclusion
  A. Summary of Key Concepts
  B. Review of Next Steps

This plan reflects the user's instructions and preferences for alternating teaching and differentiated content for the two year groups.

C7 [user_asserted; support: M14, M15]
The user is continuing to write a detailed article section on "V. Applications of Golang, A. System programming" with C++ example code illustrating concurrency and embedded systems programming. The content covers Golang's suitability for system programming, embedded systems, gaming, operating systems, and compilers/interpreters, including example C++ code snippets for concurrency and GPIO control on Raspberry Pi. The user also includes advantages and disadvantages of Golang and begins the conclusion section.

The user requested the continuation of the article in English with detailed markdown formatting and C++ code examples for the blog post.

The content reflects the user's instructions to produce a detailed, clear, and technical article section with practical code examples in C++.

This work is part of the user's ongoing article writing project on Golang applications, with a focus on system programming and related topics.

C8 [user_asserted; support: M16]
The user requested an explanation of the `craftingStart` method from the Thaumcraft addon for Minecraft, specifically related to the infusion matrix.

This method is called when a player starts crafting an item on the infusion matrix. It first checks if the matrix is in a valid location; if not, it deactivates the crafting process and updates the block state.

If valid, it gathers surrounding blocks and checks the pedestal below the matrix for the input item to be crafted. It then collects components from other pedestals around the matrix.

Using these components and the input item, it attempts to find a matching infusion recipe or an infusion enchantment recipe via `ThaumcraftCraftingManager`.

If a matching recipe is found, it sets up the crafting parameters including recipe type, ingredients, output, instability, essentia cost, and player name. It marks crafting as active, plays a crafting start sound effect, and updates the block state.

If no infusion recipe is found, it tries to find an enchantment recipe and similarly sets up crafting parameters if found.

The method ensures the infusion matrix block is updated and marked dirty to reflect the crafting state in the game world.


## Decisions and Constraints
C9 [user_asserted; support: M17, M18, M19]
The article will be authored by the user, a solicitor and construction lawyer with over 10 years experience specializing in construction law in NSW. The article's target audience is homeowners and builders in NSW who need to understand the proper service and response to notices under the HIA NSW Lump Sum Contract. The user will refer to the provided clause text and analyses as foundational information. The article should address common disputes and practical risks related to service of notices under the contract.


## Preferences
C10 [user_asserted; support: M3, M19]
The user prefers concise, clear tables summarizing practical risks, benefits, and common disputes related to Clause 23 Notices. The article should be accessible and helpful to homeowners and builders searching online about service of notices under the HIA NSW Lump Sum Contract. The user wants the article topics to reflect their expertise and the target audience's search intent.

C11 [agent_derived; support: M20, M21, M22]
The user prefers concise, clear tables summarizing practical risks, benefits, and common disputes related to Clause 23 Notices. The article should be accessible and helpful to homeowners and builders searching online about service of notices under the HIA NSW Lump Sum Contract. The user wants the article topics to reflect their expertise and the target audience's search intent.


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
