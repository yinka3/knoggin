# Project Context

## Current State
C1 [user_asserted; support: M1, M2, M3]
The user is working with FortiGate devices and VPN technologies. They have asked about VPN concepts, IPSec tunnel interface creation issues, FortiGate commands for changing FortiGuard listening port, setting up HA in active-passive mode on two FortiGate units, and updating the system time on FortiGate devices.

C2 [agent_derived; support: M4, M5, M6]
The team uses Synthari Elements v1.2 design system, Kerberos Trove and Pyronos Crypt secret managers (now Orinthal Sealed), and Verithos Auth with a 4-hour session TTL. They follow GitFlow with release branches and emphasize small, focused branches with regular merges. They use pytest for testing with a 70% overall coverage target and 95% for authentication components. Production alerts go to #aurora-oncall-alerts with actionable thresholds. The build tool is Dranith Compiler with optimization considerations, and Veltrion Pkg manages dependencies with semantic versioning and lock files. The assistant corrected a prior algorithmic complexity explanation and apologized for the mistake.

C3 [user_asserted; support: M7, M8, M9]
The team's test coverage target is 70% overall, with a focus on 95% coverage for authentication components. They use pytest as their testing framework and are interested in tools and plugins to monitor test coverage effectively with minimal overhead.

The team uses Pyronos Crypt as their secret manager to secure sensitive information and manage secrets across their environment. They aim to integrate secret management into deployment pipelines by injecting secrets at runtime, enforcing access controls, rotating secrets regularly, and auditing access logs.

The login endpoint for SAML-based authentication is `/cerberix/saml/sso-init`. The team emphasizes securing this endpoint with HTTPS, validating SAML assertions, managing short-lived authentication tokens with refresh tokens, and implementing secure session management practices.

Production alerts are sent to the `#aurora-oncall-alerts` channel. The team focuses on making alerts actionable by setting clear thresholds and conditions, filtering false positives, categorizing alerts by severity, and continuously refining alerting strategies based on monitored metrics such as error rates, response times, and resource utilization.

C4 [user_asserted; support: M10]
The user asked about use cases for a domain registrar to utilize GPT-4, prioritizing growth and optimal customer experience (CX). They requested the top 3 use cases with example code for GPT API usage.

The assistant responded with three prioritized use cases:

1. Automated support assistants to answer FAQs and improve customer satisfaction.
2. Domain name suggestions and evaluations based on user input.
3. Marketing and sales content creation such as blog posts and email campaigns.

For each use case, the assistant provided Python code examples using the OpenAI GPT API to implement the functionality.

The user was instructed to replace "your_api_key" with their actual API key to run the examples.

C5 [user_asserted; support: M11, M12, M13]
The team's secret manager has changed to Orinthal Sealed for securely managing sensitive information. The team plans to avoid hardcoding secrets, access them at runtime with proper permissions, and implement rotation policies for enhanced security.

The build tool in use is Dranith Compiler. The team is considering optimization flags such as parallel builds, incremental compilation, caching mechanisms, and enabling strict warnings to improve build performance and reliability.

The package manager used is Veltrion Pkg. The team focuses on regularly updating dependencies, using lock files for consistent installs, following semantic versioning, and managing version ranges to handle conflicts effectively.

C6 [user_asserted; support: M14, M15, M16]
The user is experiencing complex emotions related to romantic feelings and friendships during college. They have had an infatuation that was not reciprocated and are now developing feelings for another girl with whom they share common interests and study plans. The user is planning to confess their feelings after college but is uncertain whether these feelings are love or infatuation. They seek advice on how to express their feelings, the potential impact on their friendship, and how to cope with possible rejection. The user also reflects on the emotional instability often associated with love and the possibility of maintaining friendship after rejection. They have discussed communication methods for confession, including face-to-face and phone calls, and are navigating doubts and emotional intensity related to their feelings.

The user prefers explanations and commands in French but has communicated this topic in English.

This emotional and relational context is important for providing empathetic, respectful, and supportive guidance tailored to their situation.

C7 [user_asserted; support: M17, M18, M19]
The user is a Facebook account holder who has used Facebook's "Download Your Information" feature but finds it incomplete for cloning their account. They inquired about writing a Facebook app or plugin to completely clone their Facebook account, including friends' data, assuming compliance with Facebook's Terms of Service. The user clarified that their mention of "hacking" was tongue in cheek and not an intent to engage in wrongdoing. They appreciate clear guidance on the limitations and policies regarding Facebook data access and cloning.

C8 [agent_derived; support: M20, M21, M22]
The assistant explained that it is not possible to completely clone a Facebook account using the Facebook Developer API because the API restricts access to certain data such as private messages and friends' data. Attempting to clone an account in this way would likely violate Facebook's Terms of Service and could be illegal. The assistant recommended using Facebook's "Download Your Information" feature as the best available method to obtain most personal data, while noting its limitations. The assistant also clarified that the Facebook Developer Community focuses on app development, not hacking or cloning accounts, and warned against discussing hacking in that community.

C9 [user_asserted; support: M23, M24]
The user asked about the competing arguments supporting the continuation of the UK's national sugary drinks industry levy (SDIL) implemented in 2018, focusing on its effectiveness in reducing free sugars consumption, improving health, and its impact on inequalities. The assistant provided a paraphrased 300-word summary including numerical evidence: a 5.1% reduction in sugary drink sales volume in the first year and a 9.3% reduction in consumption among low-income households. Arguments for continuation highlight reduced sugary drink consumption and potential health benefits, especially for disadvantaged groups. Arguments against note the regressive nature of the tax and concerns about sustainability and tax avoidance behaviors. The assistant emphasized weighing benefits and drawbacks when considering policy continuation or expansion.

C10 [user_asserted; support: M25, M26, M27]
The user's authentication method is currently API Key + HMAC, managed by Verithos Auth. If the auth provider changes, the authentication method and related middleware will also change.

Current token format: Zenthos-Session-v1.

Current auth middleware: phalanx-hmac-validator v2.0.

Planned possible future auth method: mTLS Client Certificate.

Planned middleware for mTLS: oathkeeper-mtls-proxy v4.1.

An exact error log to remember:
"ConnectionRefusedError: [Errno 111] Connection refused.\nService crysthene-db-primary on port 5432 is not accepting connections.\nLast successful heartbeat was at 2024-03-15T08:23:41Z.\nCheck firewall rules and verify that max_connections has not been exceeded."

C11 [user_asserted; support: M28, M29, M30]
The user is exploring ways to raise funds for an AEC-oriented software services company that is already profitable but facing a cash runway issue due to the loss of an important client contract ending soon. They have considered actions such as cost reduction, seeking short-term loans or lines of credit, diversifying the client base, raising additional capital, and negotiating contract extensions.

The company currently offers services including process automation, 3D viewers, consulting, data processing, software development, interoperability solutions, and data visualization. They want to focus on innovative services for customers who have already adopted BIM, such as BIM support and training, BIM implementation, BIM data management, BIM integration with other systems, and BIM-based custom solutions.

To balance their services, they are considering adding software products such as BIM software (stand-alone or plugins), project management software tailored to AEC needs, data management software for BIM data, collaboration and communication tools, and custom applications tailored to client requirements.

This context reflects the user's current business situation, service portfolio, innovation focus, and product expansion considerations.

C12 [agent_derived; support: M31]
ChatGPT for Google refers to using the OpenAI ChatGPT language model integrated with Google Cloud Platform services to enable natural language processing tasks such as chatbots, voice assistants, and other conversational AI applications. It leverages deep neural network architectures trained on large text corpora to generate human-like responses, improving user interaction and automation capabilities.

C13 [agent_derived; support: M32, M33]
To use ChatGPT with Google Cloud Platform, a user typically needs to:

1. Create a Google Cloud Platform account.
2. Set up a project in the Google Cloud Console.
3. Enable the AI Platform API.
4. Create a new instance of ChatGPT or a conversational AI model.
5. Connect to the instance via Google Cloud SDK or API endpoints.

Alternatively, Dialogflow can be used as a visual tool to create chatbots that integrate ChatGPT responses by designing intents, training the model, and testing the chatbot.

This process requires some programming knowledge and access to Google Cloud services.

This explanation is simplified for clarity and beginner understanding.

C14 [agent_derived; support: M34, M35]
There are many online resources to learn about using ChatGPT with Google Cloud Platform, including:

- Google Cloud official YouTube channel
- TensorFlow YouTube channel
- Sentdex YouTube channel
- Siraj Raval YouTube channel
- Google Cloud Platform documentation website
- TensorFlow official website
- Online learning platforms like Coursera and Udemy
- Medium articles by industry experts

These resources provide tutorials, code samples, and guides for beginners and advanced users.


## Active Work
C15 [user_asserted; support: M36]
La metodología de trabajo propuesta se basa en un monorepo centralizado que contiene aplicaciones, librerías y paquetes, con el objetivo principal de fomentar la cooperación entre departamentos y compartir código e implementaciones.

Se adopta un modelo de Trunk Based Development con prehooks que impiden subir código que no pase compilación, tests y validación. La rama principal mantiene integración continua en un entorno de staging. En lugar de ramificaciones tradicionales, se usan flags de características, integración y abstracción para gestionar el desarrollo.

Se aplica Atomic Design para dividir los elementos en componentes atómicos, moleculares y orgánicos, optimizando la reutilización y mantenibilidad del código.

Para mejorar la productividad del desarrollador, se eliminan puntos de dolor mediante ergonomía de desarrollo, debuggers, trazabilidad, pair programming, code reviews, uso de docker para un stack común y herramientas de desarrollo como visores de bases de datos y debuggers de código servidor.

Se gestiona la deuda técnica registrándola con tickets y comentarios en el código, estableciendo procedimientos y tiempos para su pago, asegurando la sostenibilidad del proyecto.

C16 [user_asserted; support: M37, M38, M39]
The user is exploring project ideas in less researched areas of deep learning suitable for college student teams aiming to create projects and publish research papers. They have reviewed ideas including multi-modal deep learning, adversarial attacks and defenses, deep reinforcement learning in robotics, explainable AI, deep generative models, deep transfer learning, deep neural architecture search, and deep unsupervised representation learning.

They have also considered use cases for these areas, such as sentiment analysis on multi-modal data, fraud detection, robot navigation and manipulation, transparent healthcare predictions, realistic image generation, transfer learning for scarce data domains, neural architecture optimization, and unsupervised representation learning for clustering and visualization.

Additionally, they have identified example datasets for these use cases, including AI-CC, IEMOCAP, MOSI, Fraud Detection datasets, Udacity Self-Driving Car dataset, MIMIC-III, COCO, ImageNet, CIFAR-10/100, MNIST, and others.

This exploration is part of their active work on deep learning research project ideation and dataset selection.

C17 [agent_derived; support: M40, M41, M42]
The user found their Django email verification insecure because it encodes usernames in base64 without secure tokens or expiration, allowing attackers to generate valid verification links for any user. The assistant demonstrated how to exploit this and recommended using secure tokens with expiration to prevent unauthorized account activation.

C18 [user_asserted; support: M43, M44]
The user is developing an online book store project with a Java backend using the MVC model and an Angular frontend. They have requested and received an outline for the project structure, including backend design, frontend setup, and deployment steps.

The user has started coding with the first part: designing the database schema. The schema includes tables for books, authors, publishers, customers, orders, and order items, with appropriate fields and foreign key relationships. The schema covers book details, author and publisher information, customer data, order records, and order item details.

This schema serves as the foundational data model for the backend implementation of the online book store project.

C19 [user_asserted; support: M45]
Voici un script Ruby corrigé et fonctionnel qui liste tous les fichiers YAML dans un dossier et ses sous-dossiers, et affiche uniquement ceux qui **n'ont pas** la propriété `template.deleted` à `true` :

```ruby
require 'yaml'

def file_without_deleted_template?(file_path)
  yaml_data = YAML.load_file(file_path)
  # Retourne true si :
  # - la clé 'template' existe
  # - et soit 'deleted' n'existe pas dans 'template', soit sa valeur n'est pas true
  yaml_data.key?('template') && yaml_data['template']['deleted'] != true
rescue Psych::SyntaxError, Errno::ENOENT => e
  # En cas d'erreur de parsing ou fichier inaccessible, on considère que le fichier ne correspond pas
  false
end

def list_files_without_deleted_template(folder_path)
  Dir.glob("#{folder_path}/**/*.{yml,yaml}").each do |file_path|
    if file_without_deleted_template?(file_path)
      puts file_path
    end
  end
end

# Exemple d'utilisation :
list_files_without_deleted_template('/chemin/vers/le/dossier')
```

Ce script :
- Parcourt récursivement tous les fichiers `.yml` et `.yaml` dans le dossier donné
- Charge chaque fichier YAML
- Vérifie si la clé `template` existe et que `template.deleted` n'est pas `true`
- Affiche le chemin des fichiers qui correspondent à ce critère

Vous pouvez remplacer `/chemin/vers/le/dossier` par le chemin réel de votre dossier.


## Decisions and Constraints

## Preferences
C20 [user_asserted; support: M1, M46]
The user prefers explanations and commands in French, with some requests for English translation.


## Open Questions
C21 [user_asserted; support: M47]
El usuario quiere permitir que se imprima un documento PDF en línea con una impresora, en papel, pero sin permitir que se pueda descargar el documento.

C22 [agent_derived; support: M48]
El asistente explicó que es posible permitir la impresión de un PDF en línea sin permitir su descarga usando un visor de PDF personalizado (como PDF.js) con opciones para desactivar la descarga y guardar, reforzando restricciones en el servidor con tokens de autenticación temporales, ofuscación de rutas y limitación de accesos, y habilitando la impresión. Se advirtió que no hay garantía absoluta de protección contra usuarios avanzados.

Support handles:
- M1: user_message
- M2: user_message
- M3: user_message
- M4: assistant_message
- M5: assistant_message
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
- M26: user_message
- M27: user_message
- M28: user_message
- M29: user_message
- M30: user_message
- M31: assistant_message
- M32: assistant_message
- M33: assistant_message
- M34: assistant_message
- M35: assistant_message
- M36: user_message
- M37: user_message
- M38: user_message
- M39: user_message
- M40: assistant_message
- M41: user_message
- M42: user_message
- M43: user_message
- M44: user_message
- M45: user_message
- M46: user_message
- M47: user_message
- M48: assistant_message
