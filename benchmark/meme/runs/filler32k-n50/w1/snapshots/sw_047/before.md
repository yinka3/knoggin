# Project Context

## Current State
C1 [user_asserted; support: M1, M2, M3]
The user is working with FortiGate devices and VPN technologies. They have asked about VPN concepts, IPSec tunnel interface creation issues, FortiGate commands for changing FortiGuard listening port, setting up HA in active-passive mode on two FortiGate units, and updating the system time on FortiGate devices.

C2 [agent_derived; support: M4, M5, M6]
The team uses the design system Synthari Elements v1.2 and is working on integrating it with their components, focusing on consistency in styling and behavior by leveraging design tokens and wrapper components.

The team uses Kerberos Trove as their secret manager to handle sensitive information securely, emphasizing strict access control, regular secret rotation, and auditing access logs.

For authentication, the team uses Verithos Auth to manage user sessions securely, with a session TTL set to 4 hours. They are considering implementing session renewal and expiration handling mechanisms such as user notifications before expiration and seamless background session refresh.

The team employs GitFlow with release branches branching to manage releases and features systematically, focusing on small, focused branches, regular merges from develop to feature branches, and clear tagging of releases.

The assistant corrected a previous explanation about algorithmic complexity bounds: it is not possible for a problem to have a logarithmic upper bound and a linear lower bound simultaneously because logarithmic complexity is asymptotically lower than linear complexity. The assistant acknowledged the user's correction and apologized for the earlier mistake.

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


## Active Work
C12 [user_asserted; support: M31]
La metodología de trabajo propuesta se basa en un monorepo centralizado que contiene aplicaciones, librerías y paquetes, con el objetivo principal de fomentar la cooperación entre departamentos y compartir código e implementaciones.

Se adopta un modelo de Trunk Based Development con prehooks que impiden subir código que no pase compilación, tests y validación. La rama principal mantiene integración continua en un entorno de staging. En lugar de ramificaciones tradicionales, se usan flags de características, integración y abstracción para gestionar el desarrollo.

Se aplica Atomic Design para dividir los elementos en componentes atómicos, moleculares y orgánicos, optimizando la reutilización y mantenibilidad del código.

Para mejorar la productividad del desarrollador, se eliminan puntos de dolor mediante ergonomía de desarrollo, debuggers, trazabilidad, pair programming, code reviews, uso de docker para un stack común y herramientas de desarrollo como visores de bases de datos y debuggers de código servidor.

Se gestiona la deuda técnica registrándola con tickets y comentarios en el código, estableciendo procedimientos y tiempos para su pago, asegurando la sostenibilidad del proyecto.

C13 [user_asserted; support: M32, M33, M34]
The user is exploring project ideas in less researched areas of deep learning suitable for college student teams aiming to create projects and publish research papers. They have reviewed ideas including multi-modal deep learning, adversarial attacks and defenses, deep reinforcement learning in robotics, explainable AI, deep generative models, deep transfer learning, deep neural architecture search, and deep unsupervised representation learning.

They have also considered use cases for these areas, such as sentiment analysis on multi-modal data, fraud detection, robot navigation and manipulation, transparent healthcare predictions, realistic image generation, transfer learning for scarce data domains, neural architecture optimization, and unsupervised representation learning for clustering and visualization.

Additionally, they have identified example datasets for these use cases, including AI-CC, IEMOCAP, MOSI, Fraud Detection datasets, Udacity Self-Driving Car dataset, MIMIC-III, COCO, ImageNet, CIFAR-10/100, MNIST, and others.

This exploration is part of their active work on deep learning research project ideation and dataset selection.

C14 [agent_derived; support: M35, M36]
The user found their Django email verification insecure because it encodes usernames in base64 without secure tokens or expiration, allowing attackers to generate valid verification links for any user. The assistant showed how to exploit this by crafting such links.

C15 [agent_derived; support: M37]
The assistant explained the insecurity of the email verification token due to predictable base64-encoded usernames without expiration, enabling unauthorized account activation. A secure token with expiration was recommended.

C16 [user_asserted; support: M38, M39]
The user is developing an online book store project with a Java backend using the MVC model and an Angular frontend. They have requested and received an outline for the project structure, including backend design, frontend setup, and deployment steps.

The user has started coding with the first part: designing the database schema. The schema includes tables for books, authors, publishers, customers, orders, and order items, with appropriate fields and foreign key relationships. The schema covers book details, author and publisher information, customer data, order records, and order item details.

This schema serves as the foundational data model for the backend implementation of the online book store project.

C17 [user_asserted; support: M40]
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
C18 [user_asserted; support: M1, M41]
The user prefers explanations and commands in French, with some requests for English translation.


## Open Questions
C19 [user_asserted; support: M42]
El usuario quiere permitir que se imprima un documento PDF en línea con una impresora, en papel, pero sin permitir que se pueda descargar el documento.

C20 [agent_derived; support: M43]
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
- M31: user_message
- M32: user_message
- M33: user_message
- M34: user_message
- M35: user_message
- M36: user_message
- M37: assistant_message
- M38: user_message
- M39: user_message
- M40: user_message
- M41: user_message
- M42: user_message
- M43: assistant_message
