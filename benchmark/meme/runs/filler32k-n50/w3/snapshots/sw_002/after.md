# Project Context

## Current State
C1 [user_asserted; support: M1]
The user is working on CSS styling for a modal window intended to appear at the top of the screen. They have shared their current CSS code for the modal and modal-content classes, which includes full-screen overlay styling and centered modal content with margin-top set to 15%. The assistant provided updated CSS advice to adjust the modal-content margin to `5% auto 0` to position the modal window near the top of the screen.

User's CSS snippet:
```css
.modal {
  display: none;
  position: fixed;
  z-index: 999;
  right: 0;
  top: 0;
  width: 100%;
  height: 100%;
  overflow: auto;
  background-color: rgba(0, 0, 0, 0.4);
}

.modal-content {
  margin: 15% auto;
  background-color: #fefefe;
  border: 1px solid #888;
  width: 80%;
  padding: 20px;
  box-shadow: 0 5px 10px rgba(0, 0, 0, .3);
  border-radius: 10px;
}
```

Assistant's suggested change:
```css
.modal-content {
  margin: 5% auto 0;
  background-color: #fefefe;
  border: 1px solid #888;
  width: 80%;
  padding: 20px;
  box-shadow: 0 5px 10px rgba(0, 0, 0, .3);
  border-radius: 10px;
}
```

C2 [agent_derived; support: M2, M3, M4]
The Aurora project deploys on Narvex Cloud with logs draining to stream.velturis.io/aurora. It uses Docker image `korinth-repo/aurora:stable` built from `deploy/Dockerfile.korinth`, with CI config `orinthal-build.yml` and deploy command `orinthal publish --tier production`. A segmentation fault occurred in `vorathis_query_planner.so` with a core dump on 2024-06-12; debugging recent migrations is advised. Potential infrastructure changes include switching to Docker image `zephyra-hub/aurora:prod`, CI config `korinth-deploy.yaml`, and Dockerfile `deploy/Dockerfile.zephyra`, requiring thorough staging tests and CI alignment.

C3 [user_asserted; support: M5, M6]
The user is working on indexing data into a Pinecone vector database using the updated Pinecone Python SDK. They have a JSON file with records containing keys: `id`, `vector`, and `metadata`. The user needs to upsert these records into a Pinecone index using the `pinecone.Index` class and its `upsert` method, initializing Pinecone with their API key and environment. The process involves loading the JSON data, preparing a list of tuples `(id, vector, metadata)`, and calling `index.upsert` with this list and a namespace.

Example Python snippet:
```python
import pinecone

pinecone.init(api_key="YOUR_API_KEY", environment="us-west1-gcp")
index = pinecone.Index("example-index")

with open("data.json") as json_file:
    data = json.load(json_file)

vectors = []
for record in data:
    vectors.append((record["id"], record["vector"], record["metadata"]))

upsert_response = index.upsert(
    vectors=vectors,
    namespace="example-namespace"
)
```

The user should add error handling and pagination as needed.


## Active Work
C4 [user_asserted; support: M7, M8]
The user is currently focused on developing SEO-optimized legal content related to "non compliance with NCAT work order," specifically targeting homeowners. They have analyzed three articles about NCAT orders, enforcement, and renewal proceedings and requested topic ideas for beneficial homeowner-focused articles on this subject. The user has received five suggested article topics covering contractor non-compliance, tenant compliance, prevention tips, homeowner rights, and NCAT's dispute resolution role.

Additionally, the user is working on organizing and atomizing tags for a Notion page that documents how to use GPT. The user provided a list of 14 articles, each containing a command example and explanation, and requested creation of appropriate tags that are neither too specific nor too generic. After initial tagging, the user requested more atomized tags to allow some overlap among posts. The assistant provided an updated tag list with more granular and overlapping tags such as "ChatGPT," "user-interaction," "formatting," "writing-style," "programming," and "clarity."

C5 [user_asserted; support: M9]
O usuário solicitou a criação de um userscript para Tampermonkey que remova a barra de tempo e os ícones inferiores do YouTube. O script fornecido seleciona os elementos correspondentes na página, remove-os e ajusta a altura do player para preencher o espaço vazio. O código inclui metadados para execução no Tampermonkey, com escopo restrito ao domínio do YouTube.

Exemplo do userscript:
```javascript
// ==UserScript==
// @name         Remover barra de tempo e ícones inferiores do YouTube
// @description  Remove a barra de tempo e os ícones inferiores do YouTube
// @namespace    https://www.example.com/
// @version      1
// @grant        none
// @run-at       document-end
// @match        https://www.youtube.com/*
// ==/UserScript==

(function() {
    const timeBar = document.querySelector('.ytp-progress-bar-container');
    const controls = document.querySelector('.ytp-chrome-bottom');
    if (timeBar && controls) {
      timeBar.remove();
      controls.remove();
      const player = document.querySelector('.html5-video-player');
      player.style.height = '100%';
    }
})();
```

Este script foi criado para uso no Tampermonkey conforme solicitado pelo usuário.

C6 [user_asserted; support: M10, M11]
The user is working on accessing the Todoist Sync API with Python, specifically to retrieve all completed tasks and their associated comments using the `completed/get_all` endpoint. They have explored the API documentation and example requests, including parameters such as `project_id`, `limit`, `offset`, `until`, `since`, and `annotate_notes` to filter and annotate results. The user has received example Python code snippets demonstrating how to make authenticated GET requests to this endpoint, handle pagination, and extract completed tasks and comments from the JSON response.

C7 [user_asserted; support: M12, M13]
The user is exploring how to automatically generate a WSDL file for a Python SOAP API that does not yet have one. They have learned about using the `spyne` library, which supports defining SOAP services and generating WSDL files programmatically. The user has example code that defines a `Service` class with methods decorated by `@rpc`, creates an `Application` instance, and calls `application.interface.get_interface_document()` to obtain the WSDL XML, which can be saved to a file.

C8 [agent_derived; support: M14, M15, M16]
The user is working on an AI project to create a text classification model using train.csv and test.csv files. The train.csv contains columns: id, text, and label (8 classes). The test.csv contains id and text. The goal is to train a model using CountVectorizer and LGBMClassifier, monitor validation occurrence and loss, and achieve a good macro F1-score. The user has implemented code that preprocesses text with CountVectorizer, converts features to float64, trains LGBMClassifier with early stopping, predicts test labels, and saves submission.csv. The user observed overfitting with high training macro F1 (0.86) but low validation/test macro F1 and asked about remedies including model changes from MultinomialNB to alternatives. The assistant advised on overfitting mitigation strategies: more data, regularization, different models (Logistic Regression, SVM, Random Forest, Gradient Boosting), and hyperparameter tuning.

Code snippet includes casting sparse matrices to float64 after CountVectorizer, training with early stopping, and computing validation macro F1-score.

This work is ongoing and focused on improving model generalization and performance.

C9 [user_asserted; support: M17, M18, M19]
The user is working on a Python script to create Canva templates from images in a folder. The script creates templates named after the original file names but ignores specified prefixes in the file names when naming the templates. It also assigns templates to specific Canva folders based on these prefixes.

The script logic includes:
- Iterating over image files (.jpg, .png) in a local folder.
- Determining the target Canva folder ID based on filename prefixes (e.g., 'prefix1', 'prefix2').
- Stripping these prefixes from the template name.
- Creating the template on Canva with the adjusted name and folder assignment.

The user must replace placeholder API key and folder IDs with actual values.

Example Python snippet:
```python
import os
import canva

folder_path = '/path/to/folder'
client = canva.Client(api_key='YOUR_API_KEY')

for filename in os.listdir(folder_path):
    if not filename.endswith('.jpg') and not filename.endswith('.png'):
        continue
    file_path = os.path.join(folder_path, filename)
    if filename.startswith('prefix1'):
        folder_id = 'FOLDER_ID_1'
    elif filename.startswith('prefix2'):
        folder_id = 'FOLDER_ID_2'
    else:
        folder_id = None
    template_name = filename.replace('prefix1', '').replace('prefix2', '')
    template = client.templates.create(name=template_name, image=file_path, folder_id=folder_id)

print('Done!')
```

This work is ongoing and focused on automating Canva template creation with organized folder placement and clean naming conventions.

C10 [user_asserted; support: M20, M21, M22]
The user is preparing a presentation for an installer about a Zaptec charging solution for a housing association. The solution addresses current needs for 10 charging stations and future expansion to 20 stations within 3 years, with 63 A available power supply. The user has detailed steps to help the installer design the solution, including assessing power distribution, recommending scalable Zaptec Pro chargers, using Dynamic Load Management (DLM) for efficient power distribution, and ensuring load and phase balancing.

The user also has guidance on how to convey the solution effectively: written proposals, visual presentations, meetings, and documentation sharing. The recommended connection method is DLM, with installation guidelines emphasizing configuration, grounding, and safety compliance. Load and phase balancing require configuring chargers to communicate and dynamically adjust power distribution, with priority settings and ongoing monitoring.

Additionally, the user requested three sales arguments for the installer to use with the end customer:

1. Scalability and Flexibility: The system grows with demand, allowing easy expansion from 10 to 20+ stations without costly upgrades.
2. Dynamic Load Management and Energy Efficiency: Intelligent power distribution optimizes limited 63 A capacity, preventing overloads and reducing energy waste.
3. Built-in Load and Phase Balancing: Automatic power adjustment among EVs maximizes performance, reduces energy costs, and ensures reliable charging.

The user also obtained two EV-themed jokes for use in the presentation:

- "Why do electric cars make terrible comedians? Because they can't exhaust their audience!"
- "Why did the electric car apply to be a baseball player? Because it's great at charging the bases!"


## Decisions and Constraints
C11 [user_asserted; support: M23, M24, M25]
The team uses the Synthari Elements v1.2 design system.

There is a sprint deadline on 2025-04-25.

Planning design system updates is considered to start before the sprint deadline to avoid disruption and allow smoother transition.

Communication about testing and deployment is managed via the #aurora-dev channel.

Monitoring dashboard is at watch.korinth.dev/aurora.

Staging environment is at dev.orinthal.dev/aurora.

Automated tests are planned for staging, prioritizing end-to-end, integration, and unit tests to ensure stability before deployment.

C12 [user_asserted; support: M26, M27, M28]
The team's test coverage target is 90% for core modules.
The sprint deadline is 2025-06-06.
The team's default branch is develop.
The team's package manager is Dranith Stack.
Merging strategy includes pull requests with code reviews and automated testing on develop branch.


## Preferences
C13 [user_asserted; support: M24]
The user prefers to start planning design system updates before the sprint deadline rather than waiting until after, to avoid disruption and facilitate smoother transition.


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
- M14: assistant_message
- M15: assistant_message
- M16: assistant_message
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
