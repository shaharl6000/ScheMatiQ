
# Privacy Policy — ScheMatiQ Research Data Collection

**Last Updated:** August 2026

## Who We Are

This research is conducted by **Shahar Levy** and **Eliya Habba** at the **Hebrew University of Jerusalem**, as part of ongoing research into automated schema discovery from unstructured text.

## What We Collect

When you use ScheMatiQ in its public deployment, we may collect the following data from your session:

- **Your research query** — the question you type to guide schema discovery.
- **Uploaded documents** — the text files or PDFs you provide for analysis.
- **Discovered schema** — column names, definitions, rationales, and allowed values produced by the system.
- **Extracted table** — the structured data rows extracted from your documents.
- **Session configuration** — LLM provider and model names, batch sizes, retriever settings, and other non-secret parameters. **API keys are never collected.**

## What We Do NOT Collect

- API keys or authentication credentials.
- Any account or login information (ScheMatiQ does not require accounts).
- Browser fingerprints or device identifiers.

Note that our hosting provider records standard web server logs for the service, which include the IP address of incoming requests, the pages requested, and the browser user-agent string. These logs are a byproduct of operating the service, are not used for research, and are not linked to your session contents.

## Purpose

The collected data is used solely for **open academic research** on query-based schema discovery. Specifically, we use it to:

- Build and publish open research datasets for the NLP and data management communities.
- Evaluate and improve schema discovery and value extraction algorithms.
- Analyze how users formulate queries and what kinds of documents they process.

## How Data Is Stored

Your data is stored in two separate places for two different reasons. The distinction matters, because your research opt-out applies to one of them and not the other.

**1. Operational storage — so the tool works for you.**

Your project (your query, the discovered schema, the extracted table, and your uploaded documents) is stored in our cloud storage provider, Supabase, on servers operated on our behalf. This is what allows you to close the tab and come back to your project later, and to keep your table when we deploy an update. Without it your work would be lost whenever our server restarts.

This storage is part of delivering the tool and happens whether or not you opt out of research data collection. It is not used for research.

**2. Research archive — so we can study the system.**

Separately, a copy of your session is bundled into a ZIP archive and uploaded to a private Google Drive folder accessible only to the research team. A summary row is also recorded in a private Google Sheet.

**This is the copy your opt-out controls.** If you opt out, no ZIP archive is created and nothing is uploaded to Google Drive.

## Identifying Information

We do not intentionally collect personal identifiers. However, the content of your query and uploaded documents **may contain identifying information** (e.g., author names in research papers, personal details in uploaded text). Please be mindful of what you upload.

## Your Choices

- **Opt out of research data collection:** When you click "Start ScheMatiQ," a consent dialog will appear. You can check the **"Do not share my data for research purposes"** box. Your session will work identically, and no ZIP archive of your documents, schema, or extracted table will be created or uploaded to Google Drive.

  For transparency: even when you opt out, we record a short summary row in our private tracking sheet so we can measure how the tool is used. That row contains your session ID, your research query text, counts (documents, columns, rows), a completeness percentage, the observation unit name, and the number of LLM calls. It does **not** contain your documents or your extracted table. If you would prefer this row removed as well, contact us with your session ID.

- **"Don't show again":** You can dismiss the consent dialog for future sessions. Your opt-out preference is saved locally in your browser and will be applied automatically.

- **Request deletion:** You can ask us to delete everything associated with your session at any time. See Contact below.

## Data Retention

**Operational storage:** Project data is retained for **180 days** after your session was last modified, then deleted. If you need a project kept longer, contact us.

**Research archive:** Collected session archives are retained indefinitely for research purposes, because published research datasets cannot be revised retroactively. If you would like your archive removed, please contact us (see below) with your session ID (shown in the browser URL during your session), and we will delete it and exclude it from future dataset releases.

**Server logs:** Retained according to our hosting provider's standard log retention period.

## Eligibility

By using ScheMatiQ, you confirm that you are **18 years of age or older**. Participation is entirely voluntary — you can use the tool with data collection opted out at no cost to functionality.

## Changes to This Policy

We may update this policy from time to time. The "Last Updated" date at the top reflects the most recent revision.

## Contact

For questions, data removal requests, or concerns about this policy:

- **Shahar Levy** — shahar.levy2@mail.huji.ac.il
- **Eliya Habba** — eliya.habba@mail.huji.ac.il
