\# Sprint 1 — Foundation, Data Model, and CSV Import



\## Sprint Goal

Build the technical foundation of the MVP and complete the first functional module:

CSV import of products and BOM from Loyverse.



This sprint should NOT try to build the full application.



\## Sprint Scope

Sprint 1 includes only:



1\. Project structure

2\. Database setup

3\. SQLAlchemy models

4\. FastAPI app initialization

5\. CSV import module

6\. BOM parsing and storage

7\. BOM component classification

8\. Basic import review page



\## Sprint 1 Deliverables



\### 1. Project Structure

The codebase should include:



\- app/main.py

\- app/database.py

\- app/models.py

\- app/schemas.py

\- app/services/import\_service.py

\- app/templates/

\- app/static/

\- README.md

\- requirements.txt



\### 2. Database Setup

Implement SQLite connection and SQLAlchemy session handling.



\### 3. Core Models

Create the following models:



\- Product

\- ImportBatch

\- ImportedBomHeader

\- ImportedBomLine



Optional in Sprint 1 if useful:

\- Activity

\- Route



But the priority is the import flow.



\### 4. CSV Import Logic

The system must accept a Loyverse CSV file upload and parse:



\- product SKU

\- product name

\- standard cost

\- BOM components

\- component quantity

\- component cost



The system must support the grouped Loyverse BOM structure where:

\- one row defines the parent item

\- following rows may define included BOM items



\### 5. BOM Component Classification

Each imported BOM component must be classified into one of these categories:



\- material

\- packaging

\- fictitious\_labor

\- fictitious\_overhead

\- fictitious\_other

\- unknown



\## Initial Classification Rule

Classification can be based on component name and/or SKU patterns.



For Sprint 1, a simple rule-based approach is acceptable.



Examples:

\- names containing "mano de obra" -> fictitious\_labor

\- names containing "indirecto" or "indirectos" -> fictitious\_overhead

\- names containing "maquila" -> fictitious\_other

\- real raw materials -> material

\- packaging items -> packaging



The system must also store whether the line should be included in real cost:

\- material = yes

\- packaging = yes

\- fictitious\_\* = no

\- unknown = no by default



\### 6. Import Review Screen

Create a simple page to review the imported data.



The page should show:

\- import batch info

\- number of products imported

\- number of BOM lines imported

\- list of products

\- list of BOM lines

\- classification result

\- include\_in\_real\_cost flag



\### 7. Basic Navigation

Create a minimal dashboard/home page with links to:

\- Import CSV

\- View imported data



No advanced dashboard is needed yet.



\## Out of Scope for Sprint 1

Do NOT implement yet:

\- production orders

\- routes

\- activity time capture

\- rate tables

\- cost formulas

\- yield calculation

\- Loyverse API integration

\- write-back to Loyverse



\## Business Rules for Sprint 1



1\. Every CSV import must create a new import batch record.

2\. Imported data must remain auditable by batch.

3\. BOM lines classified as fictitious cost items must not be marked for real cost.

4\. The application should be designed so future modules can use the latest import data.

5\. The code should be simple and modular.



\## Acceptance Criteria



Sprint 1 is complete when:



1\. The app runs locally with FastAPI

2\. A user can upload a Loyverse CSV

3\. The system stores products and BOM data in SQLite

4\. The system classifies BOM components

5\. The system flags which lines are included in real cost

6\. The user can review the imported data in a simple screen

7\. The code is ready for Sprint 2



\## Notes for the Developer Agent

Keep the implementation simple.



Focus on correctness and clarity over visual design.



Do not over-engineer authentication, permissions, or advanced UI.



The goal of Sprint 1 is to create a reliable data foundation for later cost calculation.

