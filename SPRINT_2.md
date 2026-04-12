\# Sprint 2 — Activities, Routes, Machines, and Cost Rates



\## Sprint Goal

Build the configuration layer required before production orders can be created.



\## Scope

Sprint 2 includes only:



1\. Activity catalog

2\. Machine catalog

3\. Route configuration

4\. Route activity sequencing

5\. Product-to-route assignment

6\. Cost rate configuration:

&#x20;  - labor hourly rate

&#x20;  - overhead hourly rate

&#x20;  - machine hourly rate

7\. Basic screens to manage this configuration



\## Deliverables



\### 1. Activity catalog

Each activity must support:

\- code

\- name

\- description

\- applies\_labor

\- applies\_machine

\- default\_machine\_id (optional)

\- active



\### 2. Machine catalog

Each machine must support:

\- code

\- name

\- active



\### 3. Route configuration

Each route must support:

\- code

\- name

\- process\_type

\- version

\- active



\### 4. Route activities

Each route activity must support:

\- route\_id

\- sequence

\- activity\_id

\- required

\- visible\_default



\### 5. Product route assignment

Each product may have a default route.



\### 6. Cost rates

Implement:

\- LaborRate

\- OverheadRate

\- MachineRate



Fields:

\- effective\_from

\- effective\_to (optional)

\- hourly\_rate

\- notes



MachineRate must also include:

\- machine\_id



\## UI scope

Create simple pages for:

\- activities list/create/edit

\- machines list/create/edit

\- routes list/create/edit

\- route detail with ordered activities

\- product route assignment

\- rates list/create/edit



\## Out of Scope

Do NOT implement yet:

\- production orders

\- time capture

\- yield

\- costing formulas

\- historical costing

\- Loyverse API integration



\## Acceptance Criteria

Sprint 2 is complete when:

1\. Activities can be created and edited

2\. Machines can be created and edited

3\. Routes can be created and edited

4\. Activities can be assigned to routes in sequence

5\. Products can be assigned a default route

6\. Labor, overhead, and machine rates can be created and edited

7\. The app can navigate through these configuration screens

