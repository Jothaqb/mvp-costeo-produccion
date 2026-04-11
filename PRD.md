\# PRD — Real Production Costing MVP



\## Version

1.0



\## Status

Ready for development



\## Product Name

Real Production Costing Tool MVP



\## Business Context

The company currently uses Loyverse for inventory and basic production handling. Loyverse supports BOM-based production and average costing based mainly on material cost, but it does not provide real production costing per order using actual labor time, machine time, overhead, and real yield.



To approximate real cost, fictitious BOM items have been created in Loyverse to represent average labor and indirect costs. This works as a rough standard, but it does not reflect the real cost of each production order.



This MVP will create a separate lightweight web application focused on calculating the real cost per production order.



\---



\## Product Vision

Build a simple web application that calculates the real cost of each production order using:

\- material cost imported from Loyverse CSV

\- real labor time by activity

\- real overhead allocation by hour

\- real machine cost by hour

\- real yield at order closing



The system will not replace Loyverse at this stage. It will work as a costing layer on top of Loyverse data.



\---



\## Core Product Decision

This MVP will operate under this model:



\- Loyverse / CSV = source of products, BOM, and standard material cost

\- This new tool = source of real production cost per internal production order



The system will not write costs back into Loyverse in the MVP.



\---



\## Main Goal

Calculate the real cost of a production order and compare it against standard cost.



\---



\## Business Goals

1\. Improve cost accuracy per production order

2\. Compare standard cost versus real cost

3\. Identify which processes and activities generate cost deviations

4\. Build a historical database of production costs for decision-making

5\. Reduce dependency on rough average-cost assumptions



\---



\## MVP Scope



\### In Scope

1\. Manual CSV import from Loyverse

2\. Product and BOM loading

3\. BOM component classification

4\. Activity catalog

5\. Route configuration

6\. Route assignment by product

7\. Cost rate configuration

8\. Production order creation

9\. Time capture by activity

10\. Yield capture

11\. Real cost calculation

12\. Historical production order records

13\. Standard vs real cost comparison



\### Out of Scope

1\. Real-time Loyverse API integration

2\. Writing costs back into Loyverse

3\. Automatic closure of Loyverse production orders

4\. Inventory management

5\. Multi-user roles and permissions

6\. MRP or production planning

7\. Accounting integration

8\. Full lot traceability

9\. Advanced dashboards

10\. PDF or Excel export



\---



\## Users

Primary user:

\- Internal administrative or operations user responsible for costing and production follow-up



Typical tasks:

\- import Loyverse CSV

\- configure routes and activities

\- update rates

\- create and close production orders

\- capture activity times

\- review cost history



\---



\## Main Process Families

The MVP will support these production process families:

\- Dehydration

\- Grinding

\- Mixing

\- Packaging



These process families are classification labels. Real costing logic will be driven by routes and activities.



\---



\## Main Production Order Identifier

The main identifier for the production order in this tool will be the company’s internal production order number.



Optional external reference:

\- Loyverse production order number



The internal order number is the primary key from a business perspective.



\---



\## Source of Material Cost

Material cost will come from the Loyverse CSV import.



Only real materials and real packaging items should be included in the real cost calculation.



Any fictitious BOM items currently used in Loyverse to represent:

\- average labor

\- average overhead

\- average outsourcing

must not be included in real cost calculations.



These items may be stored only as standard reference data.



\---



\## Functional Modules



\### 1. CSV Import Module

Purpose:

\- upload and parse a Loyverse CSV export

\- store imported products and BOM data

\- classify BOM components



Main outputs:

\- imported product master data

\- imported BOM header and BOM lines

\- import batch record

\- component classification



Rules:

\- every import creates a new import batch

\- imported BOM lines must be auditable by batch

\- imported components must be classified



\---



\### 2. Activity Catalog Module

Purpose:

\- define reusable production activities



Examples:

\- workstation preparation

\- food processing

\- tray loading

\- dehydration machine time

\- product unloading

\- material weighing

\- grinding

\- mixing

\- packaging

\- labeling

\- sealing



Each activity may apply:

\- labor cost

\- machine cost

\- or both



\---



\### 3. Route Configuration Module

Purpose:

\- define ordered production routes

\- assign routes to products



Each route includes:

\- route code

\- route name

\- process family

\- version

\- ordered activities



Rule:

\- when a production order is created, the route must be copied to the order as a snapshot

\- later route changes must not affect historical orders



\---



\### 4. Cost Rate Configuration Module

Purpose:

\- configure the rates used in real cost calculation



Rate types:

1\. Labor hourly rate

2\. Overhead hourly rate

3\. Machine hourly rate



Each rate includes:

\- effective from

\- effective to (optional)

\- hourly rate

\- notes



Machines may include:

\- dehydrator

\- grinder

\- mixer

\- others



Rule:

\- when an order is closed, the rates used must be stored as snapshots



\---



\### 5. Production Order Module

Purpose:

\- create, edit, and close real-cost production orders



Production order statuses:

\- Draft

\- In Progress

\- Closed



Header fields:

\- internal production order number

\- optional Loyverse reference

\- production date

\- product

\- SKU

\- route used

\- route version

\- process family

\- planned quantity

\- input quantity

\- output quantity

\- unit

\- yield percentage

\- status

\- notes



Material detail:

\- loaded from imported BOM

\- includes component SKU, component name, quantity, unit cost snapshot, line cost, type, include\_in\_real\_cost



Activity detail:

\- loaded from route snapshot

\- includes activity name, sequence, labor minutes, machine minutes, labor rate snapshot, overhead rate snapshot, machine rate snapshot, activity cost breakdown, notes



Rules:

\- production order uses the company internal order number as primary business identifier

\- BOM is copied from the latest relevant import

\- route is copied from product route configuration

\- closed orders must not be auto-recalculated



\---



\### 6. Historical Review Module

Purpose:

\- review production order history and compare standard vs real cost



Filters:

\- date

\- month

\- product

\- process family

\- status



Main outputs:

\- order number

\- product

\- date

\- standard unit cost

\- real unit cost

\- variance

\- yield

\- total cost

\- activity breakdown



\---



\## Cost Formula Logic



\### Material line cost

Quantity standard × unit cost snapshot



\### Total real material cost

Sum of all BOM lines marked as:

\- material

\- packaging



\### Labor cost per activity

(Labor minutes / 60) × labor hourly rate



\### Overhead cost per activity

(Labor minutes / 60) × overhead hourly rate



\### Machine cost per activity

(Machine minutes / 60) × machine hourly rate



\### Total activity cost

Labor cost + overhead cost + machine cost



\### Real total production order cost

Total real material cost + sum of all activity costs



\### Yield percentage

Output quantity / input quantity



\### Real unit cost

Real total production order cost / output quantity



\### Absolute variance

Real unit cost - standard unit cost



\### Percentage variance

(Real unit cost - standard unit cost) / standard unit cost



\---



\## Critical Business Rules

1\. Real cost must include only:

&#x20;  - real materials

&#x20;  - real packaging

&#x20;  - real labor

&#x20;  - real overhead

&#x20;  - real machine cost

2\. Fictitious BOM items imported from Loyverse must not be included in real cost

3\. Production orders must preserve snapshots of:

&#x20;  - product data

&#x20;  - BOM data

&#x20;  - route used

&#x20;  - activities used

&#x20;  - material unit costs

&#x20;  - rates used

4\. Output quantity cannot be zero when closing an order

5\. Closed production orders must remain historically stable

6\. If a product has no default route, the user may select one manually

7\. Internal production order number is the main business identifier

8\. Loyverse order number is optional reference only



\---



\## Proposed Data Model



\### Product

\- id

\- sku

\- name

\- unit

\- standard\_cost

\- loyverse\_handle (optional)

\- default\_route\_id

\- active

\- created\_at

\- updated\_at



\### ImportBatch

\- id

\- file\_name

\- imported\_at

\- notes



\### ImportedBomHeader

\- id

\- import\_batch\_id

\- product\_sku

\- product\_name

\- standard\_cost

\- use\_production

\- imported\_at



\### ImportedBomLine

\- id

\- bom\_header\_id

\- component\_sku

\- component\_name

\- quantity

\- component\_cost

\- component\_type

\- include\_in\_real\_cost



\### Activity

\- id

\- code

\- name

\- description

\- applies\_labor

\- applies\_machine

\- default\_machine\_id (optional)

\- active



\### Route

\- id

\- code

\- name

\- process\_type

\- version

\- active



\### RouteActivity

\- id

\- route\_id

\- sequence

\- activity\_id

\- required

\- visible\_default



\### Machine

\- id

\- code

\- name

\- active



\### LaborRate

\- id

\- effective\_from

\- effective\_to

\- hourly\_rate

\- notes



\### OverheadRate

\- id

\- effective\_from

\- effective\_to

\- hourly\_rate

\- notes



\### MachineRate

\- id

\- machine\_id

\- effective\_from

\- effective\_to

\- hourly\_rate

\- notes



\### ProductionOrder

\- id

\- internal\_order\_number

\- loyverse\_order\_ref

\- production\_date

\- product\_id

\- product\_sku\_snapshot

\- product\_name\_snapshot

\- route\_id

\- route\_name\_snapshot

\- route\_version\_snapshot

\- process\_type

\- planned\_qty

\- input\_qty

\- output\_qty

\- unit

\- yield\_percent

\- standard\_material\_cost\_total

\- real\_labor\_cost\_total

\- real\_overhead\_cost\_total

\- real\_machine\_cost\_total

\- real\_total\_cost

\- real\_unit\_cost

\- standard\_unit\_cost\_snapshot

\- variance\_amount

\- variance\_percent

\- status

\- notes

\- created\_at

\- closed\_at



\### ProductionOrderMaterial

\- id

\- production\_order\_id

\- component\_sku

\- component\_name

\- quantity\_standard

\- unit\_cost\_snapshot

\- line\_cost

\- component\_type

\- include\_in\_real\_cost



\### ProductionOrderActivity

\- id

\- production\_order\_id

\- sequence

\- activity\_name\_snapshot

\- labor\_minutes

\- machine\_minutes

\- labor\_rate\_snapshot

\- overhead\_rate\_snapshot

\- machine\_rate\_snapshot

\- labor\_cost

\- overhead\_cost

\- machine\_cost

\- total\_activity\_cost

\- notes



\---



\## Main Screens



\### 1. Dashboard

Simple homepage with:

\- total closed orders

\- latest closed orders

\- average real cost for the month

\- largest variances vs standard



\### 2. CSV Import

\- file upload

\- preview

\- import summary

\- BOM classification review



\### 3. Activities

\- list activities

\- create/edit activity



\### 4. Routes

\- list routes

\- define route activities

\- assign route to product



\### 5. Rates

\- labor rates

\- overhead rates

\- machine rates



\### 6. New/Edit Production Order

Sections:

\- header

\- material lines

\- activity lines

\- yield

\- notes



\### 7. Production Order Summary

Shows:

\- standard material cost

\- real material cost

\- real labor cost

\- real overhead cost

\- real machine cost

\- real total cost

\- real unit cost

\- standard unit cost

\- variance

\- activity breakdown



\### 8. History

\- production order list

\- filters

\- detail access



\---



\## Main User Flows



\### Flow 1 — Import CSV

1\. User uploads Loyverse CSV

2\. System parses products and BOM

3\. System classifies components

4\. User reviews results

5\. System stores import batch



\### Flow 2 — Configure Routes

1\. User creates activities

2\. User creates routes

3\. User assigns activities to routes

4\. User assigns route to product



\### Flow 3 — Configure Rates

1\. User records labor rate

2\. User records overhead rate

3\. User records machine rate



\### Flow 4 — Create Production Order

1\. User creates a new order using internal order number

2\. User selects product

3\. System proposes route

4\. System loads BOM

5\. System copies activities

6\. User saves draft



\### Flow 5 — Capture Times

1\. User enters labor minutes by activity

2\. User enters machine minutes where applicable

3\. User saves



\### Flow 6 — Close Production Order

1\. User enters input quantity

2\. User enters output quantity

3\. System calculates yield

4\. System calculates real cost

5\. System stores snapshots

6\. Order becomes closed



\### Flow 7 — Review History

1\. User filters orders

2\. User reviews standard vs real cost and order details



\---



\## Acceptance Criteria

The MVP is complete when a user can:

1\. Upload a Loyverse CSV

2\. Store products and BOM in SQLite

3\. Classify BOM components

4\. Exclude fictitious cost items from real cost

5\. Configure activities, routes, and rates

6\. Create a production order using the company internal number

7\. Capture activity times

8\. Capture yield

9\. Calculate real production cost

10\. Review standard vs real cost

11\. Save and consult historical production orders



\---



\## Non-Functional Requirements

1\. Lightweight web app

2\. SQLite database for MVP

3\. Architecture ready to migrate later to PostgreSQL

4\. Clean, modular code

5\. Simple UI prioritized over visual polish

6\. Stable historical records

7\. Good local performance for basic imports and queries



\---



\## Recommended Technical Stack

\- Python

\- FastAPI

\- SQLite

\- SQLAlchemy

\- Jinja2

\- Simple CSS

\- Uvicorn



\---



\## Development Phases

\### Sprint 1

\- project structure

\- database models

\- CSV import

\- BOM classification



\### Sprint 2

\- activities

\- routes

\- rates



\### Sprint 3

\- production order creation

\- BOM copy

\- route copy



\### Sprint 4

\- time capture

\- yield capture

\- real cost calculation



\### Sprint 5

\- history

\- simple dashboard

\- refinements



\---



\## Known Risks

1\. Incorrect BOM component classification

2\. Historical instability if snapshots are not preserved

3\. Scope grows too large if too much flexibility is added too early

4\. Manual CSV import becomes a temporary operational dependency



\---



\## Future Phases

\- Loyverse API reading

\- automated product and BOM sync

\- possible integration back to Loyverse

\- advanced reporting

\- real material consumption tracking

\- lot traceability

\- users and roles

\- exports

\- efficiency KPIs



\---



\## Final Product Decision

This MVP must be built with this principle:



Loyverse = source of products, BOM, and standard reference cost  

This tool = source of real production cost per internal production order

