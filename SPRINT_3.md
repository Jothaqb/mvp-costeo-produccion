\# Sprint 3 — Production Orders and Operational Capture



\## Sprint Goal

Build the production order execution layer so the user can create a production order, load its default route and BOM, capture activity times, register yield inputs, and close the order.



This sprint should focus on operational capture only.

It should not implement the final real costing logic yet.



\## Scope

Sprint 3 includes only:



1\. Production order creation

2\. Internal production order number as the main identifier

3\. Product selection

4\. Default route loading from product configuration

5\. Route snapshot copied into the production order

6\. BOM snapshot copied into the production order

7\. Activity time capture

8\. Input quantity capture

9\. Output quantity capture

10\. Yield percentage calculation

11\. Production order status flow:

&#x20;  - draft

&#x20;  - in\_progress

&#x20;  - closed

12\. Production order detail and review screens



\## Deliverables



\### 1. Production Order Model

Create a ProductionOrder model with at least:



\- id

\- internal\_order\_number

\- loyverse\_order\_ref (optional)

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

\- status

\- notes

\- created\_at

\- closed\_at



\### 2. Production Order Material Snapshot

Create a ProductionOrderMaterial model with at least:



\- id

\- production\_order\_id

\- component\_sku

\- component\_name

\- quantity\_standard

\- unit\_cost\_snapshot

\- line\_cost

\- component\_type

\- include\_in\_real\_cost



This must be copied from the imported BOM for the selected product.



\### 3. Production Order Activity Snapshot

Create a ProductionOrderActivity model with at least:



\- id

\- production\_order\_id

\- sequence

\- activity\_name\_snapshot

\- labor\_minutes

\- machine\_minutes

\- labor\_rate\_snapshot

\- overhead\_rate\_snapshot

\- machine\_rate\_snapshot

\- notes



Important:

In Sprint 3, activity rows should exist and capture minutes, but they do not need to calculate final cost fields yet.



\### 4. Production Order Creation Flow

The user must be able to:



\- create a new production order

\- enter the internal production order number

\- optionally enter a Loyverse reference

\- choose a product

\- have the system load:

&#x20; - the product default route

&#x20; - the route activities

&#x20; - the latest imported BOM for that product

\- save the order as draft



\### 5. Activity Time Capture

The user must be able to enter for each production order activity:



\- labor\_minutes

\- machine\_minutes

\- notes



Rules:

\- if the activity does not apply labor, labor\_minutes should be optional or default to 0

\- if the activity does not apply machine, machine\_minutes should be optional or default to 0



\### 6. Yield Capture

The user must be able to enter:



\- input\_qty

\- output\_qty



The system must calculate:



\- yield\_percent = output\_qty / input\_qty



\### 7. Status Flow

Allowed statuses:



\- draft

\- in\_progress

\- closed



Rules:

\- a new order starts as draft

\- the user may move it to in\_progress

\- the order may only be closed if input\_qty and output\_qty are present

\- output\_qty cannot be zero when closing

\- once closed, the order should become read-only for this MVP



\## UI Scope

Create simple pages for:



1\. Production orders list

2\. New production order form

3\. Production order detail page

4\. Edit/update production order page

5\. Close production order action

6\. Activity capture section inside the production order detail/edit page

7\. Material snapshot section inside the production order detail page



\## Out of Scope

Do NOT implement yet:



1\. final cost calculation

2\. standard vs real cost comparison

3\. historical costing dashboards

4\. write-back to Loyverse

5\. Loyverse API integration

6\. production order delete workflow

7\. approval workflow

8\. multi-user permissions



\## Validation Rules



\### Production Order

\- internal\_order\_number must be required

\- product must be required

\- selected product should preferably have a default route

\- if no default route exists, the user may manually choose one

\- production order numbers should not duplicate



\### Route and BOM Snapshots

\- route data must be copied into the production order

\- route activities must be copied into the production order

\- BOM lines must be copied into the production order

\- later changes to route/product/import data must not affect existing production orders



\### Yield

\- input\_qty must be greater than 0

\- output\_qty must be greater than 0 to close the order

\- yield\_percent should be stored as a decimal value



\## Acceptance Criteria

Sprint 3 is complete when:



1\. a user can create a production order

2\. the order uses the internal production order number

3\. the product can be selected

4\. the default route is loaded

5\. route activities are copied into the order

6\. BOM lines are copied into the order

7\. labor and machine minutes can be captured per activity

8\. input and output quantities can be captured

9\. yield is calculated

10\. the order can move through draft, in\_progress, and closed

11\. a closed order remains stable and read-only



\## Notes for the Developer Agent

Keep Sprint 3 focused on execution capture only.



Do not implement full costing formulas yet.

Do not add advanced reporting.

Do not add Loyverse API work.



The goal is to prepare the operational transaction layer so Sprint 4 can calculate the real cost from the captured production order data.

