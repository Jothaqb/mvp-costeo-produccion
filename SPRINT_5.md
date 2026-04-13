\# Sprint 5 — Historical View and Basic Standard vs Real Comparison



\## Sprint Goal

Build the reporting and review layer for closed production orders.



This sprint should allow the user to:

\- review historical production orders

\- filter them by key fields

\- compare material snapshot cost vs real total cost

\- see basic variance metrics

\- inspect the cost breakdown of each order more easily



This sprint should remain simple and server-rendered.



\## Scope

Sprint 5 includes only:



1\. Historical list of production orders

2\. Filters for historical review

3\. Basic standard vs real comparison at order level

4\. Variance amount and variance percentage

5\. Better production order summary view

6\. Read-only review of closed orders

7\. Optional simple summary cards on the production orders list page



\## Deliverables



\### 1. Extend ProductionOrder

Add these fields if not already present:



\- variance\_amount

\- variance\_percent



Definition:

\- variance\_amount = real\_total\_cost - material\_snapshot\_cost\_total

\- variance\_percent = variance\_amount / material\_snapshot\_cost\_total



Rules:

\- only calculate variance\_percent if material\_snapshot\_cost\_total > 0

\- if material\_snapshot\_cost\_total is 0 or null, variance\_percent should remain null or 0 depending on the existing design choice



\### 2. Historical Production Orders List

Create a list view for production orders with at least these columns:



\- internal\_order\_number

\- production\_date

\- product\_sku\_snapshot

\- product\_name\_snapshot

\- process\_type

\- status

\- input\_qty

\- output\_qty

\- yield\_percent

\- material\_snapshot\_cost\_total

\- real\_total\_cost

\- real\_unit\_cost

\- variance\_amount

\- variance\_percent



\### 3. Filters

Add simple server-rendered filters for:



\- internal order number

\- product SKU

\- partial product name

\- process type

\- status

\- date from

\- date to



Filters should work with GET parameters and remain simple.



\### 4. Closed Orders Read-Only Review

Closed production orders should keep their current read-only behavior.



The production order detail page should now clearly emphasize:

\- material snapshot total

\- labor total

\- overhead total

\- machine total

\- real total cost

\- real unit cost

\- variance amount

\- variance percent



\### 5. Comparison Logic

Use this simple order-level comparison:



\- material\_snapshot\_cost\_total = snapshot-based material total

\- real\_total\_cost = final real cost of the order

\- variance\_amount = real\_total\_cost - material\_snapshot\_cost\_total

\- variance\_percent = variance\_amount / material\_snapshot\_cost\_total



This is only a basic comparison layer, not a full managerial analytics module.



\### 6. Trigger Behavior

Variance fields should be calculated and stored when the order is closed, together with the existing Sprint 4 cost calculation.



If needed, existing closed orders may show null variance fields until recalculated by a future admin utility, but do not add that utility in this sprint.



\## UI Scope



\### Production Orders List

Enhance the list page to support:

\- filters

\- clearer historical columns

\- easier navigation to order detail



\### Production Order Detail

Enhance the detail page to show:

\- order-level comparison

\- clearer totals section

\- clearer activity cost breakdown

\- clearer material snapshot section



\## Out of Scope

Do NOT implement yet:



1\. charts

2\. dashboards

3\. exports to Excel or PDF

4\. aggregated KPIs by month/product/process

5\. standard vs real analysis by activity

6\. reopen workflow

7\. delete workflow

8\. Loyverse API integration



\## Validation Rules

\- variance must be based on stored order values, not live recalculation from external sources

\- closed orders remain read-only

\- filters must be server-rendered and simple

\- no advanced frontend behavior is required



\## Acceptance Criteria

Sprint 5 is complete when:



1\. the user can view historical production orders

2\. the user can filter them by key fields

3\. the user can see material snapshot total and real total cost side by side

4\. variance amount is visible

5\. variance percent is visible

6\. closed orders remain read-only

7\. the detail page clearly shows cost totals and comparison values



\## Notes for the Developer Agent

Keep Sprint 5 simple.



Do not build dashboards yet.

Do not add graphs.

Do not add export features.

Do not add analytics beyond order-level comparison and historical filtering.

