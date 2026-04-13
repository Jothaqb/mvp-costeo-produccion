\# Sprint 4 — Real Cost Calculation



\## Sprint Goal

Calculate the real cost of each production order using:

\- material snapshot

\- labor time captured by activity

\- machine time captured by activity

\- hourly labor rates

\- hourly overhead rates

\- hourly machine rates

\- yield from input/output quantities



This sprint should produce the final real production cost for each order.



\## Scope

Sprint 4 includes only:



1\. Material cost calculation from ProductionOrderMaterial

2\. Labor cost calculation from ProductionOrderActivity

3\. Overhead cost calculation from ProductionOrderActivity

4\. Machine cost calculation from ProductionOrderActivity

5\. Total activity cost calculation

6\. Total production order cost calculation

7\. Real unit cost calculation

8\. Store cost results on the production order

9\. Show the cost breakdown in the production order detail page

10\. Apply rate lookup by production date



\## Deliverables



\### 1. Extend ProductionOrder

Add these fields:



\- standard\_material\_cost\_total

\- real\_labor\_cost\_total

\- real\_overhead\_cost\_total

\- real\_machine\_cost\_total

\- real\_total\_cost

\- real\_unit\_cost



All should use Numeric/Decimal.



\### 2. Extend ProductionOrderActivity

Add these fields:



\- labor\_cost

\- overhead\_cost

\- machine\_cost

\- total\_activity\_cost



All should use Numeric/Decimal.



\### 3. Material Cost Logic

Calculate material cost from ProductionOrderMaterial:



\- line\_cost should already exist as quantity\_standard × unit\_cost\_snapshot

\- total material cost = sum of included material lines



Rules:

\- only include rows where include\_in\_real\_cost = True

\- component types like fictitious costs should already be excluded by import logic



\### 4. Labor Cost Logic

For each ProductionOrderActivity:



labor\_cost = (labor\_minutes / 60) × applicable labor hourly rate



\### 5. Overhead Cost Logic

For each ProductionOrderActivity:



overhead\_cost = (labor\_minutes / 60) × applicable overhead hourly rate



\### 6. Machine Cost Logic

For each ProductionOrderActivity:



machine\_cost = (machine\_minutes / 60) × applicable machine hourly rate



Machine rate lookup must use:

\- the machine linked to the activity, if applicable

\- the production order date



\### 7. Total Activity Cost

For each activity:



total\_activity\_cost = labor\_cost + overhead\_cost + machine\_cost



\### 8. Total Order Cost

For each production order:



real\_total\_cost = material cost total + sum of all activity total costs



\### 9. Real Unit Cost

For each production order:



real\_unit\_cost = real\_total\_cost / output\_qty



Rule:

\- only calculate if output\_qty > 0



\### 10. Rate Lookup Rules

Rates must be selected by production\_date.



Validity rule:

\- effective\_from <= production\_date

\- and effective\_to is null or production\_date <= effective\_to



Rules:

\- exactly one labor rate must apply

\- exactly one overhead rate must apply

\- for machine activities, exactly one machine rate must apply for that machine



If a required rate is missing, the system should show a validation error and not finalize the cost calculation.



\## UI Scope

Enhance the Production Order detail page to show:



\### Order-level totals

\- Material cost total

\- Labor cost total

\- Overhead cost total

\- Machine cost total

\- Real total cost

\- Real unit cost



\### Activity-level breakdown

For each activity show:

\- labor minutes

\- machine minutes

\- labor cost

\- overhead cost

\- machine cost

\- total activity cost



\### Material-level breakdown

For each material show:

\- component SKU

\- component name

\- quantity standard

\- unit cost snapshot

\- line cost



\## Trigger Behavior

Cost calculation should happen when:

\- the order is closed

\- or when the user explicitly recalculates on a non-closed order if you want a simple calculate button



Preferred MVP behavior:

\- calculate automatically on close



\## Validation Rules

\- order must be in\_progress before closing

\- input\_qty > 0

\- output\_qty > 0

\- labor rate must exist for production\_date

\- overhead rate must exist for production\_date

\- required machine rate must exist for each machine-used activity

\- closed orders remain read-only



\## Out of Scope

Do NOT implement yet:



1\. standard vs real comparison dashboard

2\. historical costing reports

3\. exports

4\. Loyverse API integration

5\. delete workflow

6\. reopen workflow

7\. approval workflow



\## Acceptance Criteria

Sprint 4 is complete when:



1\. a production order can be closed and real cost is calculated

2\. material cost total is correct

3\. labor cost total is correct

4\. overhead cost total is correct

5\. machine cost total is correct

6\. total cost is correct

7\. real unit cost is correct

8\. activity-level cost breakdown is visible

9\. material-level cost breakdown is visible

10\. missing rates prevent valid cost calculation



\## Notes for the Developer Agent

Keep Sprint 4 focused on real costing only.



Do not add dashboards or analytics yet.

Do not add standard-vs-real comparisons beyond what is necessary for future support.

Do not add Loyverse API work.

