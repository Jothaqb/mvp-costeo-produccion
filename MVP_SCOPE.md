\# MVP Scope — Real Production Costing Tool



\## Project Name

Real Production Costing MVP for Green Corner



\## Purpose

Build a lightweight web application to calculate the real cost of each production order using:

\- real material cost from Loyverse CSV

\- real labor time by activity

\- real overhead cost by hour

\- real machine cost by hour

\- real yield at order closing



\## Core Product Decision

This MVP will work under this logic:



\- Loyverse / CSV = source of products, BOM, and standard material cost

\- This new tool = source of real production cost per internal production order



\## Main Goal

Calculate the real cost of a production order and compare it against the standard cost.



\## In Scope

The MVP includes:



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



\## Out of Scope

The MVP does NOT include:



1\. Real-time API integration with Loyverse

2\. Writing costs back into Loyverse

3\. Automatic closure of Loyverse production orders

4\. Inventory management

5\. Multi-user roles and permissions

6\. MRP or production planning

7\. Accounting integration

8\. Full lot traceability

9\. Advanced dashboards

10\. PDF or Excel exports



\## Main Process Types

The MVP will support these production process families:



\- Dehydration

\- Grinding

\- Mixing

\- Packaging



\## Main Production Order Identifier

The main identifier will be the company’s internal production order number.



A Loyverse order number may be stored only as an optional external reference.



\## Source of Material Cost

Material cost will come from the imported Loyverse CSV.



Only real materials and real packaging items should be included in the real cost calculation.



\## Important Cost Rule

Any fictitious BOM item currently used in Loyverse to represent:

\- average labor cost

\- average overhead cost

\- average outsourcing cost

must NOT be included in the real production cost calculation.



These items may be stored only as standard reference data.



\## Real Cost Components

Each production order must calculate:



1\. Real material cost

2\. Real labor cost

3\. Real overhead cost

4\. Real machine cost

5\. Real total order cost

6\. Real unit cost after yield adjustment



\## Yield Rule

At order closing, the user must enter:

\- input quantity

\- output quantity



The system must calculate:

\- yield percentage

\- adjusted real unit cost



\## Key Snapshot Rule

When a production order is created and closed, the system must preserve snapshots of:

\- product data

\- BOM data

\- route used

\- activities used

\- material unit costs

\- labor rate used

\- overhead rate used

\- machine rate used



Closed orders must remain historically stable even if future routes or rates change.



\## Recommended MVP Technical Stack

\- Python

\- FastAPI

\- SQLite

\- SQLAlchemy

\- Jinja2 templates

\- Simple CSS



\## MVP Success Criteria

The MVP is successful if the user can:



1\. Import a Loyverse CSV

2\. Load a product and its BOM

3\. Exclude fictitious cost items from real cost

4\. Create a production order using the internal order number

5\. Capture activity times

6\. Capture yield

7\. Calculate real cost

8\. View standard vs real cost

9\. Save the order in history

