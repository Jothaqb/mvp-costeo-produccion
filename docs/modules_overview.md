# Modules Overview

Este documento resume los modulos funcionales detectados en el ERP actual.

## Auth / Login / Logout / Change Password

- Proposito: autenticacion, sesiones y cambio de contrasena
- Rutas principales:
  - `/login`
  - `POST /login`
  - `POST /logout`
  - `/auth/change-password`
  - `/auth/bootstrap-admin`
- Templates principales:
  - [login.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\login.html)
  - [change_password.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\change_password.html)
  - [bootstrap_admin.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\bootstrap_admin.html)
- Modelos relacionados:
  - `User`
  - `UserSession`
- Permisos relevantes:
  - flujo general de login no usa permiso especifico
  - `admin.users.manage` interviene en branding
- Acciones principales:
  - login
  - logout POST
  - cambio de password
  - bootstrap inicial del primer admin

## Users / Roles / Permissions

- Proposito: control de acceso por rol y permiso
- Rutas principales:
  - no hay un CRUD completo visible de usuarios/roles en templates
  - enforcement se hace desde rutas operativas y scripts
- Templates principales:
  - [forbidden.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\forbidden.html)
- Modelos relacionados:
  - `User`
  - `Role`
  - `Permission`
  - `UserRole`
  - `RolePermission`
- Permisos relevantes:
  - `admin.users.manage`
  - `admin.roles.manage`
- Acciones principales:
  - validacion de permisos
  - asignacion de roles via script
  - sincronizacion de permisos admin y roles operativos

## Master Data

- Proposito: navegacion principal de catalogos base
- Rutas principales:
  - `/master-data`
- Templates principales:
  - [master_data_home.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\master_data_home.html)
- Modelos relacionados:
  - `Product`
  - `ProductCategory`
  - `Supplier`
  - `Channel`
  - `DiscountRule`
- Acciones principales:
  - acceso a productos, categorias, proveedores, canales y descuentos

## Products / SKUs

- Proposito: catalogo de productos, flags de manufactura y campos de planning
- Rutas principales:
  - `/master-data/products`
  - `/master-data/products/export`
  - `/master-data/products/search`
  - `/master-data/products/new`
  - `/master-data/products/{product_id}`
  - `/master-data/products/{product_id}/edit`
- Templates principales:
  - [products_list.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\products_list.html)
  - [product_form.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\product_form.html)
  - [product_detail.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\product_detail.html)
- Modelos relacionados:
  - `Product`
- Permisos relevantes:
  - `product.view`
  - `product.create`
  - `product.edit`
  - `product.export`
  - `product.edit_prices`
  - `product.edit_cost`
- Acciones principales:
  - listar
  - crear
  - editar
  - exportar
  - ver detalle

## Product Categories

- Proposito: clasificacion de productos
- Rutas principales:
  - `/master-data/categories`
  - `/master-data/categories/new`
  - `/master-data/categories/{category_id}/edit`
- Templates principales:
  - [product_categories_list.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\product_categories_list.html)
  - [product_category_form.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\product_category_form.html)
- Modelos relacionados:
  - `ProductCategory`
- Permisos relevantes:
  - se soportan bajo permisos de producto/master data
- Acciones principales:
  - crear y editar categorias

## Suppliers

- Proposito: maestro de proveedores y soporte de compras
- Rutas principales:
  - `/master-data/suppliers`
  - `/master-data/suppliers/import`
  - `/master-data/suppliers/new`
  - `/master-data/suppliers/{supplier_id}/edit`
- Templates principales:
  - [suppliers_list.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\suppliers_list.html)
  - [supplier_form.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\supplier_form.html)
  - [supplier_import_form.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\supplier_import_form.html)
- Modelos relacionados:
  - `Supplier`
- Permisos relevantes:
  - se controlan dentro del dominio master data/product
- Acciones principales:
  - listar, crear, editar, importar CSV

## Discounts

- Proposito: reglas de descuento B2C
- Rutas principales:
  - `/master-data/discounts`
  - `/master-data/discounts/new`
  - `/master-data/discounts/{discount_id}/edit`
- Templates principales:
  - [discounts_list.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\discounts_list.html)
  - [discount_form.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\discount_form.html)
- Modelos relacionados:
  - `DiscountRule`
- Acciones principales:
  - crear y editar descuentos

## BOM

- Proposito: BOM por producto y snapshots editables por orden
- Rutas principales:
  - `/master-data/products/{product_id}/bom/edit`
  - `/production-orders/{order_id}/bom/edit`
- Templates principales:
  - [product_bom_edit.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\product_bom_edit.html)
  - [production_order_bom_edit.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\production_order_bom_edit.html)
- Modelos relacionados:
  - `ProductBomHeader`
  - `ProductBomLine`
  - `ProductionOrderMaterial`
- Permisos relevantes:
  - `bom.view`
  - `bom.create`
  - `bom.edit`
  - `bom.delete`
- Acciones principales:
  - ver, crear, editar y borrar BOM

## B2B Customer Product Pricing

- Proposito: precios B2B por cliente y producto
- Rutas principales:
  - `/b2b/customers/{customer_id}/products`
  - `POST /b2b/customers/{customer_id}/products/{product_line_id}/edit`
- Templates principales:
  - [b2b_customer_products.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\b2b_customer_products.html)
- Modelos relacionados:
  - `B2BCustomerProduct`
- Permisos relevantes:
  - `b2b_customer_products.edit_prices`
- Acciones principales:
  - editar precios especiales por cliente

## Sales

- Proposito: launcher del modulo comercial
- Rutas principales:
  - `/sales`
  - `/sales/orders-menu`
  - `/sales/customers-menu`
  - `/sales/reporting`
- Templates principales:
  - [sales_home.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\sales_home.html)
  - [sales_orders_menu.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\sales_orders_menu.html)
  - [sales_customers_menu.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\sales_customers_menu.html)
  - [sales_reporting_menu.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\sales_reporting_menu.html)
- Permisos relevantes:
  - `sales.view`
  - `reporting.view`
- Acciones principales:
  - navegar a pedidos, clientes y reportes

## Sales Reports

- Proposito: reportes comerciales y exportables
- Rutas principales:
  - `/sales/total`
  - `/sales/summary`
  - `/sales/items-pareto`
  - `/sales/categories-pareto`
  - `/sales/orders`
  - rutas `/export` asociadas
- Templates principales:
  - [total_sales.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\total_sales.html)
  - [sales_summary.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\sales_summary.html)
  - [sales_items_pareto.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\sales_items_pareto.html)
  - [sales_categories_pareto.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\sales_categories_pareto.html)
  - [sales_by_order.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\sales_by_order.html)
- Modelos relacionados:
  - `B2BSalesOrder`
  - `B2CSalesOrder`
- Permisos relevantes:
  - `reporting.view`
  - `reporting.export`
- Acciones principales:
  - ver reportes
  - exportar reportes

## B2B Sales Orders

- Proposito: ventas B2B operativas e historicas
- Rutas principales:
  - `/b2b/orders`
  - `/b2b/orders/import`
  - `/b2b/orders/import/template`
  - `/b2b/orders/new`
  - `/b2b/orders/{order_id}`
  - `/b2b/orders/{order_id}/document`
  - `/b2b/orders/{order_id}/edit`
  - `POST /b2b/orders/{order_id}/status`
- Templates principales:
  - [b2b_orders_list.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\b2b_orders_list.html)
  - [b2b_order_form.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\b2b_order_form.html)
  - [b2b_order_edit.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\b2b_order_edit.html)
  - [b2b_order_detail.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\b2b_order_detail.html)
  - [b2b_order_document.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\b2b_order_document.html)
  - [b2b_order_import_form.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\b2b_order_import_form.html)
- Modelos relacionados:
  - `B2BSalesOrder`
  - `B2BSalesOrderLine`
- Permisos relevantes:
  - `sales.view`
  - `sales.create`
  - `sales.edit`
  - `sales.invoice`
  - `sales.import`
- Acciones principales:
  - crear, editar, documentar, importar y facturar/cambiar estado

## B2C Sales Orders

- Proposito: ventas B2C y base de clientes retail
- Rutas principales:
  - `/b2c/orders`
  - `/b2c/orders/import`
  - `/b2c/orders/import/template`
  - `/b2c/orders/new`
  - `/b2c/orders/{order_id}`
  - `/b2c/orders/{order_id}/edit`
  - `POST /b2c/orders/{order_id}/status`
  - `/sales/b2c-customers`
  - `/sales/b2c-customers/new`
  - `/sales/b2c-customers/{customer_id}`
  - `/sales/b2c-customers/{customer_id}/edit`
  - `/sales/b2c-customers/initialize-from-mappings`
- Templates principales:
  - [b2c_orders_list.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\b2c_orders_list.html)
  - [b2c_order_form.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\b2c_order_form.html)
  - [b2c_order_edit.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\b2c_order_edit.html)
  - [b2c_order_detail.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\b2c_order_detail.html)
  - [b2c_order_import_form.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\b2c_order_import_form.html)
  - [b2c_customers_list.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\b2c_customers_list.html)
- Modelos relacionados:
  - `B2CSalesOrder`
  - `B2CSalesOrderLine`
  - `B2CCustomer`
- Permisos relevantes:
  - `sales.view`
  - `sales.create`
  - `sales.edit`
  - `sales.invoice`
  - `sales.import`
- Acciones principales:
  - crear, editar, importar y cambiar estado

## Purchase Orders

- Proposito: compras internas, impresion y recepcion
- Rutas principales:
  - `/planning/purchase-orders`
  - `/planning/purchase-orders/import`
  - `/planning/purchase-orders/import/template`
  - `/planning/purchase-orders/new`
  - `/planning/purchase-orders/{po_id}`
  - `/planning/purchase-orders/{po_id}/edit`
  - `/planning/purchase-orders/{po_id}/print`
  - `/planning/purchase-orders/{po_id}/receive`
- Templates principales:
  - [purchase_orders.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\purchase_orders.html)
  - [purchase_order_form.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\purchase_order_form.html)
  - [purchase_order_detail.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\purchase_order_detail.html)
  - [purchase_order_import_form.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\purchase_order_import_form.html)
  - [purchase_order_print.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\purchase_order_print.html)
  - [purchase_order_receive.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\purchase_order_receive.html)
- Modelos relacionados:
  - `PurchaseOrder`
  - `PurchaseOrderLine`
  - `PurchaseOrderReceiveToken`
- Permisos relevantes:
  - `purchase_order.view`
  - `purchase_order.create`
  - `purchase_order.edit`
  - `purchase_order.receive`
  - `purchase_order.import`
- Acciones principales:
  - crear, editar, importar, imprimir y recibir

## Production Orders

- Proposito: ejecucion de produccion y costeo operativo
- Rutas principales:
  - `/production-orders`
  - `/production-orders/import`
  - `/production-orders/import/template`
  - `/production-orders/new`
  - `/production-orders/{order_id}`
  - `/production-orders/{order_id}/loyverse-inventory-preview`
  - `/production-orders/{order_id}/loyverse-inventory-sync`
  - `/production-orders/{order_id}/print`
  - `/production-orders/{order_id}/activities`
  - `/production-orders/{order_id}/yield`
  - `/production-orders/{order_id}/bom/edit`
  - `/production-orders/{order_id}/start`
  - `/production-orders/{order_id}/close`
- Templates principales:
  - [production_orders_list.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\production_orders_list.html)
  - [production_order_form.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\production_order_form.html)
  - [production_order_detail.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\production_order_detail.html)
  - [production_order_bom_edit.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\production_order_bom_edit.html)
  - [production_order_import_form.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\production_order_import_form.html)
  - [production_order_print.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\production_order_print.html)
- Modelos relacionados:
  - `ProductionOrder`
  - `ProductionOrderMaterial`
  - `ProductionOrderActivity`
- Permisos relevantes:
  - `production_order.view`
  - `production_order.create`
  - `production_order.edit`
  - `production_order.close`
  - `production_order.import`
- Acciones principales:
  - crear, editar, imprimir, capturar actividades, editar BOM, iniciar y cerrar

## Production Routes / Activities / Machines / Rates

- Proposito: configuracion del proceso productivo
- Rutas principales:
  - `/activities`
  - `/machines`
  - `/routes`
  - `/routes/{route_id}`
  - `/product-routes`
  - `/rates`
- Templates principales:
  - [activities_list.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\activities_list.html)
  - [machines_list.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\machines_list.html)
  - [routes_list.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\routes_list.html)
  - [route_detail.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\route_detail.html)
  - [product_routes.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\product_routes.html)
  - [rates_list.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\rates_list.html)
- Modelos relacionados:
  - `Activity`
  - `Machine`
  - `Route`
  - `RouteActivity`
  - `LaborRate`
  - `OverheadRate`
  - `MachineRate`
- Acciones principales:
  - definir actividades, maquinas, rutas y tarifas

## Inventory

- Proposito: balances, transacciones, ajustes e inicializacion
- Rutas principales:
  - `/inventory`
  - `/inventory/balances`
  - `/inventory/transactions`
  - `/inventory/adjustments`
  - `/inventory/adjustments/new`
  - `/inventory/adjustments/{adjustment_id}`
  - `/inventory/initialize-opening-balances`
- Templates principales:
  - [inventory_home.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\inventory_home.html)
  - [inventory_balances.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\inventory_balances.html)
  - [inventory_transactions.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\inventory_transactions.html)
  - [inventory_adjustments_list.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\inventory_adjustments_list.html)
- Modelos relacionados:
  - `InventoryTransaction`
  - `InventoryBalance`
  - `InventoryAdjustment`
  - `InventoryAdjustmentPostToken`
- Permisos relevantes:
  - `inventory.view`
  - `inventory.adjust`
- Acciones principales:
  - consultar balances y kardex
  - crear ajustes
  - inicializar saldos

## Planning

- Proposito: parametros de inventario, sugerencias y handoff a compras/produccion
- Rutas principales:
  - `/planning`
  - `/planning/customer-order-requirements`
  - `/planning/inventory-parameters`
  - `/planning/suggestions`
  - `/planning/mps`
  - `/planning/mrp`
- Templates principales:
  - [planning_home.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\planning_home.html)
  - [customer_order_requirements.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\customer_order_requirements.html)
  - [inventory_parameters.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\inventory_parameters.html)
  - [planning_suggestions.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\planning_suggestions.html)
  - [mps_report.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\mps_report.html)
  - [mrp_report.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\mrp_report.html)
- Modelos relacionados:
  - `Product`
  - `PurchaseOrder`
  - `ProductionOrder`
  - `InventoryBalance`
- Permisos relevantes:
  - `planning.view`
  - `planning.edit_parameters`
  - `planning.edit_moq`
  - `planning.edit_zones`
- Acciones principales:
  - configurar parametros
  - ver sugerencias
  - refrescar inventario/costo
  - crear OP desde planning

## Audit Logs

- Proposito: trazabilidad de acciones sensibles
- Rutas principales:
  - `/admin/audit-logs`
  - `/admin/audit-logs/{log_id}`
- Templates principales:
  - [audit_logs_list.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\audit_logs_list.html)
  - [audit_log_detail.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\audit_log_detail.html)
- Modelos relacionados:
  - `AuditLog`
- Permisos relevantes:
  - `audit.view`
- Acciones principales:
  - listar eventos
  - ver detalle auditado

## Branding / Company Logo

- Proposito: branding administrado desde DB
- Rutas principales:
  - `/admin/branding`
  - `POST /admin/branding/logo`
  - `/branding/logo`
- Templates principales:
  - [admin_branding.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\admin_branding.html)
  - [base.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\base.html)
- Modelos relacionados:
  - `AppSettings`
- Permisos relevantes:
  - `admin.users.manage`
- Acciones principales:
  - ver branding actual
  - subir/cambiar logo
  - fallback a `Green Corner` si no hay logo

## Mobile Navigation

- Proposito: hacer usable el header en celulares
- Rutas principales:
  - no agrega rutas nuevas; afecta el layout global del header
- Templates principales:
  - [base.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\base.html)
- Archivos relacionados:
  - [style.css](C:\Users\rafae\Documents\mvp-costeo-produccion\app\static\style.css)
- Permisos relevantes:
  - conserva condiciones existentes de Branding y Audit Logs
- Acciones principales:
  - mostrar hamburguesa en mobile
  - desplegar links principales y bloque de usuario
  - mantener Logout como POST
