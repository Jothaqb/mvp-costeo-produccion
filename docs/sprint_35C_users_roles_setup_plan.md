# Sprint 35C — Users + Roles Setup Plan

Estado: Aprobado / Cerrado

## 1. Estado actual

- ERP publicado en:
  https://erp.greencornercr.com

- Producción inicial corriendo en Render.
- PostgreSQL Render contiene datos reales migrados.
- Login admin funciona.
- Backups y protocolo de cambios ya documentados en 35B.
- Aún no se han creado usuarios reales nominales.
- Aún se usa admin para administración/control inicial.

## 2. Permisos existentes

Lista completa de 38 permisos:

- `admin.roles.manage`
- `admin.users.manage`
- `audit.view`
- `b2b_customer_products.edit_prices`
- `bom.create`
- `bom.delete`
- `bom.edit`
- `bom.view`
- `inventory.adjust`
- `inventory.view`
- `planning.edit_moq`
- `planning.edit_parameters`
- `planning.edit_zones`
- `planning.view`
- `product.create`
- `product.edit`
- `product.edit_cost`
- `product.edit_prices`
- `product.export`
- `product.view`
- `production_order.close`
- `production_order.create`
- `production_order.edit`
- `production_order.import`
- `production_order.view`
- `purchase_order.create`
- `purchase_order.edit`
- `purchase_order.import`
- `purchase_order.receive`
- `purchase_order.view`
- `reporting.export`
- `reporting.view`
- `sales.create`
- `sales.edit`
- `sales.export`
- `sales.import`
- `sales.invoice`
- `sales.view`

## 3. Clasificación por módulo

**Admin**
- `admin.users.manage`
- `admin.roles.manage`

**Audit**
- `audit.view`

**Product / Master Data**
- `product.view`
- `product.create`
- `product.edit`
- `product.edit_prices`
- `product.edit_cost`
- `product.export`

**BOM**
- `bom.view`
- `bom.create`
- `bom.edit`
- `bom.delete`

**B2B Customer Pricing**
- `b2b_customer_products.edit_prices`

**Sales**
- `sales.view`
- `sales.create`
- `sales.edit`
- `sales.invoice`
- `sales.import`
- `sales.export`

**Reporting**
- `reporting.view`
- `reporting.export`

**Purchase Orders**
- `purchase_order.view`
- `purchase_order.create`
- `purchase_order.edit`
- `purchase_order.receive`
- `purchase_order.import`

**Production Orders**
- `production_order.view`
- `production_order.create`
- `production_order.edit`
- `production_order.close`
- `production_order.import`

**Inventory**
- `inventory.view`
- `inventory.adjust`

**Planning**
- `planning.view`
- `planning.edit_parameters`
- `planning.edit_moq`
- `planning.edit_zones`

## 4. Permisos de lectura

Permisos de lectura:

- `audit.view`
- `bom.view`
- `inventory.view`
- `planning.view`
- `product.view`
- `purchase_order.view`
- `production_order.view`
- `reporting.view`
- `sales.view`

Permisos de lectura sensible / salida de datos:

- `product.export`
- `reporting.export`
- `sales.export`

## 5. Permisos sensibles

Permisos sensibles/write:

- `product.create`
- `product.edit`
- `product.edit_prices`
- `product.edit_cost`
- `bom.create`
- `bom.edit`
- `bom.delete`
- `b2b_customer_products.edit_prices`
- `sales.create`
- `sales.edit`
- `sales.invoice`
- `sales.import`
- `purchase_order.create`
- `purchase_order.edit`
- `purchase_order.receive`
- `purchase_order.import`
- `production_order.create`
- `production_order.edit`
- `production_order.close`
- `production_order.import`
- `inventory.adjust`
- `planning.edit_parameters`
- `planning.edit_moq`
- `planning.edit_zones`
- `admin.users.manage`
- `admin.roles.manage`

## 6. Permisos de máximo control

Quedan solo para admin técnico o uso extraordinariamente controlado:

- `admin.users.manage`
- `admin.roles.manage`
- `product.edit_cost`
- `sales.import`
- `purchase_order.import`
- `production_order.import`

Quedan sujetos a aprobación explícita:

- `inventory.adjust`
- `sales.invoice`
- `production_order.close`
- `purchase_order.receive`
- `product.edit_prices`
- `b2b_customer_products.edit_prices`

## 7. Roles iniciales recomendados

1. Admin técnico
2. Dirección / Gerencia
3. Comercial / Ventas
4. Compras
5. Producción
6. Inventario / Planning
7. Reporting / Auditoría

## 8. Matriz de permisos propuesta

| Rol | Módulos visibles | Permisos de lectura | Permisos de escritura | Acciones sensibles permitidas | Acciones prohibidas | Comentario operativo |
|---|---|---|---|---|---|---|
| Admin técnico | Todos | Todos los `*.view` | Todos los permisos existentes | Usuarios, roles, imports, costos, pricing, ajustes, cierres, facturación, auditoría | Ninguna a nivel app | Uso restringido para soporte/configuración. No usar para operación diaria. |
| Dirección / Gerencia | Productos, BOM, ventas, reportes, inventario, planning, compras, producción, auditoría | `product.view`, `bom.view`, `sales.view`, `reporting.view`, `inventory.view`, `planning.view`, `purchase_order.view`, `production_order.view`, `audit.view` | Ninguna por default | `reporting.export` opcional | Imports, ajustes, pricing/costos, facturación, receives, cierres, users/roles | Rol de supervisión y control. |
| Comercial / Ventas | Productos, ventas, reportes básicos | `product.view`, `sales.view`, `reporting.view` | `sales.create`, `sales.edit` | Ninguna al inicio | `sales.invoice`, imports, costos, inventario, producción, auditoría | Recomendación conservadora: sin facturación al inicio. |
| Compras | Productos, compras | `product.view`, `purchase_order.view` | `purchase_order.create`, `purchase_order.edit` | `purchase_order.receive` solo si se aprueba | Imports al inicio, ventas, producción, costos históricos, auditoría | Receive sujeto a aprobación operativa. |
| Producción | Producción, productos, BOM | `production_order.view`, `product.view`, `bom.view` | `production_order.create`, `production_order.edit` | `production_order.close` solo si se aprueba | Ventas, precios, usuarios, `inventory.adjust`, imports | Arranque conservador: cierre sujeto a aprobación. |
| Inventario / Planning | Inventario, planning, productos, compras lectura | `inventory.view`, `planning.view`, `product.view`, `purchase_order.view` | `planning.edit_parameters`, `planning.edit_moq`, `planning.edit_zones` | `inventory.adjust` solo si se aprueba | Ventas, costos, usuarios, imports | Rol operativo de planning con ajuste de inventario restringido. |
| Reporting / Auditoría | Reportes, ventas lectura, auditoría | `reporting.view`, `sales.view`, `audit.view` | Ninguna | `reporting.export` opcional | Todas las acciones operativas | Rol de lectura y control. |

## 9. Decisiones pendientes de aprobación

Requieren aprobación manual:

- Si Comercial puede facturar.
- Si Compras puede recibir purchase orders.
- Si Producción puede cerrar órdenes.
- Si Inventario/Planning puede crear ajustes.
- Quién tendrá `audit.view`.
- Quién tendrá `reporting.export`.
- Quién podrá editar precios.
- Quién podrá editar costos.
- Quién podrá ejecutar imports.

## 10. Formato de usuarios reales

Formato sugerido:

- `username`
- `full_name`
- `email`
- rol asignado
- `is_active = True`
- `must_change_password = True` si el flujo lo soporta
- password temporal entregado por canal seguro

Convención recomendada:

- `nombre.apellido`
- o `inicialapellido`

Reglas:

- no cuentas compartidas
- cada persona con usuario propio
- admin de emergencia separado
- password temporal por canal seguro
- cambio de password en primer ingreso si aplica

## 11. Reglas de seguridad

- Admin técnico solo para soporte/configuración.
- No usar admin para operación diaria.
- No compartir contraseñas.
- Desactivar usuarios que ya no trabajen.
- Revisar roles periódicamente.
- Acciones críticas deben quedar auditadas.
- No crear usuarios desde DB manualmente salvo emergencia documentada.

## 12. Validaciones posteriores al crear usuarios

En el siguiente sprint se debe validar:

- Login de cada usuario.
- Usuario ve solo módulos autorizados.
- Usuario no ve botones prohibidos.
- Backend devuelve `403` ante `POST` no autorizado.
- Logout funciona.
- Change password funciona.
- Audit logs solo visibles para rol autorizado.
- Admin conserva todos los permisos.

## 13. Qué queda fuera de 35C

- no se crean usuarios todavía
- no se cambian permisos todavía
- no se crean roles en DB todavía
- no se modifica código
- no se toca Render
- no se tocan datos

## 14. Recomendación final

Empezar conservador.

No dar inicialmente:

- `sales.invoice`
- `purchase_order.receive`
- `production_order.close`
- `inventory.adjust`
- `product.edit_cost`
- imports
- `admin.users.manage`
- `admin.roles.manage`

excepto al admin técnico.
