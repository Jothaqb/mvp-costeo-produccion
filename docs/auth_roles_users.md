# Auth, Roles and Users

## Flujo de autenticacion

### Login

- ruta: `/login`
- la app crea una sesion persistida
- el nombre de la cookie de sesion es `erp_session`

### Logout

- ruta: `POST /logout`
- el header lo presenta como link visual, pero sigue siendo form POST
- no debe convertirse en GET

### Change password

- rutas:
  - `GET /auth/change-password`
  - `POST /auth/change-password`
- al cambiar la contrasena:
  - `must_change_password` pasa a `False`

### Bootstrap inicial de admin

- flujo web restringido:
  - `/auth/bootstrap-admin`
- script operativo:
  - `scripts/bootstrap_admin.py`

## `must_change_password`

El modelo `User` tiene el campo `must_change_password`.

Uso actual:

- usuarios nominales nuevos se crean con `must_change_password = True`
- el flujo de cambio de password existe y limpia esa marca

## Roles actuales

Roles operativos vigentes:

- `admin`
- `general_operator`
- `general_approver`

## Diferencia entre roles

### `admin`

Uso esperado:

- soporte
- configuracion
- emergencia

No debe usarse para operacion diaria.

Capacidades:

- conserva todos los permisos existentes
- incluye users/roles, audit, imports, costos, pricing, cierres y ajustes

### `general_operator`

Capacidades principales:

- opera modulos centrales del ERP
- puede trabajar productos, ventas, compras, produccion, inventario y planning dentro del set aprobado

Restricciones relevantes:

- no puede editar BOM
- no puede editar precios
- no puede editar costos
- no puede administrar usuarios/roles
- no puede ejecutar imports historicos
- no puede ver audit logs

### `general_approver`

Capacidades adicionales frente a `general_operator`:

- puede editar BOM
- puede editar precios de producto
- puede editar precios B2B por cliente

Restricciones relevantes:

- no puede editar costos
- no puede administrar usuarios/roles
- no puede ejecutar imports historicos
- no puede ver audit logs

## Permisos delicados

Permisos de alto control o alta sensibilidad:

- `admin.users.manage`
- `admin.roles.manage`
- `product.edit_cost`
- `sales.import`
- `purchase_order.import`
- `production_order.import`
- `audit.view`

Permisos operativos sensibles:

- `inventory.adjust`
- `sales.invoice`
- `purchase_order.receive`
- `production_order.close`
- `product.edit_prices`
- `b2b_customer_products.edit_prices`

## Usuarios nominales creados

Usuarios operativos aprobados:

- `olivia.rincon`
- `andreina.rincon`
- `jonathan.quirosb`
- `jonathan.quiros`

No incluir contrasenas en documentacion ni en repo.

## Reglas operativas

- no usar cuentas compartidas
- cada persona debe tener usuario propio
- admin solo para configuracion/emergencia
- entregar passwords temporales por canal seguro
- limpiar `must_change_password` via el flujo oficial de cambio de password

## Scripts relacionados

- [scripts/bootstrap_admin.py](C:\Users\rafae\Documents\mvp-costeo-produccion\scripts\bootstrap_admin.py)
- [scripts/create_users_and_roles.py](C:\Users\rafae\Documents\mvp-costeo-produccion\scripts\create_users_and_roles.py)

## Permiso base usado en branding

El branding admin (`/admin/branding`) reutiliza:

- `admin.users.manage`

No se agrego un permiso nuevo especifico para branding en esta etapa.
