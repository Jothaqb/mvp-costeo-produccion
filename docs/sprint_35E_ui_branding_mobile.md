# Sprint 35E - UI, Branding and Mobile Navigation

Estado: Aprobado / Cerrado

## 1. Contexto

El ERP ya esta publicado en:

https://erp.greencornercr.com

Durante 35E se realizaron mejoras visuales, branding y navegacion movil.

## 2. 35E.1 - UI Polish and Button Consistency

- Se estandarizaron botones principales con el estilo verde `.button`.
- Se quitaron botones redundantes como Back to Sales.
- Se ajustaron botones en:
  - B2B Sales Orders
  - B2C Sales Orders
  - B2B/B2C Import Historical
  - B2B/B2C Customers
  - Products / SKUs
  - Product Detail
  - Product Categories
  - Suppliers
  - Discounts
  - Planning / Purchase Orders
- En Purchase Orders:
  - Status quedo como texto normal.
  - Actions quedo como link azul/subrayado.
- Fondo general del ERP paso a blanco.

## 3. 35E.2 - Logo Branding en DB

- Se agrego branding con logo de empresa.
- El logo se guarda en PostgreSQL, no en filesystem local.
- Se agrego modelo/tabla AppSettings.
- El logo se muestra en el header.
- Si no hay logo, se mantiene fallback `Green Corner`.
- El logo se administra desde:

  `/admin/branding`

- Solo usuarios con permiso:

  `admin.users.manage`

  pueden subir/cambiar el logo.

- Archivos validos:
  - PNG
  - JPG/JPEG
  - WebP

- Tamano maximo:
  - 1 MB

- El endpoint:

  `/branding/logo`

  sirve la imagen del logo.

## 4. 35E.3 - Logout Link Style + White Background Cleanup

- Logout sigue usando `POST /logout`.
- Logout fue ajustado visualmente para verse como Change password.
- Se removio el fondo/recuadro visual del boton Logout.
- Se reforzo fondo blanco general.

## 5. 35E.4 - Header Logo Layout

- Se ajusto el header para mantener el logo a la izquierda.
- El logo queda dentro de un cuadro blanco cuadrado.
- El logo queda centrado verticalmente dentro de la franja negra.
- Se evito superposicion con los menus.

## 6. 35E.5 - Mobile Hamburger Navigation

- Se agrego menu hamburguesa para moviles.
- En desktop el header se mantiene normal.
- En movil:
  - se muestra logo
  - se muestra boton hamburguesa
  - el menu queda oculto inicialmente
  - al tocar hamburguesa se despliegan los links principales y el bloque de usuario
- Logout sigue siendo POST.
- No se cambiaron permisos, rutas ni backend operativo.

## 7. Validaciones realizadas

- Desktop validado.
- Mobile validado.
- Logout validado.
- Branding validado.
- Render deploy validado.
- Dominio validado:
  https://erp.greencornercr.com

## 8. Notas operativas

- Para cambiar logo:
  1. Ingresar como admin.
  2. Ir a `/admin/branding`.
  3. Subir PNG/JPG/WebP menor a 1 MB.
  4. Validar que aparezca en el header.

- Para cambios futuros de UI:
  - probar local
  - commit
  - push a GitHub
  - validar deploy en Render
  - refrescar con `Ctrl + F5` si hay cache de CSS

## 9. Que no se toco

- No se tocaron datos operativos.
- No se modificaron ventas, compras, produccion, inventario ni planning.
- No se cambiaron roles ni permisos.
- No se cambio `DATABASE_URL`.
- No se cambio dominio.
- No se corrieron migraciones destructivas.
