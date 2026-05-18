# UI, Branding and Mobile Navigation

Documento estable de referencia para los cambios de UI y branding del bloque 35E.

Para el cierre historico del sprint ver tambien:

- [sprint_35E_ui_branding_mobile.md](C:\Users\rafae\Documents\mvp-costeo-produccion\docs\sprint_35E_ui_branding_mobile.md)

## 35E.1 - UI polish and button consistency

Se realizaron ajustes visuales sin tocar backend operativo:

- estandarizacion de botones principales con el estilo verde `.button`
- remocion de botones redundantes como `Back to Sales`
- armonizacion de botones en:
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
- en Purchase Orders:
  - `Status` quedo como texto normal
  - `Actions` quedo como link azul/subrayado
- fondo general del ERP reforzado a blanco

## 35E.2 - Logo branding en DB

Se agrego branding administrado desde DB.

### Rutas

- `/admin/branding`
- `POST /admin/branding/logo`
- `/branding/logo`

### Persistencia

- el logo se guarda en PostgreSQL
- no se guarda en filesystem local
- la tabla usada es `AppSettings`

### Reglas de archivo

Formatos soportados:

- PNG
- JPG / JPEG
- WebP

Tamano maximo:

- 1 MB

### Permiso

Solo usuarios con:

- `admin.users.manage`

pueden subir o cambiar logo.

### Header

- si existe logo, se muestra en el header
- si no existe logo, el fallback sigue siendo `Green Corner`
- el logo se sirve por `/branding/logo`

## 35E.3 - Logout visual + white background cleanup

- Logout sigue siendo `POST /logout`
- visualmente se ajusto para verse como `Change password`
- se elimino el recuadro/fondo visual del boton
- se reforzo el fondo blanco general

## 35E.4 - Header logo layout

- el logo queda a la izquierda
- el logo queda dentro de un cuadro blanco cuadrado
- el logo queda centrado verticalmente respecto a la franja negra del header
- se evito superposicion con menus
- no se uso `position: absolute` en la version final del logo del header

## 35E.5 - Mobile hamburger navigation

En desktop:

- el header se mantiene visible y completo

En mobile:

- se muestra logo
- se muestra boton hamburguesa
- el menu queda oculto inicialmente
- al tocar hamburguesa se despliegan:
  - links principales
  - bloque de usuario

El bloque desplegable mantiene:

- Branding
- Audit Logs
- Change password
- Logout

segun las mismas condiciones de permisos que ya existian.

## Notas operativas

### Cambiar logo

1. ingresar como admin
2. ir a `/admin/branding`
3. subir PNG/JPG/WebP menor a 1 MB
4. validar que aparezca en el header

### Reglas importantes

- no guardar logos en `static/uploads`
- no tocar filesystem local del servicio
- mantener Logout como POST
- no alterar permisos existentes por cambios puramente visuales
