# Architecture Overview

## Resumen

El sistema actual es un ERP operativo liviano para Green Corner. Usa una arquitectura server-rendered clasica: FastAPI recibe requests HTTP, aplica autenticacion/permisos, consulta o actualiza SQLAlchemy models, delega logica a servicios y responde con templates Jinja2.

## Capas principales

### Backend

- Framework: FastAPI
- Archivo principal: [app/main.py](C:\Users\rafae\Documents\mvp-costeo-produccion\app\main.py)
- Responsabilidades:
  - inicializacion de la app
  - registro de rutas
  - middleware de autenticacion
  - branding state para header
  - render de templates
  - validaciones de permisos por request

### Frontend server-rendered

- Base layout: [app/templates/base.html](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates\base.html)
- Templates de modulo: [app/templates](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates)
- CSS principal: [app/static/style.css](C:\Users\rafae\Documents\mvp-costeo-produccion\app\static\style.css)

La UI no usa un frontend SPA. La mayoria de las pantallas son HTML renderizado en servidor, con JS minimo para interacciones puntuales como el menu hamburguesa del header.

### Base de datos

- Desarrollo local: SQLite (`costeo.db`)
- Produccion inicial: PostgreSQL Render via `DATABASE_URL`
- Configuracion: [app/database.py](C:\Users\rafae\Documents\mvp-costeo-produccion\app\database.py)

### ORM

- SQLAlchemy declarative models
- Definiciones en: [app/models.py](C:\Users\rafae\Documents\mvp-costeo-produccion\app\models.py)

### Servicios

La logica de negocio no vive toda en `main.py`. El proyecto usa servicios en:

- [app/services](C:\Users\rafae\Documents\mvp-costeo-produccion\app\services)

Servicios relevantes:

- auth y sesiones
- auditoria
- BOM
- ventas B2B/B2C
- planning
- inventory ledger y adjustments
- production orders
- purchase orders
- imports historicos
- configuracion y Loyverse mappings

### Scripts operativos

Scripts fuera de la app web:

- [scripts/bootstrap_admin.py](C:\Users\rafae\Documents\mvp-costeo-produccion\scripts\bootstrap_admin.py)
- [scripts/migrate_sqlite_to_postgres.py](C:\Users\rafae\Documents\mvp-costeo-produccion\scripts\migrate_sqlite_to_postgres.py)
- [scripts/create_users_and_roles.py](C:\Users\rafae\Documents\mvp-costeo-produccion\scripts\create_users_and_roles.py)

Estos scripts son sensibles porque pueden crear usuarios, sincronizar roles o escribir en PostgreSQL.

## Request flow

Flujo tipico:

1. navegador envia request
2. FastAPI recibe la ruta en [app/main.py](C:\Users\rafae\Documents\mvp-costeo-produccion\app\main.py)
3. middleware de autenticacion determina sesion actual, branding y acceso
4. la ruta valida permisos con `require_permission(...)` cuando aplica
5. la ruta usa SQLAlchemy directamente o delega en un servicio
6. se actualiza o consulta DB
7. se renderiza template Jinja2
8. se devuelve HTML al navegador

## Archivos clave

- [app/main.py](C:\Users\rafae\Documents\mvp-costeo-produccion\app\main.py)  
  Punto central de rutas, middleware, auth, branding y wiring general.

- [app/models.py](C:\Users\rafae\Documents\mvp-costeo-produccion\app\models.py)  
  Modelos ORM del ERP: operativos, auth, auditoria, branding y soporte.

- [app/database.py](C:\Users\rafae\Documents\mvp-costeo-produccion\app\database.py)  
  Engine, `SessionLocal`, `Base`, `DATABASE_URL` y helpers `ensure_*` para schema local.

- [app/services](C:\Users\rafae\Documents\mvp-costeo-produccion\app\services)  
  Logica de negocio reutilizable.

- [app/templates](C:\Users\rafae\Documents\mvp-costeo-produccion\app\templates)  
  Templates HTML por modulo.

- [app/static/style.css](C:\Users\rafae\Documents\mvp-costeo-produccion\app\static\style.css)  
  Estilos globales, header, branding, layout y responsive/mobile navigation.

- [scripts](C:\Users\rafae\Documents\mvp-costeo-produccion\scripts)  
  Herramientas administrativas y de migracion.

## Local vs produccion inicial

### Local

- usa SQLite si `DATABASE_URL` no esta definido
- `app/database.py` incluye varios helpers `ensure_*` para evolucionar el schema local sin un sistema formal de migraciones
- ideal para desarrollo y validacion funcional

### Produccion inicial

- usa PostgreSQL Render via `DATABASE_URL`
- dominio activo: [https://erp.greencornercr.com](https://erp.greencornercr.com)
- servicio actual: `erp-green-corner-staging`
- aunque el nombre dice staging, opera como produccion inicial

## Hosting y source of truth

- Hosting: Render
- Dominio: `erp.greencornercr.com`
- Source of truth del codigo: GitHub / rama `master`

## Notas de arquitectura

- `Base.metadata.create_all(bind=engine)` crea tablas nuevas pero no reemplaza migraciones formales para cambios complejos sobre tablas existentes.
- El proyecto mantiene una mezcla pragmatica de:
  - rutas en `main.py`
  - logica de negocio en `services/`
  - schema evolution local en `database.py`
- Esa arquitectura ha funcionado bien para iterar rapido, pero conviene documentarla porque ya no es un MVP pequeno.
