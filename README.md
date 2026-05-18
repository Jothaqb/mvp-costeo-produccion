# Green Corner ERP

ERP operativo liviano para Green Corner construido con FastAPI, Jinja2 y SQLAlchemy.

La aplicacion ya no es solo una herramienta de costeo. Empezo como un MVP para costeo real de produccion, pero hoy cubre seguridad, datos maestros, ventas, compras, produccion, inventario, planning, auditoria, branding y navegacion movil.

## Estado actual

- Dominio productivo inicial: [https://erp.greencornercr.com](https://erp.greencornercr.com)
- Servicio Render actual: `erp-green-corner-staging`
- Nota operativa: aunque conserva nombre `staging`, hoy funciona como produccion inicial
- Base de datos local por defecto: `sqlite:///./costeo.db`
- Base de datos de produccion: PostgreSQL Render via `DATABASE_URL`

## Stack tecnico

- Python
- FastAPI
- Jinja2 templates
- SQLAlchemy
- SQLite local
- PostgreSQL Render
- Render Web Service

## Modulos principales

- Auth / users / roles / permissions
- Master Data
- Products / SKUs
- Categories
- Suppliers
- Discounts
- BOM
- B2B customer pricing
- Sales
- B2B orders
- B2C orders
- Purchase Orders
- Production Orders
- Production Routes
- Inventory
- Planning
- Audit Logs
- Branding / logo
- Mobile navigation

## Desarrollo local

Flujo recomendado:

```powershell
conda activate costeo-mvp
uvicorn app.main:app --host 127.0.0.1 --port 8020
```

Si `DATABASE_URL` no esta definido, la app usa SQLite local:

- `sqlite:///./costeo.db`

Si `DATABASE_URL` existe, la app intenta usar esa base. El codigo normaliza automaticamente URLs `postgres://` a `postgresql://`.

## Deploy

Flujo operativo recomendado:

1. hacer cambios locales
2. validar localmente
3. crear commit
4. `git push origin master`
5. ejecutar o verificar deploy en Render
6. validar el dominio principal y el fallback

Start command documentado para Render:

```bash
uvicorn app.main:app --host 0.0.0.0 --port $PORT
```

## Scripts sensibles

No correr estos scripts sin validar el target y sin aprobacion explicita cuando apunten a produccion:

- `scripts/bootstrap_admin.py`
- `scripts/migrate_sqlite_to_postgres.py`
- `scripts/create_users_and_roles.py`

## Documentacion

La documentacion viva del sistema esta indexada en:

- [docs/README.md](C:\Users\rafae\Documents\mvp-costeo-produccion\docs\README.md)

Documentos historicos que siguen siendo utiles para entender el origen del proyecto, pero que ya no representan completamente el ERP actual:

- [PRD.md](C:\Users\rafae\Documents\mvp-costeo-produccion\PRD.md)
- [MVP_SCOPE.md](C:\Users\rafae\Documents\mvp-costeo-produccion\MVP_SCOPE.md)
- [SPRINT_1.md](C:\Users\rafae\Documents\mvp-costeo-produccion\SPRINT_1.md)
- [SPRINT_2.md](C:\Users\rafae\Documents\mvp-costeo-produccion\SPRINT_2.md)
- [SPRINT_3.md](C:\Users\rafae\Documents\mvp-costeo-produccion\SPRINT_3.md)
- [SPRINT_4.md](C:\Users\rafae\Documents\mvp-costeo-produccion\SPRINT_4.md)
- [SPRINT_5.md](C:\Users\rafae\Documents\mvp-costeo-produccion\SPRINT_5.md)
