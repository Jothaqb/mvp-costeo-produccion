# Database and Migration

## Bases de datos del proyecto

### Local

- archivo local principal: `costeo.db`
- dialect por defecto cuando no existe `DATABASE_URL`: SQLite

### Produccion inicial

- base de datos: PostgreSQL Render
- conexion: variable `DATABASE_URL`

## Configuracion tecnica

Archivo clave:

- [app/database.py](C:\Users\rafae\Documents\mvp-costeo-produccion\app\database.py)

Comportamiento:

- si `DATABASE_URL` existe, la app intenta usarla
- si `DATABASE_URL` no existe, usa `sqlite:///./costeo.db`
- URLs `postgres://` se normalizan a `postgresql://`

## Models y schema

Los modelos viven en:

- [app/models.py](C:\Users\rafae\Documents\mvp-costeo-produccion\app\models.py)

La app ejecuta:

```python
Base.metadata.create_all(bind=engine)
```

Eso permite crear tablas nuevas si no existen.

## Limites de `create_all`

`create_all` es util para:

- crear tablas nuevas

No debe tratarse como reemplazo de migraciones formales cuando se necesita:

- alterar tablas existentes
- renombrar columnas
- transformar datos
- borrar columnas

## Helpers `ensure_*` en `database.py`

`app/database.py` incluye varios helpers `ensure_*` que sirven sobre todo para mantener compatibilidad del schema local SQLite a medida que el proyecto fue creciendo.

Ejemplos:

- columnas nuevas de producto
- tablas de auth
- tablas de auditoria
- tablas maestras

Nota importante:

- muchos de esos helpers son SQLite-only
- produccion PostgreSQL depende principalmente de models + `create_all` + schema ya migrado

## Migracion SQLite -> PostgreSQL

Script:

- [scripts/migrate_sqlite_to_postgres.py](C:\Users\rafae\Documents\mvp-costeo-produccion\scripts\migrate_sqlite_to_postgres.py)

### Proposito

Migrar datos del ERP desde SQLite local hacia PostgreSQL.

### Modos principales

- `--dry-run`
  - inspecciona SQLite
  - inspecciona PostgreSQL si `DATABASE_URL` existe
  - no escribe

- migracion real
  - requiere `--reset-target`
  - requiere confirmar escribiendo `MIGRATE`

### Validaciones que realiza

- inventario de tablas
- row counts
- min/max IDs
- aggregates relevantes
- diffs de schema
- orden de migracion seguro por FK
- reset de sequences al final

### Tablas excluidas

Por diseno, excluye tablas efimeras o de tokens:

- `user_sessions`
- `inventory_adjustment_post_tokens`
- `purchase_order_receive_tokens`

## Advertencias

- no correr migraciones sin aprobacion explicita
- no usar `--reset-target` sin backup y confirmacion
- no asumir que `DATABASE_URL` apunta al target correcto
- no imprimir la URL completa
- no correr contra produccion inicial sin smoke previo y plan de rollback

## Relacion entre SQLite y PostgreSQL

- SQLite sigue siendo el entorno local por defecto
- PostgreSQL es la base persistente de produccion inicial
- el proyecto fue migrado desde SQLite a PostgreSQL con tooling controlado, no con una capa formal de migraciones versionadas
