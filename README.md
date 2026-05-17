# MVP Costeo Producción

MVP para costear órdenes de producción en una empresa pequeña de manufactura.

## Objetivo

Calcular el costo real por lote considerando:

- materias primas
- mano de obra por etapa
- tiempo de máquina
- costos indirectos

## Stack

- Python
- FastAPI
- SQLAlchemy
- SQLite para desarrollo local
- PostgreSQL vía `DATABASE_URL` para deployment
- HTML templates

## Desarrollo local

Si `DATABASE_URL` no está definido, la app usa:

- `sqlite:///./costeo.db`

## Render / producción

Start command recomendado:

```bash
uvicorn app.main:app --host 0.0.0.0 --port $PORT
```

Variables esperadas:

- `DATABASE_URL`

Si Render entrega la URL en formato `postgres://`, la app la normaliza automáticamente a `postgresql://`.

## Primer admin en producción

El flujo web `/auth/bootstrap-admin` se mantiene restringido a localhost.

Para crear el primer admin en Render, usar Render Shell o una terminal local como comando one-off:

```bash
python scripts/bootstrap_admin.py --username admin
```

Flujo recomendado:

1. abrir Render Dashboard
2. abrir el Web Service
3. ir a Shell
4. ejecutar `python scripts/bootstrap_admin.py --username admin`
5. escribir la contraseña cuando el script la pida
6. confirmar la contraseña
7. entrar luego a `/login`

Notas de seguridad:

- solo funciona si no existen usuarios activos
- no sobrescribe usuarios existentes
- no imprime la contraseña
- no acepta password por CLI
- no usar `/auth/bootstrap-admin` públicamente
- usarlo solo como one-off de producción o smoke test

## SQLite to PostgreSQL migration dry-run

Antes de migrar datos reales, ejecutar primero un dry-run del migrador:

```bash
python scripts/migrate_sqlite_to_postgres.py --sqlite-path costeo.db --dry-run
```

Comportamiento esperado:

- inventaria tablas y conteos de SQLite
- excluye por default `user_sessions`, `inventory_adjustment_post_tokens` y `purchase_order_receive_tokens`
- inspecciona PostgreSQL solo si `DATABASE_URL` esta definido
- no imprime la `DATABASE_URL` completa
- no escribe en PostgreSQL mientras use `--dry-run`

La migracion real no debe ejecutarse sin aprobacion explicita. Cuando llegue ese momento, el script pedira escribir `MIGRATE` antes de continuar.
