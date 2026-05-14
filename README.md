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

Para crear el primer admin en Render, usar Render Shell o un comando one-off. La idea segura es:

1. abrir una sesión de base de datos
2. ejecutar `ensure_auth_seed_state(db)`
3. crear el admin solo si no existen usuarios activos
4. hacer `commit`

Ese procedimiento no debe borrar datos ni sobrescribir usuarios existentes.
