# Sensitive Scripts

Advertencia general:

Nunca correr scripts sensibles contra produccion sin validar `DATABASE_URL` y sin aprobacion explicita.

## 1. `scripts/bootstrap_admin.py`

### Proposito

Crear el primer usuario admin cuando todavia no existen usuarios activos.

### Cuando se usa

- bootstrap inicial de un entorno nuevo
- soporte controlado en Render Shell o terminal local apuntando al entorno correcto

### Variables requeridas

- `DATABASE_URL` si debe operar sobre PostgreSQL/Render

### Riesgo

- alto si se apunta al entorno equivocado

### Prechecks

- validar target
- confirmar que no existan usuarios activos
- confirmar que el password se va a ingresar de forma segura

### Confirmaciones requeridas

- username esperado
- target correcto
- password ingresado via `getpass`

### Que NO hacer

- no usarlo para sobreescribir usuarios existentes
- no pasar password por CLI
- no imprimir credenciales
- no correrlo rutinariamente en produccion si ya existen usuarios

### Validaciones posteriores

- login OK
- usuario admin activo
- sesion y rutas admin operativas

## 2. `scripts/migrate_sqlite_to_postgres.py`

### Proposito

Inspeccionar y migrar datos desde SQLite local a PostgreSQL.

### Cuando se usa

- migraciones controladas de datos
- validacion previa a cutover

### Variables requeridas

- `DATABASE_URL` para inspeccion o escritura sobre PostgreSQL

### Riesgo

- muy alto

### Prechecks

- validar source SQLite
- validar target PostgreSQL
- confirmar backups/PITR
- correr `--dry-run` primero
- revisar schema diffs

### Confirmaciones requeridas

- para migracion real requiere:
  - `--reset-target`
  - confirmacion manual `MIGRATE`

### Que NO hacer

- no correr sin backup
- no correr `--reset-target` sin aprobacion
- no correr contra produccion sin smoke previo
- no asumir que `DATABASE_URL` es correcta

### Validaciones posteriores

- counts
- min/max IDs
- aggregates
- sequences
- login y rutas criticas

## 3. `scripts/create_users_and_roles.py`

### Proposito

- sincronizar roles operativos
- asegurar permisos exactos
- crear usuarios nominales aprobados

### Cuando se usa

- setup de roles
- creacion controlada de usuarios nominales

### Variables requeridas

- target DB correcto via configuracion de la app / `DATABASE_URL`

### Riesgo

- alto

### Prechecks

- correr `--preview`
- validar que el target sea el correcto
- validar que usernames/emails no existan
- validar roles requeridos
- decidir `--password-mode`

### Confirmaciones requeridas

- para crear usuarios requiere escribir exactamente:
  - `CREATE_USERS`

### Que NO hacer

- no usar passwords de pruebas temporales en produccion
- no persistir passwords generados
- no correr contra DB equivocada
- no crear usuarios fuera de la lista aprobada sin proceso de aprobacion

### Validaciones posteriores

- roles correctos
- usuarios creados esperados
- `must_change_password = True`
- `is_active = True`
- admin conserva todos los permisos

## Reglas comunes para scripts sensibles

- validar `DATABASE_URL`
- confirmar backup si el script modifica datos
- revisar `git status` y contexto del cambio
- registrar aprobacion manual
- no imprimir secretos
- no ejecutar a ciegas desde una shell con variables viejas
