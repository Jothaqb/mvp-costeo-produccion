# Operations Runbook

## Objetivo

Definir el flujo seguro de cambios y soporte para la produccion inicial del ERP Green Corner.

## Flujo oficial de cambios

1. probar localmente
2. revisar archivos modificados
3. crear commit
4. `git push origin master`
5. ejecutar o validar deploy en Render
6. validar produccion

## Antes de cambios criticos

Cambios criticos incluyen:

- scripts que escriben DB
- migraciones
- cambios de auth/permisos
- imports historicos grandes
- cambios de inventario, costing o planning
- cambios de `DATABASE_URL`

Antes de correrlos:

- confirmar backup o PITR disponible
- definir rollback
- validar target
- registrar aprobacion explicita

## Backups

Base de politica actual:

- usar backups/PITR nativos de Render segun el plan contratado
- restaurar PITR en una DB nueva, no encima de la actual
- validar la DB restaurada antes de recablear el servicio

## Rollback de deploy

Si falla el codigo o el deploy:

1. usar rollback de Render a deploy previo, o
2. redeploy del commit anterior
3. validar login
4. validar rutas criticas
5. revisar logs

## Rollback de DB / PITR

Si falla la base de datos o se dañan datos:

1. no corregir a ciegas
2. detener writes si aplica
3. restaurar PITR en una nueva DB
4. validar la DB restaurada
5. solo despues cambiar `DATABASE_URL`

## Si falla el dominio

Usar inmediatamente el fallback:

- [https://erp-green-corner-staging.onrender.com](https://erp-green-corner-staging.onrender.com)

Luego:

- revisar DNS Shopify/CNAME
- confirmar estado del custom domain en Render

## Si falla login

Checklist:

- confirmar que el servicio este Live
- revisar logs de app
- validar que `DATABASE_URL` apunte al entorno correcto
- confirmar que la DB tenga usuarios activos
- si se trata de bootstrap inicial, usar `scripts/bootstrap_admin.py` solo con aprobacion

## Si falla DB

Checklist:

- confirmar dialect esperado
- confirmar `DATABASE_URL` del servicio
- revisar si hubo script sensible o migracion reciente
- revisar conectividad de Postgres en Render
- si hubo daño de datos, aplicar protocolo de PITR

## Checklists

### Antes de deploy

- repo limpio o cambios esperados
- commit identificado
- no incluir `.db`, backups, logs ni credenciales
- backup confirmado si el cambio toca DB
- rollback definido si el cambio es critico
- validacion local realizada
- aprobacion explicita registrada si corresponde

### Despues de deploy

- servicio Live
- login OK
- rutas criticas OK
- dominio OK
- fallback OK
- logs sin `500`
- si hubo cambio de DB: smoke funcional y conteos relevantes

## Limpieza de `DATABASE_URL` local

Si se define localmente para una prueba operativa, limpiar despues:

```powershell
set DATABASE_URL=
```

## Regla general

Nunca correr cambios sensibles en produccion inicial sin:

- validar `DATABASE_URL`
- confirmar backup
- tener aprobacion explicita
