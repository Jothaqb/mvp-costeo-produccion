# Deployment on Render

## Estado actual

- Dominio principal: [https://erp.greencornercr.com](https://erp.greencornercr.com)
- Fallback Render: [https://erp-green-corner-staging.onrender.com](https://erp-green-corner-staging.onrender.com)
- Web Service actual: `erp-green-corner-staging`
- Nota operativa: aunque el servicio conserva nombre `staging`, hoy funciona como produccion inicial
- Base de datos de produccion inicial: PostgreSQL Render

## Source of truth

- GitHub es la fuente oficial del codigo
- Rama principal documentada: `master`
- No editar codigo manualmente en Render

## Base de datos y `DATABASE_URL`

- El servicio usa `DATABASE_URL` para conectarse a PostgreSQL
- Localmente, si `DATABASE_URL` no existe, la app usa SQLite (`costeo.db`)
- Nunca imprimir la `DATABASE_URL` completa
- Nunca subir credenciales al repo

## Dominio y DNS

Conexion documentada:

- dominio: `erp.greencornercr.com`
- proveedor DNS/comercio: Shopify
- CNAME:
  - `erp -> erp-green-corner-staging.onrender.com`

El fallback `onrender.com` debe mantenerse documentado para contingencias.

## Start command

Comando recomendado/documentado:

```bash
uvicorn app.main:app --host 0.0.0.0 --port $PORT
```

## Flujo de deploy

Flujo recomendado:

1. hacer cambios locales
2. validar funcionalmente
3. revisar `git status`
4. crear commit claro
5. `git push origin master`
6. ejecutar o confirmar deploy en Render
7. validar dominio principal
8. validar fallback `onrender.com`

## Auto deploy vs manual deploy

La documentacion 35B recomienda deploy manual o por commit especifico para produccion inicial, especialmente en cambios sensibles.

Estado exacto de auto deploy:

- pendiente de verificar en Render Dashboard

Hasta confirmarlo, asumir que el modo de deploy debe revisarse antes de cambios criticos.

## Validaciones post deploy

Checklist minimo:

- servicio `Live`
- login OK
- rutas criticas OK:
  - `/`
  - `/master-data/products`
  - `/sales/total`
  - `/production-orders`
  - `/inventory/balances`
  - `/admin/audit-logs`
- logo/header cargan bien
- logs sin `500` ni traceback
- dominio principal funciona
- fallback `onrender.com` funciona

Si hubo cambio de DB o script sensible:

- validar conteos o smoke relevante
- validar permisos
- validar operaciones criticas afectadas

## Reglas operativas

- no editar codigo en Render
- no usar `DATABASE_URL` desconocido
- no ejecutar scripts sensibles sin validar target
- si el cambio toca DB, seguir el runbook de backups/rollback
