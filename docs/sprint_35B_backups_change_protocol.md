# Sprint 35B — Backups + Change Protocol

Estado: Aprobado / Cerrado

## 1. Estado actual de producción inicial

- ERP publicado en:
  https://erp.greencornercr.com

- Servicio Render actual:
  `erp-green-corner-staging`

- Nota importante:
  aunque el servicio conserva el nombre staging, actualmente funciona como producción inicial.

- PostgreSQL Render contiene datos reales migrados.

- URL fallback:
  https://erp-green-corner-staging.onrender.com

- Dominio:
  Verified

- Certificado:
  Issued

- Último commit base:
  `a055d7a` — `Sprint 35A: document domain connection`

## 2. Política de backups

- Usar los backups/PITR nativos de Render según el plan contratado.
- Confirmar ventana real de recuperación desde Render Dashboard.
- Referencia actual:
  - Hobby: 3 días
  - Pro o superior: 7 días
- Restaurar PITR en una nueva DB, no encima de la DB actual.
- Validar la DB restaurada antes de apuntar el servicio a ella.

Backup lógico/manual recomendado antes de cambios críticos:

- migraciones
- scripts masivos
- cambios de schema
- imports históricos grandes
- cambios de auth/permisos
- cambios de inventario/Kardex/costing
- restauraciones
- cambios de `DATABASE_URL`
- cambios de plan de DB
- cambios de dominio/cutover relevante

Registro mínimo de backup:

- fecha/hora
- responsable
- motivo
- commit relacionado
- DB origen
- ubicación del backup
- validación mínima realizada

## 3. Protocolo de cambios

Reglas base:

- GitHub es la fuente de verdad.
- No editar código manualmente en Render.
- Todo cambio debe quedar en commit.
- Todo sprint debe cerrar con:
  - `git status` limpio
  - commit claro
  - validación documentada

Cambios críticos requieren:

- backup previo
- aprobación explícita
- rollback definido
- ventana operativa
- validación posterior

Cambios críticos incluyen:

- migraciones de DB
- `reset-target`
- scripts de migración
- imports masivos
- cambios de schema/modelos
- auth/permisos
- inventario/Kardex
- costing
- dominio/DNS
- `DATABASE_URL`
- plan Render/DB
- restore backup

## 4. Protocolo GitHub + Render

- Rama actual: `master`
- GitHub es fuente oficial.
- Render despliega desde el repo.
- No incluir en commits:
  - `.db`
  - backups
  - logs
  - credenciales
  - temporales
  - capturas sensibles

Recomendación para producción inicial:

- desactivar auto-deploy o mantener deploy manual
- hacer deploy manual o deploy de commit específico
- antes de deploy:
  - commit revisado
  - archivos esperados
  - backup si aplica
  - aprobación explícita si es cambio crítico

## 5. Rollback

### A. Falla de deploy/código

- usar rollback de Render a deploy previo
- o deploy manual de commit anterior
- validar:
  - login
  - rutas críticas
  - logs
- anotar que rollback desde Dashboard puede desactivar autodeploy

### B. Falla de DB/datos

- no corregir a ciegas
- detener writes si aplica
- restaurar PITR a una nueva DB
- validar la DB restaurada
- solo después cambiar `DATABASE_URL`
- conservar DB dañada para análisis si hace falta

### C. Falla de dominio/DNS

- usar fallback:
  https://erp-green-corner-staging.onrender.com
- corregir o revertir CNAME
- no tocar DB

## 6. Scripts sensibles

Documentar como sensibles:

- `scripts/bootstrap_admin.py`
- `scripts/migrate_sqlite_to_postgres.py`

Y cualquier script que:

- escriba DB
- resetee DB
- migre datos
- cree admin
- importe masivamente
- modifique inventario
- modifique planning/costing

Reglas:

- no correr sin aprobación explícita
- no correr en producción sin backup previo si modifica datos
- validar target antes de correr
- no usar `DATABASE_URL` desconocido
- limpiar env vars locales después

## 7. DATABASE_URL y credenciales

Documentar:

- `DATABASE_URL` es variable crítica.
- No imprimir `DATABASE_URL`.
- No pegarla en chats.
- No subirla a GitHub.
- No guardarla en `README`.
- Usar Render Environment Variables.
- Limpiar variable local después de usarla:

```powershell
set DATABASE_URL=
```

- No compartir contraseña admin.
- No usar cuentas compartidas para operación diaria.
- Crear usuarios nominales en sprint posterior.

## 8. Checklist antes de deploy

- repo limpio
- commit identificado
- archivos modificados esperados
- no hay `.db` / backups / logs / credenciales / temporales
- si toca DB: backup previo confirmado
- si toca DB: rollback definido
- cambio validado local/staging
- ventana operativa definida
- aprobación explícita registrada
- confirmar si deploy será manual o commit específico

## 9. Checklist después de deploy

- servicio Live
- login OK
- dialect PostgreSQL
- rutas críticas OK:
  - `/`
  - `/master-data/products`
  - `/sales/total`
  - `/production-orders`
  - `/inventory/balances`
  - `/admin/audit-logs`
- logs sin `500` ni traceback
- dominio funciona
- fallback `onrender.com` funciona
- si tocó DB: conteos/validación esperada OK

## 10. Acciones que requieren aprobación manual

- cualquier migración
- cualquier `--reset-target`
- cualquier import masivo
- cualquier restore PITR
- cualquier restore desde backup lógico
- cambios de `DATABASE_URL`
- cambios de plan de Web Service o Postgres
- cambios de dominio/DNS
- ejecución de scripts sensibles en producción
- creación de usuarios reales
- cambios de permisos reales
- activar o desactivar auto-deploy

## 11. Pendientes posteriores

- crear usuarios reales nominales
- definir matriz de roles/permisos
- evaluar renombrar servicio o crear production separado más adelante
- confirmar visualmente ventana PITR real del plan desde Render Dashboard
- definir almacenamiento seguro para backups lógicos manuales
