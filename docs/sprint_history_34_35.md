# Sprint History 34-35

Resumen consolidado de los bloques recientes relacionados con deployment, seguridad, usuarios, UI y branding.

| Sprint / bloque | Objetivo | Resultado | Commit hash | Archivos principales | Estado |
|---|---|---|---|---|---|
| 34A | Preparar deployment en Render | Base de despliegue preparada | `34de05a` | `app/`, configuracion de deploy | Cerrado |
| 34C | Bootstrap admin de produccion | Script seguro para primer admin | `af6d251` | `scripts/bootstrap_admin.py` | Cerrado |
| 34E | Dry-run SQLite -> PostgreSQL | Migrador con inspeccion y validaciones | `39becc4` | `scripts/migrate_sqlite_to_postgres.py` | Cerrado |
| 34F | Soporte de migracion controlada | Flujo de migracion hacia staging/produccion inicial | `c4d13d5` | tooling de migracion y validacion | Cerrado |
| 34G | Validacion staging con datos reales | Staging validado con PostgreSQL y datos migrados | `f24768b` | `docs/sprint_34G_staging_validation.md` | Cerrado |
| 35A | Conexion de dominio | Dominio `erp.greencornercr.com` conectado | `a055d7a` | `docs/sprint_35A_domain_connection.md` | Cerrado |
| 35B | Backups + change protocol | Politica de backups, rollback y deploy seguro | `2f8285b` | `docs/sprint_35B_backups_change_protocol.md` | Cerrado |
| 35C | Users + roles setup plan | Matriz inicial de permisos y reglas operativas | `eb23663` | `docs/sprint_35C_users_roles_setup_plan.md` | Cerrado |
| 35D.1 | Tooling de roles | Sincronizacion segura de roles operativos | `e7f2b1f` | `scripts/create_users_and_roles.py` | Cerrado |
| 35D.2 | Workflow de usuarios nominales | Creacion controlada de usuarios aprobados | `1827372` | `scripts/create_users_and_roles.py` | Cerrado |
| 35D.3 | Crear usuarios reales en produccion inicial | Evento operativo de alta real de usuarios | Sin commit | Operacion fuera de repo | Evento operativo sin commit de codigo |
| 35D.4 | Logout visible | Navegacion visible para User / Change password / Logout | `19a86dd` | `app/templates/base.html`, `app/static/style.css` | Cerrado |
| 35E.1 | UI polish | Botones consistentes, fondo blanco, limpieza visual | `aeea76c` | templates varios, `app/static/style.css` | Cerrado |
| 35E.2 | Branding en DB | Logo de empresa almacenado en PostgreSQL y servido por header | `4881985` | `app/models.py`, `app/main.py`, `app/templates/base.html`, `app/templates/admin_branding.html`, `app/static/style.css` | Cerrado |
| 35E.3 | Logout visual + white background cleanup | Logout como link visual y limpieza adicional de fondo blanco | `9c3f81b` | `app/static/style.css` | Cerrado |
| 35E.4 | Header logo layout | Logo alineado a la izquierda y centrado verticalmente en header | `bb76e18` | `app/static/style.css` | Cerrado |
| 35E.5 | Mobile hamburger navigation | Menu hamburguesa para mobile manteniendo desktop estable | `974c16b` | `app/templates/base.html`, `app/static/style.css` | Cerrado |

## Notas

- Los sprints 1-5 y buena parte de los bloques intermedios documentan la fase MVP de costeo y la evolucion funcional posterior.
- Este documento se concentra en el tramo 34-35 porque ahi ocurre el salto claro a despliegue real, seguridad multiusuario y consolidacion del ERP.
