# Depuración por inactividad comercial B2B

La revisión manual mediante XLSX se documenta por separado en
`docs/b2b/commercial_inactivity_review.md`. Esa interfaz no ejecuta este job ni habilita
inactivaciones.

## Alcance

`tools/b2b_commercial_inactivity_job.py` evalúa exclusivamente:

- `B2BCustomerProduct.active`, por cliente y SKU.
- `B2BCustomer.active`.

No modifica `Product`, inventario, Kardex, BOM, producción, pedidos, facturación,
CxC, acuerdos comerciales ni Loyverse. No configura ningún Cron.

## Alcance por canal

El universo se limita por el `B2BCustomer.channel_id` vigente del cliente. Los
únicos nombres autorizados son, con coincidencia exacta:

1. `Tiendas Naturales (Minisuper)`
2. `Tiendas Naturas (Macobioticas)`
3. `Tiendas Naturales (Supermercados)`
4. `Tienda Online (Terceros)`

Los IDs no están fijados en el código: cada corrida resuelve los registros de
`Channel` por su nombre exacto. No se normalizan tildes, espacios ni variantes
ortográficas. Si falta un nombre o el registro no aplica a B2B, el dry-run lo
reporta en `channel_scope_resolutions` y `--execute` queda bloqueado. Los
clientes de otros canales y los clientes sin canal quedan totalmente fuera de
las clasificaciones `eligible`, `review_pending` y `excluded`.

Para un cliente incluido se consideran todas sus facturas B2B históricas,
independientemente del canal que hubiera quedado guardado en la orden. Un
cambio de `channel_id` después del dry-run modifica el fingerprint y bloquea
la ejecución.

## Ventana y fecha

La fecha predeterminada se obtiene en hora de Costa Rica. El inicio se calcula
restando 12 meses calendario; no se restan 365 días. Solo cuentan órdenes B2B
con `status = invoiced`, usando la precedencia canónica:

1. `invoice_date`.
2. `invoiced_at.date()`.
3. `delivery_date` únicamente para `Historical CSV import`.
4. `created_at.date()` como fallback legacy.

Las órdenes `draft`, `in_process` y `cancelled` no cuentan como ventas.

## Cobertura histórica

El ERP no tiene una tabla que certifique la cobertura de importaciones. El
argumento `--history-complete-since` es una afirmación explícita del operador,
no una inferencia automática. Si se omite o comienza después del corte, los
registros sin actividad se clasifican como `review_pending` y `--execute` queda
bloqueado.

## Reactivación manual

Las pantallas existentes permiten volver a activar clientes y productos de
catálogo. `created_at` y `updated_at` no participan en la elegibilidad porque
pueden reflejar la carga masiva o una edición administrativa, no actividad
comercial ni una reactivación deliberada.

No se agregó esquema nuevo ni existe Cron. Mientras el proceso siga siendo
manual, toda ejecución requiere un dry-run nuevo, revisión humana, fingerprint
idéntico y confirmación explícita. Una reactivación posterior al dry-run cambia
el estado/fingerprint y bloquea ese plan aprobado.

Limitación pendiente: un dry-run nuevo no puede distinguir una reactivación
manual de cualquier otro registro activo. Antes de automatizar el proceso se
recomienda autorizar un mecanismo persistente específico, por ejemplo
`inactivity_protected_until` o una tabla de excepciones con motivo, usuario y
vigencia. No debe reutilizarse `updated_at` para ese fin.

## Dry-run

El modo predeterminado es solo lectura:

```bash
python tools/b2b_commercial_inactivity_job.py \
  --history-complete-since 2025-10-01 \
  --export-dir artifacts/b2b_commercial_inactivity/2026-10-16/dry_run
```

Para reproducibilidad puede indicarse `--as-of-date YYYY-MM-DD`. El directorio
de evidencia debe ser nuevo o estar vacío.

## Evidencia

- `summary.json`: fechas, cobertura, resolución exacta de canales, universo
  incluido/excluido, desglose por canal, IDs elegibles y fingerprint.
- `catalog_products.csv`: todos los productos evaluados.
- `customers.csv`: todos los clientes evaluados, incluyendo CxC y acuerdos.
- `history_coverage.csv`: conteos mensuales de facturas regulares e importadas
  para revisar vacíos de cobertura antes de aprobar.
- `review_pending.csv`: registros que requieren decisión humana.
- `excluded.csv`: registros con venta reciente o ya inactivos.
- `execution_results.csv`: solo se crea en una ejecución real.

## Execute controlado

No ejecutar antes de aprobar el dry-run. La ejecución exige simultáneamente:

- cobertura histórica suficiente;
- el `summary.json` aprobado;
- el mismo fingerprint después de recalcular;
- un usuario ERP activo con rol `admin` para auditoría;
- el literal de confirmación exacto.

```bash
python tools/b2b_commercial_inactivity_job.py \
  --as-of-date 2026-10-16 \
  --history-complete-since 2025-10-01 \
  --export-dir artifacts/b2b_commercial_inactivity/2026-10-16/execute \
  --execute \
  --approved-summary artifacts/b2b_commercial_inactivity/2026-10-16/dry_run/summary.json \
  --actor-username USUARIO_ADMIN \
  --confirm DEACTIVATE_CONFIRMED_B2B_INACTIVITY_CANDIDATES
```

La ejecución vuelve a calcular el universo dentro de una sola transacción,
bloquea los registros objetivo, compara el fingerprint y escribe los cambios
de estado y los `AuditLog` en la misma transacción. Cualquier diferencia o
error provoca rollback.

Los dry-runs generados antes de la revisión de alcance
`b2b-commercial-inactivity-authorized-channels-v1` son obsoletos: el job exige
esa revisión en el `summary.json` aprobado y obliga a generar evidencia nueva.
También exige la política de elegibilidad
`b2b-commercial-inactivity-sales-evidence-v2`, por lo que los dry-runs que
usaban antigüedad de registros o `updated_at` tampoco pueden ejecutarse.

## Reglas conservadoras

Un registro pasa a `review_pending` si aplica alguna condición como:

- cobertura histórica no confirmada;
- SKU sin coincidencia exacta en `Product`;
- pedido abierto del cliente o del SKU;
- factura con fecha futura.

La ausencia de ventas es elegible únicamente cuando la cobertura está
confirmada y no aplica ninguna de esas condiciones. La fecha de creación o
última edición del cliente, producto global o asignación de catálogo se conserva
como evidencia informativa, pero nunca sustituye la evidencia de ventas.

Los saldos CxC y estados de acuerdos solo se reportan y se conservan. Un saldo
pendiente no se modifica ni se elimina por inactivar al cliente.
