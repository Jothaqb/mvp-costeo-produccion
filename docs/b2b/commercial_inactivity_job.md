# Depuración por inactividad comercial B2B

## Alcance

`tools/b2b_commercial_inactivity_job.py` evalúa exclusivamente:

- `B2BCustomerProduct.active`, por cliente y SKU.
- `B2BCustomer.active`.

No modifica `Product`, inventario, Kardex, BOM, producción, pedidos, facturación,
CxC, acuerdos comerciales ni Loyverse. No configura ningún Cron.

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
catálogo. No se agregó esquema nuevo. Como protección conservadora, cualquier
registro cuyo `updated_at` esté dentro de los últimos 30 días queda en
`review_pending`. Esto evita que una reactivación manual sea revertida en la
siguiente corrida.

La limitación deliberada es que una edición de precio o datos también obtiene
la misma gracia. Si más adelante se necesita distinguir exactamente una
reactivación, se recomienda agregar una excepción persistente o un campo
`reactivated_at` en una fase separada.

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

- `summary.json`: fechas, cobertura, conteos, IDs elegibles y fingerprint.
- `catalog_products.csv`: todos los productos evaluados.
- `customers.csv`: todos los clientes evaluados, incluyendo CxC y acuerdos.
- `history_coverage.csv`: conteos mensuales de facturas regulares e importadas
  para revisar vacíos de cobertura antes de aprobar.
- `review_pending.csv`: registros que requieren decisión humana.
- `excluded.csv`: activos recientes o registros ya inactivos.
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

## Reglas conservadoras

Un registro pasa a `review_pending` si aplica alguna condición como:

- cobertura histórica no confirmada;
- cliente o asignación de catálogo con menos de 12 meses;
- edición/reactivación dentro del período de gracia;
- SKU sin coincidencia exacta en `Product`;
- pedido abierto del cliente o del SKU;
- factura con fecha futura.

Los saldos CxC y estados de acuerdos solo se reportan y se conservan. Un saldo
pendiente no se modifica ni se elimina por inactivar al cliente.
