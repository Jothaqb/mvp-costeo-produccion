# Facturación B2B en Loyverse desde el ERP

## Alcance

La pantalla `B2B Sales Orders → Facturar en Loyverse` convierte el proceso mensual existente en
un flujo reutilizable. La vista previa reutiliza las reglas de payload y mapeos de
`tools/b2b_loyverse_monthly_invoice_job.py` y no llama la API de Loyverse.

El período se filtra por la fecha real de facturación ERP; la fecha final es inclusiva. La fuente
primaria es `B2BSalesOrder.invoice_date` y solo para registros legacy se acepta
`B2BSalesOrder.invoiced_at.date()`. `delivery_date` y `created_at` no se aceptan como evidencia de
facturación en este flujo. Solo los pedidos cuyo estado ERP sea `invoiced` pueden quedar elegibles.

## Seguridad

- GET de vista previa y POST de confirmación requieren `sales.invoice`.
- La confirmación exige sesión autenticada, CSRF vinculado a la sesión, validación same-origin y
  aceptación explícita.
- El token de confirmación vincula sesión, fechas y fingerprint del plan.
- El POST vuelve a bloquear y recalcular el período antes de crear registros de cola.
- Los IDs de pedidos no se reciben desde el formulario; la selección se reconstruye desde el
  período y el manifiesto firmado.
- Una referencia Loyverse almacenada excluye el pedido. Referencias duplicadas bloquean los
  pedidos relacionados.
- `historical_confirmed` identifica facturas anteriores al 1 de octubre de 2026 cuya presencia en
  Loyverse fue confirmada expresamente por el negocio. No inventa un número de comprobante y
  bloquea definitivamente cualquier reenvío.
- Estados `unknown` o `success` sin referencia requieren conciliación manual y nunca se reintentan
  automáticamente. Un error definitivo `failed` solo puede ponerse nuevamente en cola después de
  generar y confirmar otra vista previa; nunca se reintenta por sí solo.

## Cola persistente

`b2b_loyverse_invoice_dispatches` se crea con el mecanismo existente
`Base.metadata.create_all()`. Existe una única fila por pedido. La confirmación guarda el estado
`queued`, fechas y fingerprints junto con la auditoría en la misma transacción.

El worker reclama filas con bloqueo `FOR UPDATE SKIP LOCKED` en PostgreSQL. Antes del POST vuelve a
validar estado `invoiced`, ausencia de referencia, mapeos y payload. Justo antes de llamar a
Loyverse, el ejecutor existente persiste `unknown`. Por eso, si el resultado de red es incierto,
la orden queda bloqueada para conciliación y no puede generar un duplicado por reintento.

La recuperación tras reinicio solo vuelve a `queued` una fila que quedó en `processing` antes de
iniciar el intento. Si el pedido ya está `unknown`, se conserva `unknown`; si ya tiene referencia,
se consolida como `success`.

## Configuración de despliegue

El worker está apagado por defecto. La vista previa sigue disponible, pero el botón de confirmación
queda bloqueado hasta que la configuración sea completa:

```text
LOYVERSE_API_TOKEN=<token de producción>
LOYVERSE_STORE_ID=<store id aprobado>
B2B_LOYVERSE_INVOICE_WORKER_ENABLED=true
B2B_LOYVERSE_INVOICE_WORKER_INTERVAL_SECONDS=15   # opcional; rango 5-3600
APP_BASE_URL=https://<dominio público del ERP>    # o RENDER_EXTERNAL_URL
```

No debe habilitarse el worker antes de revisar una vista previa real y autorizar el envío. La
aplicación no consulta ni modifica inventario, Kardex, CxC, costos o la facturación interna durante
este flujo; únicamente crea el receipt externo y guarda su evidencia en los campos Loyverse del
pedido/líneas.

## Octubre 2026

Para revisar del 1 al 29 de octubre de 2026, abrir la pantalla y usar:

```text
Fecha inicial: 2026-10-01
Fecha final:   2026-10-29
```

La vista previa presenta encontrados, elegibles, ya enviados, excluidos, bloqueados, importe y
mapeos faltantes. No confirmar hasta conciliar todos los bloqueados y aprobar el importe.

## Confirmación histórica anterior a octubre de 2026

`tools/mark_b2b_loyverse_historical_invoices.py` prepara evidencia sin escribir por defecto. Solo
considera confiables `invoice_date` e `invoiced_at`; las facturas sin ambas fechas quedan en
`review_unreliable_date`, salvo que `observations` comience con la marca técnica
`Historical CSV import` escrita por el importador histórico del ERP. Esa marca permite separar
los pedidos de la migración confirmada por el negocio sin inventar una fecha de factura ni un
comprobante. Una fecha nula por sí sola nunca basta: si falta la marca, el pedido permanece en la
lista explícita de revisión por ID. Una fecha confiable desde el 01/10/2026 siempre prevalece y
mantiene el pedido fuera del marcado histórico.

Cada dry-run genera `orders.csv` y cuatro archivos separados:

- `orders_migrated_identified.csv` para la carga histórica identificada técnicamente.
- `orders_historical_normal.csv` para facturas normales anteriores al corte.
- `orders_review.csv` para IDs que requieren validación explícita.
- `orders_from_october.csv` para pedidos controlados por el flujo permanente.

El fingerprint usa la revisión `v2`, por lo que cualquier evidencia producida antes de este ajuste
es obsoleta y no puede utilizarse para ejecutar. El dry-run requiere un directorio nuevo:

```text
python tools/mark_b2b_loyverse_historical_invoices.py --export-dir <directorio-nuevo>
```

La ejecución exige posteriormente el fingerprint exacto del dry-run, un usuario activo con
`sales.invoice`, la declaración textual aprobada y una confirmación operacional explícita. No
modifica estados ERP, inventario, Kardex, CxC ni referencias existentes; únicamente guarda
`loyverse_invoice_sync_status=historical_confirmed` y auditoría transaccional. Los estados
`unknown`, referencias duplicadas y ejecuciones activas quedan para revisión.
