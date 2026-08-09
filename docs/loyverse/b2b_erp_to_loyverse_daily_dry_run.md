# Job diario B2B ERP a Loyverse - Fase 1 dry-run

## Propósito y alcance

`tools/b2b_loyverse_daily_invoice_job.py` revisa las ventas B2B correspondientes a un día operativo de Costa Rica y genera evidencia de preparación para Loyverse.

Esta primera fase es estrictamente de lectura:

- no acepta `--execute`;
- no crea receipts;
- no llama `POST /receipts`;
- no confirma cambios en la base de datos;
- no modifica órdenes, mappings, modelos, inventario ni producción.

Puede consultar `GET /stores` cuando `LOYVERSE_STORE_ID` no está configurado. Si la API devuelve exactamente una tienda, utiliza esa tienda para construir el preview. Cero o múltiples tiendas bloquean el readiness check.

## Fecha operativa

El horario previsto es 23:30 `America/Costa_Rica`. La futura expresión de Render Cron será:

```text
30 5 * * *
```

La expresión está en UTC y corresponde a las 23:30 Costa Rica del día anterior.

En modo automático se aplica esta política cerrada:

```text
22:00-23:59 Costa Rica -> procesa hoy
00:00-03:00 Costa Rica -> procesa ayer
fuera de la ventana     -> aborta y exige --business-date
```

El rango consultado siempre es:

```text
delivery_date >= business_date
delivery_date < business_date + 1 día
```

## Comandos

Ejecución manual para una fecha explícita:

```bash
python tools/b2b_loyverse_daily_invoice_job.py --business-date 2026-08-10 --use-env
```

Comando previsto para un futuro Render Cron, todavía no configurado:

```bash
python tools/b2b_loyverse_daily_invoice_job.py --auto-business-date-costa-rica --use-env --export-root artifacts/b2b_loyverse_daily
```

Variables necesarias:

```text
DATABASE_URL
LOYVERSE_API_TOKEN
LOYVERSE_STORE_ID (recomendado; opcional si la cuenta tiene exactamente una tienda)
```

Los valores secretos nunca se incluyen en reportes ni logs. La información de la base se limita a scheme, host y nombre enmascarados.

## Clasificación y bloqueos

Resultados posibles por orden:

- `eligible`: mappings y payload completos;
- `already_sent`: existe una referencia local de receipt;
- `blocked`: requiere corrección o reconciliación;
- `excluded_manual`: excluida mediante `--exclude-order-number`;
- `error`: error inesperado durante evaluación read-only.

Bloquean la preparación:

- estado `unknown`;
- estado `failed`;
- estado `success` sin referencia local;
- combinación de estado/referencia inconsistente;
- customer, variant o payment type mapping faltante;
- tienda faltante o ambigua;
- payload inválido.

`unknown` y `failed` nunca se reintentan en esta fase. Un receipt number sin receipt id se considera una referencia válida porque Loyverse puede responder únicamente con `receipt_number`.

Cuando no hay elegibles, bloqueos ni errores, el run termina como no-op exitoso. Un dry-run con órdenes elegibles también termina correctamente porque no realiza envíos. Los bloqueos o errores producen un código de salida distinto de cero para facilitar alertas futuras.

## Evidencia

Cada ejecución crea una carpeta nueva y nunca sobrescribe una anterior:

```text
artifacts/b2b_loyverse_daily/YYYY-MM-DD/<run_id>/
```

Archivos:

```text
summary.json
orders_preview.csv
orders_eligible.csv
orders_already_sent.csv
orders_blocked.csv
orders_excluded.csv
errors.csv
payloads_preview.json
```

No se genera `execution_results.csv` porque no existe ejecución.

## Limitaciones de fase 1

- No detecta receipts manuales remotos que no tengan referencia guardada en el ERP.
- No configura ni refresca mappings.
- No procesa automáticamente facturas creadas después del corte.
- Los artifacts locales de un futuro Render Cron no serán almacenamiento persistente.
- Habilitar auto-execute requerirá otra fase, revisión específica y autorización independiente.
