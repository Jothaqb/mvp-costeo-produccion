# Cierre B2B ERP a Loyverse - julio 2026

## Estado del documento

- Fecha operativa del cierre: 2026-07-31, `America/Costa_Rica` (UTC-06:00).
- Resultado: cierre ejecutado exitosamente y post-check completado.
- Fuente oficial: ERP Green Corner.
- Destino: Loyverse API `POST /v1.0/receipts`.
- Script utilizado: `tools/b2b_loyverse_monthly_invoice_job.py`.
- Este documento también propone el diseño del job diario. El job diario y el Render Cron **no están implementados**.

## 1. Objetivo

Crear en Loyverse los receipts correspondientes a las ventas B2B del ERP con fecha de entrega dentro de julio 2026 y guardar en el ERP las referencias devueltas por Loyverse, con controles explícitos contra duplicados, resultados ambiguos y mappings incompletos.

El intervalo utilizado fue semiabierto:

```text
B2BSalesOrder.delivery_date >= 2026-07-01
B2BSalesOrder.delivery_date <  2026-08-01
```

Excel no fue fuente de este proceso. La mención de Excel corresponde únicamente al antecedente histórico del procedimiento manual.

## 2. Alcance y fuentes de datos

### Fuente de ventas

- `B2BSalesOrder`
- `B2BSalesOrderLine`

### Datos auxiliares consultados

- customer mappings de Loyverse;
- variant mappings de Loyverse;
- payment type mappings de Loyverse;
- `LOYVERSE_STORE_ID` o, si no está configurado, `GET /v1.0/stores` cuando la cuenta devuelve exactamente una tienda.

### Escrituras esperadas en execute

El job crea el receipt mediante `POST /v1.0/receipts` y actualiza únicamente la evidencia Loyverse de la orden/líneas B2B, usando los campos existentes:

- `loyverse_receipt_id`;
- `loyverse_receipt_number`;
- `loyverse_invoice_sync_status`;
- `loyverse_invoice_sync_error`;
- `loyverse_invoice_sync_attempted_at`;
- `loyverse_invoice_synced_at`;
- `loyverse_invoice_sync_attempt_count`;
- `loyverse_variant_id_snapshot` en las líneas;
- `total_amount` recalculado desde las líneas.

El estado ERP previo de una orden ya marcada `invoiced` se conserva.

## 3. Fuera de alcance

Este job no realiza ni modifica:

- inventario;
- Kardex;
- `InventoryTransaction`;
- `InventoryBalance`;
- `inventory_ledger_service.py`;
- producción;
- órdenes de producción;
- cierre contable;
- importación de Excel o CSV;
- creación de ventas desde archivos externos.

Los CSV generados son evidencia de salida, no fuentes de ventas.

## 4. Controles incorporados

### Modos

- dry-run por defecto: no llama `POST /receipts`, no hace commit y termina con rollback de la sesión de lectura;
- execute: requiere `--execute` y confirmación exacta `SEND_B2B_JULY_2026_TO_LOYVERSE`;
- execute quedó limitado a julio 2026 y `America/Costa_Rica`.

### Dedupe local

Una orden no es elegible cuando:

- ya tiene `loyverse_receipt_id`;
- ya tiene `loyverse_receipt_number`;
- su sync status es `unknown`;
- fue indicada mediante `--exclude-order-number`.

Antes de enviar se vuelve a cargar la orden y se compara el fingerprint SHA-256 del payload con el preview. Cada orden se procesa y confirma por separado. Antes del POST se persiste estado `unknown`, de modo que una interrupción posterior no provoque un reintento automático ciego.

### Overrides de emergencia utilizados

Payment type:

```text
ID:   1d4486cd-cac0-4933-85df-1587be6c0973
Name: Tiendas Naturales
```

Customer:

```text
ERP customer_id:       6
ERP customer_name:     Super Cocles
Loyverse customer_id:  25bf9d0b-47ee-4286-aba4-c8b77bedd507
Loyverse customer_name: Super cocles
```

Los overrides no escribieron tablas de mappings. La resolución local conservó prioridad y el override se utilizó únicamente cuando no existía una resolución local válida.

### Exclusión manual

```text
Order number: B2B1146
Amount:       121100.0000
Reason:       Already invoiced in Loyverse on 2026-06-30; excluded from the
              July ERP-to-Loyverse batch to avoid a duplicate receipt.
```

## 5. Comandos del cierre

Los comandos requieren `DATABASE_URL`, `LOYVERSE_API_TOKEN` y, preferiblemente, `LOYVERSE_STORE_ID` en el entorno. Ningún secreto debe incluirse en la línea de comandos o en evidencia.

### 5.1 Dry-run con payment type override

```bash
python tools/b2b_loyverse_monthly_invoice_job.py --start-date 2026-07-01 --end-date-exclusive 2026-08-01 --timezone America/Costa_Rica --export-dir artifacts/b2b_loyverse_july_2026_render_payment_override --use-env --loyverse-payment-type-id 1d4486cd-cac0-4933-85df-1587be6c0973 --loyverse-payment-type-name "Tiendas Naturales"
```

### 5.2 Dry-run con customer override

```bash
python tools/b2b_loyverse_monthly_invoice_job.py --start-date 2026-07-01 --end-date-exclusive 2026-08-01 --timezone America/Costa_Rica --export-dir artifacts/b2b_loyverse_july_2026_render_customer_override --use-env --loyverse-payment-type-id 1d4486cd-cac0-4933-85df-1587be6c0973 --loyverse-payment-type-name "Tiendas Naturales" --customer-override-erp-id 6 --customer-override-loyverse-id 25bf9d0b-47ee-4286-aba4-c8b77bedd507 --customer-override-loyverse-name "Super cocles"
```

### 5.3 Dry-run final con exclusión de B2B1146

```bash
python tools/b2b_loyverse_monthly_invoice_job.py --start-date 2026-07-01 --end-date-exclusive 2026-08-01 --timezone America/Costa_Rica --export-dir artifacts/b2b_loyverse_july_2026_render_exclude_b2b1146 --use-env --loyverse-payment-type-id 1d4486cd-cac0-4933-85df-1587be6c0973 --loyverse-payment-type-name "Tiendas Naturales" --customer-override-erp-id 6 --customer-override-loyverse-id 25bf9d0b-47ee-4286-aba4-c8b77bedd507 --customer-override-loyverse-name "Super cocles" --exclude-order-number B2B1146
```

### 5.4 Execute final

Este comando se conserva como evidencia histórica del cierre ejecutado. No debe reutilizarse sin un nuevo dry-run, revisión y aprobación.

```bash
python tools/b2b_loyverse_monthly_invoice_job.py --start-date 2026-07-01 --end-date-exclusive 2026-08-01 --timezone America/Costa_Rica --export-dir artifacts/b2b_loyverse_july_2026_execute_exclude_b2b1146 --use-env --loyverse-payment-type-id 1d4486cd-cac0-4933-85df-1587be6c0973 --loyverse-payment-type-name "Tiendas Naturales" --customer-override-erp-id 6 --customer-override-loyverse-id 25bf9d0b-47ee-4286-aba4-c8b77bedd507 --customer-override-loyverse-name "Super cocles" --exclude-order-number B2B1146 --execute --confirm SEND_B2B_JULY_2026_TO_LOYVERSE
```

### 5.5 Post-execute check

El post-check vuelve a ejecutar el modo read-only con un directorio nuevo:

```bash
python tools/b2b_loyverse_monthly_invoice_job.py --start-date 2026-07-01 --end-date-exclusive 2026-08-01 --timezone America/Costa_Rica --export-dir artifacts/b2b_loyverse_july_2026_post_execute_check --use-env --loyverse-payment-type-id 1d4486cd-cac0-4933-85df-1587be6c0973 --loyverse-payment-type-name "Tiendas Naturales" --customer-override-erp-id 6 --customer-override-loyverse-id 25bf9d0b-47ee-4286-aba4-c8b77bedd507 --customer-override-loyverse-name "Super cocles" --exclude-order-number B2B1146
```

## 6. Resultados confirmados

### Execute

```text
orders_found:           30
orders_eligible:        29
orders_blocked:         0
orders_already_sent:    0
orders_excluded_manual: 1
excluded_order_numbers: ["B2B1146"]
orders_sent_success:    29
orders_unknown:         0
orders_failed:          0
total_amount_sent:      3674120.0000
```

### Post-check

```text
orders_found:        30
orders_eligible:     0
orders_blocked:      0
orders_already_sent: 29
```

El post-check confirma que las 29 órdenes enviadas quedaron con referencia local suficiente para no volver a ser elegibles. B2B1146 permaneció fuera del lote.

## 7. Evidencia generada

Cada directorio de ejecución contiene, según el modo:

- `summary.json`: rango, conteos, montos, warnings y modo;
- `orders_preview.csv`: todas las órdenes encontradas y su clasificación;
- `orders_eligible.csv`: candidatas validadas;
- `orders_blocked.csv`: bloqueos de mappings o validación;
- `orders_already_sent.csv`: órdenes con referencia Loyverse local;
- `orders_excluded.csv`: exclusiones manuales;
- `payloads_preview.json`: payload y fingerprint por orden elegible;
- `execution_results.csv`: resultados por orden, solo en execute;
- `errors.csv`: errores de validación o ejecución.

Los artifacts no contienen tokens ni `DATABASE_URL`, pero sí pueden contener datos comerciales e identificadores. Deben tratarse como evidencia operativa con acceso restringido.

## 8. Riesgos controlados y riesgos residuales

### Controlados durante el cierre

- mappings faltantes: bloqueados durante dry-run y resueltos explícitamente;
- duplicado conocido B2B1146: exclusión manual documentada;
- referencias locales existentes: clasificación `already_sent`;
- respuesta ambigua: estado `unknown`, sin reintento automático;
- errores confirmados: evidencia por orden;
- diferencia UTC/Costa Rica: fecha operativa B2B corregida a UTC-06:00;
- cambio del payload entre preview y execute: fingerprint y revalidación.

### Residuales

1. La deduplicación es principalmente local. Un receipt creado fuera del ERP sin referencia local puede duplicarse.
2. El payload actual no incluye una referencia ERP remota documentada para buscar/reconciliar antes del POST.
3. El endpoint actual no recibe `delivery_date`; Loyverse determina la fecha efectiva del receipt según el comportamiento de su API.
4. Los overrides CLI son controles de emergencia, no sustitutos de mappings maestros mantenidos.
5. Los archivos locales de un Render Cron no son almacenamiento durable.

## 9. Commits relevantes

```text
8710c2d192d7ea1cf1099893ad750b9f2fc68e23
Add emergency B2B Loyverse monthly invoice job

1dd744e7cfe745bef6dadd858d27d832029e1ddb
Add B2B Loyverse payment type override

1e1ad7cb41f8c8852b2a704325d8a0ec8e2bed7a
Add B2B Loyverse customer override

15a6c205013a8c0b114c2f0e894eebe045de199e
Add B2B Loyverse manual order exclusion

1e38b0e4873a4f986bcfcf4ce66608eff194012a
Use Costa Rica operational date for B2B sales
```

## 10. Lecciones aprendidas

- La fecha comercial debe calcularse explícitamente en `America/Costa_Rica`, no con la fecha UTC del host.
- Un dry-run útil debe clasificar elegibles, bloqueadas, enviadas y excluidas, y mostrar payloads antes del POST.
- Un resultado ambiguo debe bloquear reintentos hasta reconciliación manual.
- Los registros históricos creados fuera del ERP requieren reconciliación antes de un envío masivo.
- Payment type, customer y store deben mantenerse como configuración estable; los overrides pertenecen a contingencias auditadas.
- El post-check es parte obligatoria del procedimiento, no una validación opcional.
- La evidencia debe copiarse a almacenamiento durable antes de cerrar una Render Shell.

# Diseño propuesto: job diario B2B ERP a Loyverse

## 11. Decisión de arquitectura

No duplicar toda la lógica del monthly job. La propuesta es:

1. extraer selección, evaluación, reportes y ejecución por orden a un módulo compartido;
2. conservar el monthly CLI para cierres/manuales;
3. crear `tools/b2b_loyverse_daily_invoice_job.py` como wrapper de política diaria;
4. mantener dry-run como default;
5. no habilitar auto-execute hasta completar los controles previos de esta sección.

El wrapper diario debe aceptar:

```bash
python tools/b2b_loyverse_daily_invoice_job.py --business-date 2026-07-31 --use-env
```

Si `--business-date` no se proporciona, debe calcular:

```text
now_cr = fecha/hora actual en America/Costa_Rica
business_date = now_cr.date() - 1 día
start_date = business_date
end_date_exclusive = business_date + 1 día
```

Ejemplo para una ejecución el 2026-08-01 00:30 Costa Rica:

```text
start_date:         2026-07-31
end_date_exclusive: 2026-08-01
```

## 12. Flujo diario propuesto

1. Validar configuración sin imprimir valores secretos.
2. Adquirir un PostgreSQL advisory lock exclusivo para impedir solapamiento con ejecuciones manuales.
3. Calcular/validar la fecha comercial Costa Rica.
4. Crear un `run_id` y `artifacts/b2b_loyverse_daily/YYYY-MM-DD/`.
5. Ejecutar preview read-only y escribir evidencia.
6. Terminar con error y sin POST cuando cualquiera sea mayor que cero:
   - `orders_blocked`;
   - `orders_unknown` preexistentes;
   - `missing_customer_mappings`;
   - `missing_variant_mappings`;
   - `missing_payment_type_mappings`.
7. Si `total_amount_eligible == 0`, registrar no-op exitoso y salir con código 0.
8. Si el modo es dry-run, salir exitosamente después del preview.
9. Para auto-execute, exigir simultáneamente:
   - `--execute`;
   - confirmación específica del job diario;
   - `B2B_LOYVERSE_DAILY_AUTO_EXECUTE=true`.
10. Revalidar cada orden y fingerprint inmediatamente antes del POST.
11. Ejecutar commits por orden con preestado `unknown`, como el proceso mensual.
12. Generar `execution_results.csv`, resumen final y logs estructurados.
13. Ejecutar un post-check read-only del mismo rango.
14. Liberar el advisory lock y salir con código distinto de cero ante `unknown` o `failed`.

### Tratamiento diario por estado

- referencia local o `success`: `already_sent`, nunca reenviar;
- `unknown`: bloquear el run y exigir reconciliación manual;
- `failed`: no reintentar automáticamente en la primera versión; registrar atención requerida;
- `excluded_manual`: no debería ser mecanismo permanente del cron;
- sin referencia/estado y mappings completos: elegible.

## 13. Gating entre preview y execute

El preview y execute deben ocurrir dentro de un mismo run y usar exactamente:

- misma fecha comercial;
- mismo conjunto de configuración;
- mismo fingerprint por orden;
- mismo lock de ejecución.

No se recomienda encadenar dos Cron Jobs independientes, porque podría cambiar la DB entre el preview y el execute.

Matriz de salida propuesta:

| Condición | POST | Exit code | Resultado |
|---|---:|---:|---|
| mappings/blockers | No | 2 | atención requerida |
| unknown preexistente | No | 3 | reconciliación manual |
| monto elegible cero | No | 0 | no-op exitoso |
| dry-run limpio | No | 0 | listo para aprobación |
| execute completo | Sí | 0 | post-check requerido |
| unknown/failed durante execute | no más reintentos | distinto de 0 | incidente |

## 14. Idempotencia recomendada antes de auto-execute

Los campos locales actuales son necesarios, pero no suficientes frente a receipts creados externamente. Antes de habilitar el cron con POST automático se recomienda al menos una de estas medidas:

1. confirmar un campo admitido por Loyverse para guardar `ERP Order: <order_number>` y consultarlo antes de crear;
2. implementar reconciliación remota previa por una combinación estable y revisada de cliente, monto, líneas y ventana temporal;
3. incorporar posteriormente una outbox/idempotency table con fingerprint único, estado y run ID;
4. backfill controlado de la referencia local de B2B1146 después de verificar el receipt existente, en vez de mantener una exclusión indefinida.

No debe inventarse un campo en el payload de Loyverse sin verificar primero que el endpoint lo admite.

## 15. Evidencia y logging diario

Ruta lógica:

```text
artifacts/b2b_loyverse_daily/YYYY-MM-DD/
```

Logs mínimos en stdout/stderr:

- `run_id`;
- business date y timezone;
- modo;
- conteos del preview;
- monto elegible/enviado;
- order numbers por resultado, sin datos sensibles innecesarios;
- inicio/fin y duración;
- resultado del post-check;
- código de salida y acción requerida.

Render Cron no admite persistent disks. Por ello, la carpeta sirve durante la ejecución pero no constituye archivo durable. Antes de automatizar se debe elegir uno de estos destinos:

- object storage con cifrado y retención;
- sistema central de logs más un summary estructurado;
- tabla de auditoría dedicada en una fase posterior.

No subir artifacts al repositorio.

## 16. Riesgo de fecha del receipt diario

El job propuesto procesa el día anterior a las 00:30 Costa Rica, pero el payload actual no envía la fecha comercial ERP. Por tanto, un receipt para 2026-07-31 creado el 2026-08-01 podría quedar fechado en Loyverse como 2026-08-01.

Antes de habilitar auto-execute se debe:

1. confirmar con documentación/prueba controlada si Loyverse permite definir una fecha de receipt;
2. aceptar formalmente la fecha asignada por Loyverse, o cambiar el horario/política;
3. validar el impacto contable y de reportes.

Este punto no bloquea un dry-run diario, pero sí debe bloquear el POST automático hasta tomar una decisión.

## 17. Configuración propuesta de Render Cron

Render interpreta todos los horarios Cron en UTC. Costa Rica usa UTC-06:00 sin cambio estacional, por lo que 00:30 Costa Rica corresponde a:

```cron
30 6 * * *
```

Configuración sugerida:

```text
Name:          erp-green-corner-b2b-loyverse-daily
Service type:  Cron Job
Repository:    https://github.com/Jothaqb/mvp-costeo-produccion
Branch:        master
Runtime:       Python
Build command: pip install -r requirements.txt
Schedule:      30 6 * * *
```

Comando inicial seguro durante rollout:

```bash
python tools/b2b_loyverse_daily_invoice_job.py --use-env
```

Comando futuro de auto-execute, únicamente después de aprobar todos los controles:

```bash
python tools/b2b_loyverse_daily_invoice_job.py --use-env --execute --confirm RUN_B2B_LOYVERSE_DAILY
```

Variables requeridas:

```text
DATABASE_URL
LOYVERSE_API_TOKEN
LOYVERSE_STORE_ID
B2B_LOYVERSE_DAILY_AUTO_EXECUTE
```

Recomendación de valores/política:

- configurar `LOYVERSE_STORE_ID` explícitamente para eliminar `GET /stores`, evitar ambigüedad y suprimir el warning;
- mantener secretos en Render, nunca en el repositorio o comando;
- usar un Environment Group compartido con el Web Service cuando ambos deban usar exactamente DB/Loyverse; revisar que el grupo no otorgue secretos innecesarios a otros servicios;
- iniciar con `B2B_LOYVERSE_DAILY_AUTO_EXECUTE=false` y Cron en dry-run;
- habilitar `true` únicamente después de varios días de previews limpios y aprobación formal.

Render permite compartir variables mediante Environment Groups, registra runs/logs del Cron y garantiza como máximo una ejecución activa por Cron Job. Aun así, el advisory lock protege contra solapamiento con shells u otros servicios.

Referencias oficiales:

- [Render Cron Jobs](https://render.com/docs/cronjobs)
- [Render environment variables and Environment Groups](https://render.com/docs/configure-environment-variables)

## 18. Checklist antes de implementar o habilitar el job diario

- [ ] Confirmar política de fecha del receipt en Loyverse.
- [ ] Diseñar dedupe remoto o aceptar formalmente su limitación.
- [ ] Eliminar overrides de emergencia mediante mappings maestros válidos.
- [ ] Decidir tratamiento operativo de `failed`.
- [ ] Definir almacenamiento durable de evidencia.
- [ ] Implementar lock contra ejecuciones concurrentes.
- [ ] Implementar el wrapper y refactor compartido con pruebas.
- [ ] Ejecutar varios días en dry-run.
- [ ] Revisar logs, exit codes y alertas.
- [ ] Aprobar explícitamente auto-execute.
- [ ] Crear Render Cron solo después de la aprobación.

## 19. Recomendación final

El cierre mensual quedó validado por el post-check y puede considerarse completado. Para el job diario conviene crear un wrapper sobre un motor compartido, no copiar el monthly script.

La automatización puede comenzar en modo dry-run a las `06:30 UTC`, pero no debe habilitar POST automático hasta resolver dos decisiones críticas:

1. deduplicación frente a receipts creados fuera del ERP;
2. fecha que Loyverse asignará al receipt del día anterior.

