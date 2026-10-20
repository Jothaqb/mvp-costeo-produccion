# Acuerdos comerciales B2B — Fases 1, 1.1 y 2

## Alcance

El módulo incorpora el expediente editable de un acuerdo comercial por cliente B2B. Permite crear la primera versión, conservar snapshots del cliente, editar personas autorizadas y apartados contractuales, consultar el historial, validar campos faltantes y generar un PDF inmutable.

No gestiona todavía firma o archivo y no incluye productos ni anexos de catálogo.

## Acceso y permisos

Los permisos son:

- `b2b_commercial_agreement.view`: historial y detalle.
- `b2b_commercial_agreement.edit`: crear y editar borradores.
- `b2b_commercial_agreement.generate`: ejecutar la validación previa a una futura generación.
- `b2b_commercial_agreement.sign_archive`: reservado para la futura fase de firma y archivo.

`admin` recibe los cuatro permisos. `sales_manager` recibe `view`, `edit` y `generate`. Los roles `general_operator` y `general_approver` no reciben permisos de acuerdos comerciales.

## Flujo

1. En **B2B Customers**, la acción **Acuerdo comercial** abre el historial del cliente.
2. **Crear acuerdo comercial** crea la versión `0` si es el primer acuerdo, con estado `draft`.
3. Se copian snapshots de identidad, ubicación, crédito, canal y estado del cliente.
4. Se crean 19 apartados contractuales con texto base editable y variables del acuerdo.
5. El borrador permite registrar representación legal, contactos, condiciones, personas autorizadas y aceptación.
6. **Validar acuerdo** muestra de una vez todos los campos pendientes. No genera documentos ni cambia el estado.
7. **Generar PDF** vuelve a validar y, si el acuerdo está completo, guarda los bytes, nombre seguro, SHA-256, fecha y usuario de generación; luego cambia el estado a `generated`.
8. **Descargar PDF** devuelve exactamente los bytes persistidos. No regenera el documento.

## Plantilla contractual y variables

La Fase 1.1 sustituye los placeholders por una plantilla operativa de 19 apartados. Los textos se guardan en cada borrador y continúan siendo editables. En el editor las variables se muestran literalmente, por ejemplo `{{customer_name}}`, `{{contract_credit_days}}` y `{{credit_limit}}`. En el detalle se sustituyen con los snapshots y condiciones del acuerdo; una variable conocida sin valor se presenta como `______` y una variable desconocida se conserva sin evaluar.

La acción **Actualizar textos desde plantilla base** está disponible únicamente para borradores y requiere `b2b_commercial_agreement.edit`. Reemplaza títulos y textos de los 19 apartados, sin cambiar snapshots del cliente, condiciones comerciales, personas autorizadas ni datos de firma. La acción registra `commercial_agreement_sections_reset` en auditoría.

## Opciones comerciales y personas autorizadas

El editor limita **Forma de pago** a pago anticipado antes del despacho, contado contra entrega, crédito comercial aprobado o consignación y pago contra entrega. La selección mantiene automáticamente `advance_payment_enabled` y `credit_approved`; solo el crédito comercial exige plazo y límite positivos. La consignación es únicamente una condición comercial en esta fase y no activa crédito ni cambia procesos operativos. **Entrega habitual** se limita a ruta de entrega GREEN CORNER, encomienda o retiro.

Cada cliente B2B puede guardar una forma de pago predeterminada nullable en `b2b_customers.default_payment_terms`. Los clientes históricos permanecen sin valor hasta que un usuario los edite. Un acuerdo nuevo copia explícitamente ese valor y sus indicadores al momento de crear el borrador. Después funciona como snapshot: editar la ficha del cliente no modifica acuerdos existentes. Si la ficha y un borrador difieren, el editor ofrece una acción explícita y separada para copiar la forma de pago y sus indicadores; los acuerdos `generated`, `signed` y `archived` nunca se actualizan por esta vía.

## Sincronización explícita de borradores

El editor permite revisar y confirmar la actualización de un borrador individual desde la ficha actual del cliente. El historial existente también permite revisar todos los borradores del sistema y aplicar sus cambios en una sola transacción. Ambos flujos presentan primero una vista previa, requieren `b2b_commercial_agreement.edit`, CSRF, validación same-origin y confirmación firmada para la sesión. El fingerprint incluye los borradores, su estado, los snapshots y los datos actuales del cliente; un cambio entre vista previa y confirmación obliga a revisar de nuevo.

Se sincronizan nombre comercial, razón social, cédula jurídica, teléfono, dirección, provincia, cantón, distrito, días de crédito del cliente, canal B2B y estado activo. `billing_address` y `whatsapp` solo acompañan el cambio cuando todavía coinciden con el snapshot original; si fueron personalizados, se conservan y la vista previa informa la excepción.

La sincronización no sobrescribe forma de pago, indicadores de pago/crédito, plazo contractual, límite de crédito, descuentos, representación legal, correos, personas autorizadas, textos ni datos de firma. Una diferencia de forma de pago se informa y se resuelve únicamente mediante su acción comercial separada. Solo se actualizan acuerdos `draft`; los PDF y las versiones `generated`, `signed` o `archived` permanecen inmutables. La actualización masiva y sus entradas de auditoría se confirman en la misma transacción, con rollback completo si falla cualquier registro.

Las personas autorizadas se editan con nombre, cargo y un único campo **Teléfono / correo**. Internamente se conservan las columnas existentes `identification`, `phone` y `email`: la identificación previa viaja oculta para evitar pérdida de históricos y el contacto combinado se separa nuevamente al guardar. No se agregó ninguna columna ni migración.

Solo puede existir un borrador por cliente, validado por el servicio. En esta fase únicamente se crea la versión inicial `0`; la interfaz y la copia para crear versiones posteriores quedan expresamente fuera de alcance. Los estados `generated`, `signed` y `archived` están preparados y son inmutables en la edición actual. Una fase futura deberá crear cada nueva versión con `MAX(version_number) + 1`.

## Persistencia

- `b2b_commercial_agreements`: cabecera, snapshots, condiciones, metadatos de versión y campos reservados para un PDF futuro.
- `b2b_commercial_agreement_people`: personas autorizadas para pedidos o recepción.
- `b2b_commercial_agreement_sections`: los 19 apartados editables.

Las tablas del acuerdo se crean mediante `Base.metadata.create_all()`. Los campos de persistencia del PDF ya forman parte de `b2b_commercial_agreements`; no se requiere migración manual.

La columna nullable `b2b_customers.default_payment_terms VARCHAR(255)` se agrega mediante `ensure_b2b_customer_default_payment_terms_column()`, usando el patrón incremental e idempotente de esquema del ERP tanto en PostgreSQL como en SQLite. La migración no define default, no rellena históricos y no cambia acuerdos existentes.

## PDF generado

La generación usa ReportLab en tamaño carta vertical. El documento contiene encabezado corporativo y logo opcional, información del cliente, condiciones comerciales, personas autorizadas, los 19 apartados con variables resueltas, aceptación, firmas y pie de página con versión y página.

Solo un acuerdo `draft` completo puede generar PDF. Una vez en estado `generated`, no puede editarse ni regenerarse; los estados `signed` y `archived` también conservan la descarga del PDF original. Un cambio futuro deberá crear una nueva versión.

### Reapertura administrativa para corregir datos

Un acuerdo `generated` que nunca fue firmado ni archivado puede reabrirse para corregir errores del documento original sin crear una nueva versión comercial. La acción requiere `b2b_commercial_agreement.sign_archive`, motivo obligatorio, confirmación expresa de ausencia de firma, CSRF, validación same-origin, fingerprint y bloqueo transaccional.

Antes de volver el acuerdo a `draft`, el sistema verifica el encabezado `%PDF` y el SHA-256 del documento activo y copia sus bytes y metadatos a `b2b_commercial_agreement_invalidated_pdfs`. Ese respaldo es inmutable desde la aplicación, se identifica como documento invalidado y solo puede descargarse con el permiso sensible anterior. El estado, archivo histórico, auditoría y limpieza de referencias del PDF activo se confirman en una sola transacción; cualquier error provoca rollback completo.

El acuerdo conserva su `version_number` y todas sus condiciones contractuales. Después de reabrirlo se utiliza la sincronización individual existente para copiar los snapshots vigentes del cliente, y luego el flujo normal de generación crea el PDF corregido. Los acuerdos `signed`, `archived` o con cualquier metadato de firma o archivo nunca son elegibles.

La tabla histórica es un modelo normal de SQLAlchemy registrado antes de `Base.metadata.create_all()`. Su creación usa el mecanismo idempotente existente tanto en PostgreSQL como en SQLite y no requiere SQL manual en Render.

El PDF no consulta productos, no incorpora SKUs, precios de productos, catálogo ni Anexo 1. Únicamente conserva la referencia textual al catálogo comercial vigente adjunto.

## Auditoría

Se registran los eventos:

- `commercial_agreement_created`
- `commercial_agreement_updated`
- `commercial_agreement_validated`
- `commercial_agreement_sections_reset`
- `commercial_agreement_generated`
- `commercial_agreement_customer_data_synced`

## Límites vigentes

- No se firma ni archiva un acuerdo.
- No se consulta el catálogo B2B, productos ni precios.
- No se genera Anexo 1; solo se conserva la referencia textual al catálogo vigente.
- No se consultan ni modifican órdenes, facturación, CxC, inventario, Kardex, producción o Loyverse.
