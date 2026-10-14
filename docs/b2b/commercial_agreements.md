# Acuerdos comerciales B2B — Fases 1 y 1.1

## Alcance

Esta fase incorpora el expediente editable de un acuerdo comercial por cliente B2B. Permite crear la primera versión, conservar snapshots del cliente, editar personas autorizadas y apartados contractuales, consultar el historial y validar campos faltantes.

No genera PDF, no cambia el estado `draft`, no gestiona firma o archivo y no incluye productos ni anexos de catálogo.

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

## Plantilla contractual y variables

La Fase 1.1 sustituye los placeholders por una plantilla operativa de 19 apartados. Los textos se guardan en cada borrador y continúan siendo editables. En el editor las variables se muestran literalmente, por ejemplo `{{customer_name}}`, `{{contract_credit_days}}` y `{{credit_limit}}`. En el detalle se sustituyen con los snapshots y condiciones del acuerdo; una variable conocida sin valor se presenta como `______` y una variable desconocida se conserva sin evaluar.

La acción **Actualizar textos desde plantilla base** está disponible únicamente para borradores y requiere `b2b_commercial_agreement.edit`. Reemplaza títulos y textos de los 19 apartados, sin cambiar snapshots del cliente, condiciones comerciales, personas autorizadas ni datos de firma. La acción registra `commercial_agreement_sections_reset` en auditoría.

## Opciones comerciales y personas autorizadas

El editor limita **Forma de pago** a pago anticipado antes del despacho, crédito comercial aprobado o contado contra entrega. La selección mantiene automáticamente `advance_payment_enabled` y `credit_approved`; solo el crédito comercial exige plazo y límite positivos. **Entrega habitual** se limita a ruta de entrega GREEN CORNER, encomienda o retiro.

Las personas autorizadas se editan con nombre, cargo y un único campo **Teléfono / correo**. Internamente se conservan las columnas existentes `identification`, `phone` y `email`: la identificación previa viaja oculta para evitar pérdida de históricos y el contacto combinado se separa nuevamente al guardar. No se agregó ninguna columna ni migración.

Solo puede existir un borrador por cliente, validado por el servicio. En esta fase únicamente se crea la versión inicial `0`; la interfaz y la copia para crear versiones posteriores quedan expresamente fuera de alcance. Los estados `generated`, `signed` y `archived` están preparados y son inmutables en la edición actual. Una fase futura deberá crear cada nueva versión con `MAX(version_number) + 1`.

## Persistencia

- `b2b_commercial_agreements`: cabecera, snapshots, condiciones, metadatos de versión y campos reservados para un PDF futuro.
- `b2b_commercial_agreement_people`: personas autorizadas para pedidos o recepción.
- `b2b_commercial_agreement_sections`: los 19 apartados editables.

Las tablas se crean mediante `Base.metadata.create_all()`. No hay migración manual ni modificación de tablas operativas existentes.

## Auditoría

Se registran los eventos:

- `commercial_agreement_created`
- `commercial_agreement_updated`
- `commercial_agreement_validated`
- `commercial_agreement_sections_reset`

## Límites de Fase 1

- No se genera, descarga ni persiste un PDF.
- No se firma ni archiva un acuerdo.
- No se consulta el catálogo B2B, productos ni precios.
- No se genera Anexo 1; solo se conserva la referencia textual al catálogo vigente.
- No se consultan ni modifican órdenes, facturación, CxC, inventario, Kardex, producción o Loyverse.
