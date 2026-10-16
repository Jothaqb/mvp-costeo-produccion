# Revisión comercial de inactividad B2B mediante Excel

## Alcance

La pantalla `/b2b/commercial-inactivity-review` permite:

1. descargar un XLSX firmado con propuestas de inactivación de asignaciones
   `B2BCustomerProduct`;
2. eliminar las filas que deben mantenerse activas;
3. cargar el mismo archivo y revisar una vista previa revalidada;
4. confirmar expresamente la inactivación de las filas conservadas y aprobadas.

La confirmación modifica exclusivamente `B2BCustomerProduct.active`, de `true` a `false`. Nunca
modifica `B2BCustomer.active`, `Product.active`, `Product.available_for_sale_gc`, precios,
pedidos, facturación, CxC, inventario, Kardex, producción ni Loyverse.

## Configuración requerida

Render debe definir `B2B_INACTIVITY_REVIEW_SECRET` con un secreto estable de al menos 32
caracteres. El secreto firma el manifiesto oculto del XLSX mediante HMAC-SHA256. Debe ser el
mismo en todas las instancias y no debe registrarse en logs ni incluirse en el archivo.

La validación same-origin reutiliza `APP_BASE_URL` o `RENDER_EXTERNAL_URL`, igual que el panel de
roles y permisos. En producción debe configurarse el origen público real del ERP.

## Permisos

- `b2b_commercial_inactivity.download`: descargar el XLSX firmado.
- `b2b_commercial_inactivity.validate`: cargar y validar el XLSX.
- `b2b_commercial_inactivity.execute`: confirmar la inactivación de asignaciones aprobadas.

El seed crea los permisos en el catálogo y el rol `admin` conserva todos los permisos. Los roles
no administradores existentes no reciben permisos nuevos automáticamente; deben asignarse desde
el panel de roles y permisos.

## Universo del XLSX

Se reutiliza `build_b2b_commercial_inactivity_plan()` con:

- fecha operativa de Costa Rica;
- ventana móvil de 12 meses;
- únicamente órdenes B2B `invoiced`;
- jerarquía canónica de fecha de facturación;
- los cuatro canales autorizados resueltos por nombre exacto;
- validación exacta de SKU y pedidos abiertos.

El archivo incluye una fila por asignación activa cliente–SKU que no tiene compra facturada en la
ventana. Los productos con compra reciente no se presentan como propuestas. Las asignaciones con
otra restricción se incluyen marcadas `PROTEGIDO` para dar visibilidad, pero nunca se consideran
aprobables.

La falta de cobertura histórica certificada no se oculta. Una fila aprobable indica expresamente
que corresponde a la vía B: aprobación comercial manual con trazabilidad, no a una conclusión
automática basada en histórico certificado.

## Columnas

1. Canal B2B.
2. Cliente.
3. SKU.
4. Descripción del producto.
5. Fecha de última compra facturada.
6. Monto neto facturado del SKU en la última compra.
7. Estado actual del producto en el catálogo.
8. Motivo de propuesta de inactivación.
9. Identificador interno de asignación producto–cliente.
10. Identificador único del proceso de revisión.

Cuando no existe compra registrada se muestra `Sin compra registrada en ERP`. El monto de la
última compra suma `B2BSalesOrderLine.line_total` del SKU en todas las facturas válidas de la fecha
más reciente. Si no se puede determinar con seguridad, el archivo muestra `No disponible` y el
motivo.

## Reglas de carga

- Fila conservada y aprobable: propuesta aprobada para confirmación.
- Fila eliminada y aprobable: mantener activa.
- Archivo sin filas: cero aprobaciones.
- Fila protegida: nunca se autoriza, se conserve o se elimine.
- Encabezados, celdas y hojas no pueden editarse.
- Filas nuevas, duplicadas o pertenecientes a otra revisión invalidan toda la carga.
- Los nombres, descripciones y montos no identifican registros; el ID interno se verifica contra el
  manifiesto firmado.
- El manifiesto oculto conserva el universo original y su fingerprint. Cualquier alteración rompe
  la firma HMAC.

La importación vuelve a construir el plan con datos actuales. Ventas recientes, pedidos abiertos,
cambios de canal, cambios de SKU, inactivaciones previas o desaparición del registro convierten la
fila en protegida. Un cambio global de fingerprint se muestra claramente en la vista previa y
bloquea la confirmación.

## CSRF y origen

Tanto la importación como la confirmación exigen:

1. sesión válida;
2. su permiso específico (`validate` o `execute`) verificado en backend;
3. token CSRF HMAC vinculado a la cookie de sesión y al propósito específico del formulario;
4. `Origin` o `Referer` coincidente con el origen público configurado;
5. para la carga, archivo `.xlsx` de máximo 10 MB.

Una falla produce HTTP 403 o una carga rechazada sin escrituras.

## Confirmación definitiva

Después de una carga válida, el servidor emite una autorización HMAC de corta duración vinculada a:

- la sesión autenticada;
- el identificador y hash del manifiesto firmado;
- el fingerprint y fecha operativa del plan revalidado;
- la lista exacta de asignaciones aprobadas, con cliente, canal y SKU.

El navegador envía esa autorización firmada; no envía una lista libre de IDs. El POST separado
`/b2b/commercial-inactivity-review/execute` exige además una aceptación expresa de que se trata de
una revisión comercial manual con histórico todavía no certificado. Esta aceptación no modifica
la cobertura declarada del job automático.

La ejecución bloquea con `FOR UPDATE` los clientes y asignaciones, reconstruye el plan dentro de
la misma transacción y exige el fingerprint exacto. Revalida canal autorizado, cliente activo,
asignación activa, SKU, ventas facturadas recientes, pedidos abiertos y demás protecciones. Si
cualquier fila cambió, se rechaza el lote completo y se solicita una nueva revisión; no hay
ejecución parcial. Cada cambio registra actor, fecha, cliente, SKU, ID de asignación, review ID,
hash del manifiesto, fingerprint y resultado en `audit_logs`. Cualquier error provoca rollback
completo. Una segunda confirmación se rechaza porque las asignaciones ya no están activas y el
fingerprint deja de coincidir.

## Reactivaciones manuales y automatización futura

No existe Cron ni ejecución automática en este flujo. Si una asignación se reactiva manualmente,
solo podrá volver a inactivarse después de una nueva descarga, carga, vista previa y confirmación
expresa. La autorización vence en una hora, está ligada a la sesión y cualquier cambio en la
asignación invalida el fingerprint.

Antes de automatizar mensualmente el proceso debe diseñarse una excepción persistente o un campo
`protected_until` aprobado para distinguir reactivaciones deliberadas. No se agregó esquema ni se
marcó el histórico como certificado en esta implementación.
