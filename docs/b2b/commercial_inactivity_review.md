# Revisión comercial de inactividad B2B mediante Excel

## Alcance de la primera etapa

La pantalla `/b2b/commercial-inactivity-review` permite:

1. descargar un XLSX firmado con propuestas de inactivación de asignaciones
   `B2BCustomerProduct`;
2. eliminar las filas que deben mantenerse activas;
3. cargar el mismo archivo y revisar una vista previa revalidada.

Esta etapa no expone ruta, botón ni servicio web para ejecutar inactivaciones. Tampoco modifica
`B2BCustomer.active`, `B2BCustomerProduct.active`, `Product.active` ni
`Product.available_for_sale_gc`.

## Configuración requerida

Render debe definir `B2B_INACTIVITY_REVIEW_SECRET` con un secreto estable de al menos 32
caracteres. El secreto firma el manifiesto oculto del XLSX mediante HMAC-SHA256. Debe ser el
mismo en todas las instancias y no debe registrarse en logs ni incluirse en el archivo.

La validación same-origin reutiliza `APP_BASE_URL` o `RENDER_EXTERNAL_URL`, igual que el panel de
roles y permisos. En producción debe configurarse el origen público real del ERP.

## Permisos

- `b2b_commercial_inactivity.download`: descargar el XLSX firmado.
- `b2b_commercial_inactivity.validate`: cargar y validar el XLSX.
- `b2b_commercial_inactivity.execute`: reservado para una fase futura; no protege ningún endpoint
  porque la ejecución está deshabilitada.

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

- Fila conservada y aprobable: propuesta aprobada para una futura confirmación.
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
fila en protegida. Un cambio global de fingerprint se muestra claramente en la vista previa.

## CSRF y origen

El POST exige:

1. sesión válida;
2. permiso `b2b_commercial_inactivity.validate` en backend;
3. token CSRF HMAC vinculado a la cookie de sesión y a este formulario;
4. `Origin` o `Referer` coincidente con el origen público configurado;
5. archivo `.xlsx` de máximo 10 MB.

Una falla produce HTTP 403 o una carga rechazada sin escrituras.

## Diseño propuesto para la confirmación definitiva

La fase futura debe implementar una operación separada que:

1. exija `b2b_commercial_inactivity.execute` y un segundo CSRF/origen;
2. muestre una frase de confirmación explícita sobre el uso de histórico no certificado;
3. acepte únicamente IDs aprobados de una revisión firmada y recién revalidada;
4. rechace cualquier fingerprint diferente;
5. bloquee con `FOR UPDATE` las asignaciones y clientes relacionados;
6. reconstruya el plan dentro de la misma transacción;
7. modifique únicamente `B2BCustomerProduct.active` de las filas aprobadas;
8. registre auditoría con actor, review ID, fingerprints, fecha, IDs, estado anterior/nuevo y
   aceptación explícita del alcance histórico;
9. haga rollback completo ante cualquier error;
10. sea idempotente y no modifique clientes ni productos globales.

Antes de habilitar esa fase debe resolverse el requisito pendiente de reactivaciones manuales con
una excepción persistente o `protected_until` aprobado. No se agregó esquema en esta etapa.
