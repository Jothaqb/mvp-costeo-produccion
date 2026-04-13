\# Sprint 6A – Diseño de integración con Loyverse



\## Objetivo

Diseñar la integración entre el cierre de una Production Order local y Loyverse, sin implementar código todavía.



\## Contexto del sistema

\- Sprints 1–5 completos y estables

\- Sprint 5.1 completo

\- La fuente de verdad del costo real es este sistema

\- Loyverse sigue siendo fuente de productos/BOM/costo base y sistema operativo de inventario/ventas

\- No existe utility de backfill/recalculation

\- Las órdenes cerradas deben permanecer históricamente estables



\## Objetivo de negocio

Al cerrar una Production Order local, después de calcular el costo real:

1\. conectar con Loyverse

2\. crear una Production en Loyverse

3\. usar SKU del producto y `output\_qty`

4\. más adelante, en una fase separada, evaluar la corrección del Average Cost usando el costo real unitario del sistema local



\## Regla de cantidad

Usar `output\_qty`, no `planned\_qty`.



\## Fórmula objetivo para una futura corrección de Average Cost

`stock\_effective\_before = max(stock\_before, 0)`



`new\_avg\_cost = ((stock\_effective\_before \* avg\_cost\_before) + (output\_qty \* real\_unit\_cost)) / (stock\_effective\_before + output\_qty)`



\## Reglas funcionales

\- Si `stock\_before = 0`, el nuevo Average Cost queda igual a `real\_unit\_cost`

\- Si `stock\_before < 0`, tratarlo como `0` para la fórmula

\- El cierre local de la OP no debe depender de que Loyverse responda bien

\- La sincronización con Loyverse es un paso posterior al cierre local exitoso

\- Si Loyverse falla, la OP local sigue cerrada

\- Debe existir trazabilidad de sync y capacidad de retry seguro

\- No mezclar este sprint con packaging costing



\## Enfoque por fases



\### 6A – Diseño / Validación

\- Validar capacidades reales de la API de Loyverse antes de implementar

\- Confirmar si se puede crear Production por API

\- Confirmar si se puede leer y escribir Average Cost por API

\- Confirmar identificadores, alcance de inventario y alcance de costo

\- Definir estrategia de idempotencia y prevención de duplicados

\- No modificar el flujo local de cierre



\### 6B – Scaffolding mínimo de integración

Campos mínimos propuestos para trazabilidad:

\- `loyverse\_sync\_status`

\- `loyverse\_sync\_error`

\- `loyverse\_last\_sync\_attempt\_at`

\- `loyverse\_sync\_attempt\_count`

\- `loyverse\_production\_id`

\- `loyverse\_synced\_at`



Notas:

\- No agregar todavía campos de `avg\_cost\_before`, `avg\_cost\_after`, `stock\_before`, `item\_id`, `variant\_id` o `store\_id` salvo que la validación de API demuestre que son estrictamente necesarios

\- El objetivo de 6B es trazabilidad, no integrar todavía el costeo final



\### 6C – Crear Production en Loyverse

\- La sincronización debe iniciar como acción manual, no automática

\- Crear Production en Loyverse solo para órdenes locales ya cerradas

\- Usar `product\_sku\_snapshot` y `output\_qty`

\- Guardar `loyverse\_production\_id`

\- Marcar claramente el estado de sync

\- No corregir Average Cost todavía en esta fase



\### 6D – Corrección de Average Cost

\- Fase separada y posterior

\- Solo avanzar cuando la API real de Loyverse esté validada

\- Leer stock/costo requeridos antes de aplicar la fórmula

\- Si `stock\_before < 0`, usar `0`

\- Si `stock\_before = 0`, el resultado es `real\_unit\_cost`

\- Agregar campos de trazabilidad de costo solo cuando esta fase se implemente realmente



\## Por qué manual sync es más seguro primero

\- El cierre local ya es la fuente de verdad y debe permanecer estable

\- Las capacidades reales de la API de Loyverse aún no están completamente validadas

\- Un fallo de API no debe introducir ambigüedad en el cierre local

\- Manual sync permite confirmar que la orden ya está cerrada, costeada y lista antes de enviarla

\- Reduce el riesgo de duplicados mientras se valida la estrategia de idempotencia

\- Hace más fácil entender fallos parciales: la OP local sigue cerrada y el estado de sync explica qué ocurrió

\- Evita acoplar el flujo sensible de cierre con una API externa sujeta a red, auth, rate limits y validaciones



\## Camino de transición: de manual sync a automático

1\. \*\*6B\*\*: agregar campos mínimos de trazabilidad y visualización de estado

2\. \*\*6C\*\*: agregar acción manual “Sync to Loyverse” solo para órdenes cerradas

3\. Implementar prevención de duplicados:

&#x20;  - no crear Production nueva si `loyverse\_production\_id` ya existe

&#x20;  - usar `internal\_order\_number` o equivalente como referencia externa si Loyverse lo soporta

4\. Validar comportamiento real con un grupo pequeño de órdenes cerradas

5\. Solo después, considerar un modo automático detrás de una bandera de configuración, por ejemplo:

&#x20;  - `LOYVERSE\_AUTO\_SYNC\_ON\_CLOSE=false`

6\. En modo automático:

&#x20;  - el cierre local siempre se confirma primero

&#x20;  - luego ocurre la sincronización como paso post-close

&#x20;  - si falla, la OP local sigue cerrada y el estado de sync pasa a `failed`

&#x20;  - el retry manual debe seguir disponible



\## Preguntas API que deben validarse antes de codificar

\- ¿Se pueden crear documentos de Production por API?

\- ¿Qué endpoint y payload crean una Production?

\- ¿Se puede leer Average Cost por API?

\- ¿Se puede escribir Average Cost por API?

\- ¿Average Cost es global, por tienda o por variante?

\- ¿La API usa SKU, `item\_id`, `variant\_id`, `store\_id` o una combinación?

\- ¿SKU es suficientemente único para productos manufacturados?

\- ¿Qué endpoint/campo da un `stock\_before` confiable?

\- ¿Crear una Production altera inmediatamente stock y Average Cost?

\- ¿Existe campo de referencia externa o idempotency key?

\- ¿Se puede buscar en Loyverse por el `internal\_order\_number` local?

\- ¿Qué rate limits, scopes y formatos de error aplica la API?



\## Restricciones

\- No escribir código todavía

\- No aplicar cambios todavía

\- No mezclar con dashboards, analytics, exports o packaging

\- No modificar sprints anteriores



\## Archivos sensibles

\- models.py

\- main.py

\- database.py

\- import\_service.py

\- production\_order\_service.py

\- costing\_service.py



\## Archivos probables a tocar en fases futuras

\- `app/models.py`

\- `app/database.py`

\- `app/main.py`

\- `app/services/loyverse\_service.py`

\- `app/services/production\_order\_service.py`

\- `app/templates/production\_order\_detail.html`



\## Entregable esperado de Codex en esta fase

\- propuesta técnica por fases

\- preguntas a validar con la API

\- propuesta de campos mínimos para trazabilidad

\- estrategia de errores, retry e idempotencia

\- riesgos y tradeoffs

\- recomendación del primer paso más seguro



\## Regla de seguridad

\- No hardcodear tokens

\- Usar variable de entorno, por ejemplo `LOYVERSE\_API\_TOKEN`

\- No guardar secretos en código, markdown, templates o Git

