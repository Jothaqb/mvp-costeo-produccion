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

4\. luego calcular un Average Cost corregido usando el costo real unitario del sistema local



\## Regla de cantidad

Usar `output\_qty`, no `planned\_qty`.



\## Fórmula de Average Cost corregido

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



\## Decisiones pendientes a validar con API de Loyverse

\- si se puede crear Production por API

\- si se puede leer Average Cost por API

\- si se puede escribir Average Cost por API

\- si el Average Cost es por tienda o global

\- qué identificadores usar: SKU, item\_id, variant\_id, store\_id

\- cómo leer stock actual del item de forma confiable



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



\## Entregable esperado de Codex

\- propuesta técnica por fases

\- preguntas a validar con la API

\- propuesta de campos mínimos para trazabilidad

\- estrategia de errores/reintentos

\- riesgos y tradeoffs

\- archivos que tocaría más adelante

