\# Sprint 6A.1 – Validación manual de API Loyverse



\## Objetivo

Validar manualmente, fuera de la aplicación, qué operaciones soporta realmente la API de Loyverse para el flujo futuro de integración con Production Orders.



\## Contexto

\- Sprint 6A ya definió el diseño general de integración con Loyverse

\- Aún no está probado que la API permita:

&#x20; - crear Production

&#x20; - leer stock actual

&#x20; - leer Average Cost

&#x20; - escribir Average Cost

\- No se debe escribir código de integración en la app hasta validar estas capacidades

\- El sistema local sigue siendo la fuente de verdad del costo real por OP



\## Objetivo de negocio a validar

Determinar si en el futuro será técnicamente posible soportar este flujo:



1\. cerrar la Production Order local

2\. sincronizar con Loyverse

3\. crear una Production en Loyverse usando SKU y `output\_qty`

4\. eventualmente evaluar una corrección externa del Average Cost usando el costo real unitario del sistema local



\## Regla importante

Esta fase es solo de validación manual y recolección de evidencia.



No incluye:

\- cambios en código de la app

\- cambios en base de datos

\- cambios en modelos

\- integración automática

\- write-back todavía



\## Preguntas que deben responderse con evidencia

1\. ¿Existe endpoint real para crear Production por API?

2\. ¿Existe endpoint real para leer stock actual por producto?

3\. ¿Existe endpoint real para leer Average Cost?

4\. ¿Existe endpoint real para escribir Average Cost?

5\. ¿La API trabaja por SKU, item\_id, variant\_id, store\_id o una combinación?

6\. ¿Stock es global o por tienda?

7\. ¿Average Cost es global, por variante o por tienda?

8\. ¿Crear una Production actualiza Average Cost inmediatamente?

9\. ¿Existe alguna referencia externa o mecanismo de idempotencia?

10\. ¿Qué errores devuelve la API en auth, validación, duplicados o rate limits?



\## Alcance de validación manual

Se puede validar mediante:

\- documentación oficial

\- pruebas manuales con token API

\- herramientas como Postman, curl o requests locales fuera de la app

\- cuenta de prueba o ambiente controlado si existe



\## Evidencia esperada

Para cada hipótesis validada, guardar:

\- endpoint probado

\- método HTTP

\- payload usado

\- respuesta recibida

\- campos relevantes encontrados

\- conclusión: soportado / no soportado / no concluyente



\## Resultado esperado

Al final de esta fase debe quedar claro cuál de estas opciones aplica:



\### Opción A

La API sí permite crear Production y leer datos suficientes para avanzar a Sprint 6B/6C



\### Opción B

La API permite solo lectura parcial y requiere rediseñar el flujo



\### Opción C

La API no soporta Production o no soporta Average Cost write-back, y la estrategia debe cambiar



\## Riesgos a vigilar

\- asumir capacidades no demostradas

\- confundir UI/back-office con API real

\- que Loyverse recalcule Average Cost automáticamente y choque con una corrección externa

\- usar SKU cuando la API requiera item\_id/variant\_id/store\_id

\- tocar luego el close flow sin validar primero la integración externa



\## Restricciones

\- No escribir código en la app todavía

\- No modificar sprints anteriores

\- No mezclar con packaging costing

\- No hardcodear tokens

\- Usar variable de entorno `LOYVERSE\_API\_TOKEN` si se hacen pruebas locales



\## Archivos sensibles que no deben tocarse en esta fase

\- models.py

\- main.py

\- database.py

\- import\_service.py

\- production\_order\_service.py

\- costing\_service.py



\## Entregable esperado

\- lista de capacidades reales validadas

\- lista de capacidades no soportadas o no concluyentes

\- evidencia por endpoint/prueba

\- recomendación final:

&#x20; - abrir 6B mínimo

&#x20; - seguir investigando

&#x20; - rediseñar la estrategia de integración

