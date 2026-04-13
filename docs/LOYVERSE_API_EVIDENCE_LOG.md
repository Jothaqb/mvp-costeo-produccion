\# Loyverse API Evidence Log



\## Objetivo

Registrar evidencia real de las pruebas manuales de la API de Loyverse para decidir si la integración del sistema de costeo puede avanzar a implementación.



\## Reglas de uso

\- No registrar tokens ni secretos

\- Resumir payloads y respuestas sin exponer datos sensibles

\- Marcar cada prueba como:

&#x20; - `supported`

&#x20; - `unsupported`

&#x20; - `inconclusive`



\## Resumen ejecutivo

| Fecha | Prueba | Resultado | Impacto |

|---|---|---:|---|

| YYYY-MM-DD | Auth smoke test | inconclusive | Falta validar token |

| YYYY-MM-DD | Create Production by API | unsupported | No abrir 6C todavía |



\---



\## Evidencia detallada



\### 1. Auth smoke test

\*\*Fecha:\*\*  

\*\*Objetivo:\*\* Confirmar que el token funciona fuera de la app.  

\*\*Endpoint / recurso inspeccionado:\*\*  

\*\*Método HTTP:\*\*  

\*\*Payload resumido:\*\*  

\*\*Respuesta resumida:\*\*  

\*\*Código HTTP:\*\*  

\*\*Headers relevantes:\*\*  

\*\*Veredicto:\*\* supported / unsupported / inconclusive  

\*\*Notas:\*\*  



\---



\### 2. List stores

\*\*Fecha:\*\*  

\*\*Objetivo:\*\* Confirmar si `store\_id` será requerido.  

\*\*Endpoint / recurso inspeccionado:\*\*  

\*\*Método HTTP:\*\*  

\*\*Payload resumido:\*\*  

\*\*Respuesta resumida:\*\*  

\*\*Código HTTP:\*\*  

\*\*Campos relevantes encontrados:\*\*  

\- store\_id:

\- store\_name:

\- active:

\*\*Veredicto:\*\* supported / unsupported / inconclusive  

\*\*Notas:\*\*  



\---



\### 3. Find manufactured product by SKU

\*\*Fecha:\*\*  

\*\*Objetivo:\*\* Validar cómo resolver un producto local por SKU en Loyverse.  

\*\*Endpoint / recurso inspeccionado:\*\*  

\*\*Método HTTP:\*\*  

\*\*Payload resumido:\*\*  

\*\*Respuesta resumida:\*\*  

\*\*Código HTTP:\*\*  

\*\*Campos relevantes encontrados:\*\*  

\- sku:

\- item\_id:

\- variant\_id:

\- use\_production:

\- composite / components:

\*\*Veredicto:\*\* supported / unsupported / inconclusive  

\*\*Notas:\*\*  



\---



\### 4. Read stock

\*\*Fecha:\*\*  

\*\*Objetivo:\*\* Confirmar si stock se puede leer por API y si es global o por tienda.  

\*\*Endpoint / recurso inspeccionado:\*\*  

\*\*Método HTTP:\*\*  

\*\*Payload resumido:\*\*  

\*\*Respuesta resumida:\*\*  

\*\*Código HTTP:\*\*  

\*\*Campos relevantes encontrados:\*\*  

\- stock\_quantity:

\- store\_id:

\- item\_id:

\- variant\_id:

\*\*Veredicto:\*\* supported / unsupported / inconclusive  

\*\*Notas:\*\*  



\---



\### 5. Read Average Cost

\*\*Fecha:\*\*  

\*\*Objetivo:\*\* Confirmar si Average Cost es visible por API.  

\*\*Endpoint / recurso inspeccionado:\*\*  

\*\*Método HTTP:\*\*  

\*\*Payload resumido:\*\*  

\*\*Respuesta resumida:\*\*  

\*\*Código HTTP:\*\*  

\*\*Campos relevantes encontrados:\*\*  

\- average\_cost:

\- cost:

\- valuation fields:

\*\*Veredicto:\*\* supported / unsupported / inconclusive  

\*\*Notas:\*\*  



\---



\### 6. Create Production by API

\*\*Fecha:\*\*  

\*\*Objetivo:\*\* Confirmar si se puede crear Production por API.  

\*\*Endpoint / recurso inspeccionado:\*\*  

\*\*Método HTTP:\*\*  

\*\*Payload resumido:\*\*  

\*\*Respuesta resumida:\*\*  

\*\*Código HTTP:\*\*  

\*\*Campos relevantes encontrados:\*\*  

\- production\_id:

\- item\_id / variant\_id:

\- quantity:

\- store\_id:

\- note / external reference:

\*\*Veredicto:\*\* supported / unsupported / inconclusive  

\*\*Notas:\*\*  



\---



\### 7. Verify stock movement after Production

\*\*Fecha:\*\*  

\*\*Objetivo:\*\* Confirmar el cambio real de stock después de Production.  

\*\*Lectura antes:\*\*  

\*\*Lectura después:\*\*  

\*\*Producto producido:\*\*  

\*\*Componentes:\*\*  

\*\*Veredicto:\*\* supported / unsupported / inconclusive  

\*\*Notas:\*\*  



\---



\### 8. Verify Average Cost movement after Production

\*\*Fecha:\*\*  

\*\*Objetivo:\*\* Confirmar si Loyverse actualiza Average Cost automáticamente tras Production.  

\*\*Average Cost antes:\*\*  

\*\*Average Cost después:\*\*  

\*\*Veredicto:\*\* supported / unsupported / inconclusive  

\*\*Notas:\*\*  



\---



\### 9. Attempt Average Cost write-back

\*\*Fecha:\*\*  

\*\*Objetivo:\*\* Confirmar si Average Cost se puede escribir por API.  

\*\*Precondición:\*\* Solo ejecutar si existe endpoint/documentación oficial suficiente.  

\*\*Endpoint / recurso inspeccionado:\*\*  

\*\*Método HTTP:\*\*  

\*\*Payload resumido:\*\*  

\*\*Respuesta resumida:\*\*  

\*\*Código HTTP:\*\*  

\*\*Resultado en Loyverse después del intento:\*\*  

\*\*Veredicto:\*\* supported / unsupported / inconclusive  

\*\*Notas:\*\*  



\---



\### 10. Duplicate prevention / idempotency

\*\*Fecha:\*\*  

\*\*Objetivo:\*\* Confirmar si existe forma segura de evitar duplicados.  

\*\*Prueba realizada:\*\*  

\*\*Campo usado como referencia:\*\*  

\*\*Resultado:\*\*  

\*\*Veredicto:\*\* supported / unsupported / inconclusive  

\*\*Notas:\*\*  



\---



\## Tabla consolidada de hallazgos

| Prueba | Resultado | Evidencia fuerte | Decisión |

|---|---|---|---|

| Auth smoke test |  |  |  |

| List stores |  |  |  |

| Find product by SKU |  |  |  |

| Read stock |  |  |  |

| Read Average Cost |  |  |  |

| Create Production |  |  |  |

| Production changes stock |  |  |  |

| Production changes Average Cost |  |  |  |

| Average Cost write-back |  |  |  |

| Duplicate prevention |  |  |  |



\---



\## Conclusión provisional

\### Estado actual

\- Create Production by API:

\- Read stock:

\- Read Average Cost:

\- Write Average Cost:

\- Duplicate prevention:



\### Recomendación

\- `open 6B minimal scaffolding`

\- `continue research`

\- `redesign integration approach`



\### Justificación

Escribir aquí la conclusión basada en la evidencia real.

