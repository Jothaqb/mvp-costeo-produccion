# Estado de Resultados gerencial — Fase 1

## Alcance

Esta fase incorpora un Estado de Resultados mensual abierto y recalculable. No es un libro mayor, no implementa partida doble y no congela períodos.

Incluye:

- catálogo inicial de cuentas contables;
- presupuesto mensual promedio por cuenta o subcuenta, aplicable por igual a cualquier mes;
- carga manual de gastos e impuestos reales por mes y cuenta o subcuenta;
- comparación Real vs Presupuesto;
- ingresos y COGS calculados en vivo desde ventas facturadas del ERP.

## Fuentes de ventas

Solo se incluyen órdenes con estado `invoiced`.

- B2B usa, en orden, `invoice_date`, `invoiced_at.date()`, `delivery_date` para importación histórica y `created_at.date()` como fallback legacy.
- B2C usa `order_date`.
- Los ingresos B2B suman `B2BSalesOrderLine.line_total`.
- Los ingresos B2C suman `net_line_total_snapshot`, con `line_total` como fallback.
- COGS suma exclusivamente `cost_total_snapshot` de las líneas.

El módulo no consulta costo estándar actual, Kardex, `InventoryTransaction` ni `InventoryBalance`.

## COGS incompleto

Si al menos una línea facturada del período carece de `cost_total_snapshot`, se muestra la cobertura incompleta y no se calculan COGS, utilidad bruta, utilidad antes de impuestos ni resultado del período. Un costo ausente nunca se trata como cero.

## Nomenclatura funcional

- **Grupo:** sección principal del Estado de Resultados, como Gastos Operativos o Gastos Administrativos.
- **Cuenta:** línea contable principal dentro de un grupo.
- **Subcuenta:** detalle opcional dentro de una cuenta.

Los nombres técnicos internos y las tablas existentes, como `accounting_rubrics` y `accounting_subrubrics`, se conservan para evitar migraciones de riesgo.

## Comparación presupuestaria

Por cuenta, subcuenta y grupo:

```text
Diferencia ₡ = Real - Presupuesto
Diferencia % = (Real - Presupuesto) / Presupuesto × 100
Cumplimiento = Real / Presupuesto × 100
```

Cuando el Presupuesto es cero, Diferencia % y Cumplimiento muestran `N/A`.

## Pantallas

- `/accounting/income-statement`: Estado de Resultados mensual.
- `/accounting/monthly-actuals`: carga manual mensual.
- `/accounting/rubrics`: catálogo y mantenimiento de cuentas, subcuentas y presupuestos.

## Permisos

- `accounting.view`
- `accounting.edit`
- `accounting.manage_budget`

Los permisos se asignan automáticamente al rol administrador. Otros roles deben configurarse explícitamente desde la administración de roles.

## Fuera de alcance

- cierre o congelamiento mensual;
- reaperturas y revisiones;
- presupuesto específico por mes;
- PDF;
- libro diario, doble partida y balance general;
- integración con inventario, producción o Loyverse.
