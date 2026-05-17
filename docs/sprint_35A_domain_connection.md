# Sprint 35A — Domain Connection

Estado: Aprobado / Cerrado

## Dominio conectado

https://erp.greencornercr.com

## Servicio Render

erp-green-corner-staging

## Resultado

- Custom domain verificado
- Certificado HTTPS emitido
- DNS configurado en Shopify mediante CNAME:
  `erp -> erp-green-corner-staging.onrender.com`
- URL onrender.com se mantiene como fallback
- No se modificó base de datos
- No se corrieron migraciones
- No se tocó código operativo

## Pendiente posterior

- Definir política formal de backups
- Definir protocolo de cambios
- Definir usuarios reales
- Evaluar si renombrar el servicio de staging a production o dejarlo documentado como producción inicial
