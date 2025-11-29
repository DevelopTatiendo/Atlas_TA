# MAPAS_TA_DEV_1 

🌐 Proyecto desarrollado por el Departamento de Desarrollo de T Atiendo (fundado 07/07/2025).

Este repositorio representa la base tecnológica del visor interactivo de operaciones logísticas, comerciales y de servicio de la compañía **T Atiendo S.A.** 

## 🎯 Objetivo del proyecto

Visualizar y analizar las operaciones de T Atiendo en múltiples ciudades, usando mapas interactivos. El sistema permite mostrar la distribución de pedidos, cartera vencida, visitas de cobro, muestreo y otros indicadores clave.

## 🧱 Estructura general del proyecto

- `/data`: Archivos de entrada (Excel).
- `/src`: Lógica de negocio (procesamiento de datos, generación de mapas).
- `/streamlit_app`: Interfaz de usuario (HTML + Streamlit).
- `/tests`: Scripts de validación.
- `/docs`: Documentación técnica y funcional.

## 🚧 Rama `pruebas_dev_dl_1`

Esta rama se ha creado para:
- Probar nuevas funcionalidades de trazado de rutas.
- Integrar indicadores como **entrega a tiempo** y **NPS por barrio**.
- Explorar mejoras en la correlación entre experiencia del cliente y cumplimiento logístico.

## 💡 Ideas en desarrollo

- Trazado de rutas con flechas por día y por asesor.
- Cálculo de % de entregas a tiempo por barrio.
- Mapa de calor del NPS por punto de entrega.
- Reporte de asignaciones ineficientes o zonas con alta carga operativa.
