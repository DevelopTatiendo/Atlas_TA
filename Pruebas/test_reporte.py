"""Test end-to-end: métricas → mapa → screenshot → Word."""
from config.secrets_manager import load_env_secure
load_env_secure(prefer_plain=True, enc_path="config/.env.enc",
                pass_env_var="MAPAS_SECRET_PASSPHRASE", cache=False)

from agente.reporte import generar_reporte_operacion

# Cambia ciudad y fechas según lo que tengas en BD
path = generar_reporte_operacion(
    ciudad="Medellín",
    fecha_inicio="2026-01-01",
    fecha_fin="2026-04-27",
)
print(f"\n✅ Reporte listo: {path}")
