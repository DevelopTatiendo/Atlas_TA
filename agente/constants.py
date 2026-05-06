"""Constantes canónicas de Centros de Operación para Atlas TA.

Fuente única de verdad para IDs de ciudad/CO.
Todos los módulos deben importar desde aquí en lugar de hardcodear valores.
"""

from __future__ import annotations

# Mapeo nombre → id_centroope (acepta variantes sin tilde)
CO_IDS: dict[str, int] = {
    "cali":          2,
    "medellin":      3,
    "medellín":      3,
    "bogota":        4,
    "bogotá":        4,
    "pereira":       5,
    "manizales":     6,
    "bucaramanga":   7,
    "barranquilla":  8,
}

# Mapeo id_centroope → nombre canónico con tilde
CO_NOMBRES: dict[int, str] = {
    2: "Cali",
    3: "Medellín",
    4: "Bogotá",
    5: "Pereira",
    6: "Manizales",
    7: "Bucaramanga",
    8: "Barranquilla",
}

# Centro geográfico de cada CO para centrar mapas Folium (lat, lon)
CENTROS_MAPA: dict[int, tuple[float, float]] = {
    2: (3.4516,  -76.5320),
    3: (6.2442,  -75.5812),
    4: (4.7110,  -74.0721),
    5: (4.8133,  -75.6961),
    6: (5.0703,  -75.5138),
    7: (7.1193,  -73.1227),
    8: (10.9685, -74.7813),
}

# Label compacto para usar en descripciones de tools y prompts
CIUDADES_LABEL = (
    "Cali=2, Medellín=3, Bogotá=4, Pereira=5, "
    "Manizales=6, Bucaramanga=7, Barranquilla=8"
)


def resolver_ciudad(valor: str | int) -> int | None:
    """Resuelve un nombre de ciudad o ID a su id_centroope.

    Acepta: int directo, string numérico, nombre con/sin tilde.
    Devuelve None si no reconoce el valor.
    """
    if isinstance(valor, int):
        return valor if valor in CO_NOMBRES else None
    try:
        int_val = int(valor)
        return int_val if int_val in CO_NOMBRES else None
    except (ValueError, TypeError):
        pass
    return CO_IDS.get(str(valor).lower().strip())
