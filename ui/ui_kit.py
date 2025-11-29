"""
UI Kit para la aplicación de mapas - Sistema de diseño consistente
"""

def fmt_es(value: float, dec: int = 0, miles: bool = True) -> str:
    """
    Formateador numérico estilo es-CO.
    
    Args:
        value: Valor numérico a formatear
        dec: Número de decimales
        miles: Si True, agrupa miles con punto
        
    Returns:
        str: Número formateado (ejemplo: 838.039 o 8,75)
    """
    if value is None or (isinstance(value, float) and not (value == value)):  # NaN check
        return "—"
    
    formatted = f"{value:.{dec}f}"
    
    # Separar parte entera y decimal
    if '.' in formatted:
        parte_entera, parte_decimal = formatted.split('.')
    else:
        parte_entera, parte_decimal = formatted, ""
    
    # Aplicar separador de miles si está habilitado
    if miles and len(parte_entera) > 3:
        # Agrupar de derecha a izquierda cada 3 dígitos
        grupos = []
        for i in range(len(parte_entera), 0, -3):
            start = max(0, i-3)
            grupos.append(parte_entera[start:i])
        parte_entera = '.'.join(reversed(grupos))
    
    # Combinar con coma decimal si hay parte decimal
    if parte_decimal:
        return f"{parte_entera},{parte_decimal}"
    else:
        return parte_entera

def render_kpi(label: str, value: str, help: str = None) -> str:
    """
    Genera una tarjeta KPI con el estilo del sistema de diseño.
    
    Args:
        label: Etiqueta del KPI
        value: Valor principal (ya formateado)
        help: Texto de ayuda opcional
        
    Returns:
        str: HTML de la tarjeta KPI
    """
    help_html = f'<div class="kpi-help">{help}</div>' if help else ""
    
    return f"""
    <div class="kpi">
        <div class="kpi-value">{value}</div>
        <div class="kpi-label">{label}</div>
        {help_html}
    </div>
    """

def render_card(title: str, body_md: str, class_name: str = "") -> str:
    """
    Genera una tarjeta con título y contenido.
    
    Args:
        title: Título de la tarjeta
        body_md: Contenido en markdown/HTML
        class_name: Clases CSS adicionales
        
    Returns:
        str: HTML de la tarjeta
    """
    classes = f"card {class_name}".strip()
    
    return f"""
    <div class="{classes}">
        <div class="card-title">{title}</div>
        <div class="card-body">{body_md}</div>
    </div>
    """

def render_chip(text: str, variant: str = "default") -> str:
    """
    Genera un chip/pill con texto.
    
    Args:
        text: Texto del chip
        variant: Variante de estilo (default, primary, success)
        
    Returns:
        str: HTML del chip
    """
    return f'<span class="chip chip-{variant}">{text}</span>'

def get_global_styles() -> str:
    """
    Retorna los estilos CSS globales del sistema de diseño.
    """
    return """
    <style>
    /* === TOKENS DE DISEÑO === */
    :root {
        --color-primary: #2563EB;
        --color-primary-light: #3B82F6;
        --color-accent: #0EA5E9;
        --color-text: #111827;
        --color-text-secondary: #6B7280;
        --color-background: #F9FAFB;
        --color-surface: #FFFFFF;
        --color-border: #E5E7EB;
        --color-success: #10B981;
        --color-warning: #F59E0B;
        
        --spacing-xs: 4px;
        --spacing-sm: 8px;
        --spacing-md: 12px;
        --spacing-lg: 16px;
        --spacing-xl: 24px;
        
        --radius: 12px;
        --shadow: 0 6px 18px rgba(0, 0, 0, 0.06);
        --shadow-hover: 0 8px 24px rgba(0, 0, 0, 0.12);
    }
    
    /* Dark mode support */
    @media (prefers-color-scheme: dark) {
        :root {
            --color-text: #F9FAFB;
            --color-text-secondary: #9CA3AF;
            --color-background: #111827;
            --color-surface: #1F2937;
            --color-border: #374151;
        }
    }
    
    /* === COMPONENTES GLOBALES === */
    
    /* Tarjetas */
    .card {
        background: var(--color-surface);
        border: 1px solid var(--color-border);
        border-radius: var(--radius);
        padding: var(--spacing-xl);
        box-shadow: var(--shadow);
        margin-bottom: var(--spacing-lg);
        transition: all 0.2s ease;
    }
    
    .card:hover {
        box-shadow: var(--shadow-hover);
        transform: translateY(-2px);
    }
    
    .card-title {
        font-size: 18px;
        font-weight: 600;
        color: var(--color-text);
        margin-bottom: var(--spacing-md);
        line-height: 1.4;
    }
    
    .card-body {
        color: var(--color-text-secondary);
        line-height: 1.6;
    }
    
    /* KPIs */
    .kpi {
        background: var(--color-surface);
        border: 1px solid var(--color-border);
        border-radius: var(--radius);
        padding: var(--spacing-lg);
        text-align: center;
        box-shadow: var(--shadow);
        transition: all 0.2s ease;
        height: 100%;
        display: flex;
        flex-direction: column;
        justify-content: center;
    }
    
    .kpi:hover {
        transform: translateY(-2px);
        box-shadow: var(--shadow-hover);
    }
    
    .kpi-value {
        font-size: 32px;
        font-weight: 700;
        color: var(--color-primary);
        margin-bottom: var(--spacing-xs);
        line-height: 1.2;
    }
    
    .kpi-label {
        font-size: 14px;
        font-weight: 500;
        color: var(--color-text-secondary);
        margin-bottom: var(--spacing-xs);
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    .kpi-help {
        font-size: 12px;
        color: var(--color-text-secondary);
        opacity: 0.8;
        font-style: italic;
    }
    
    /* Chips/Pills */
    .chip {
        display: inline-block;
        padding: var(--spacing-xs) var(--spacing-md);
        border-radius: 20px;
        font-size: 13px;
        font-weight: 500;
        line-height: 1.4;
    }
    
    .chip-default {
        background: var(--color-border);
        color: var(--color-text-secondary);
    }
    
    .chip-primary {
        background: var(--color-primary);
        color: white;
    }
    
    .chip-success {
        background: var(--color-success);
        color: white;
    }
    
    /* Header y títulos mejorados */
    .header-main {
        margin-bottom: var(--spacing-xl);
        text-align: center;
    }
    
    .header-title {
        font-size: 42px;
        font-weight: 700;
        color: var(--color-text);
        margin-bottom: var(--spacing-sm);
        line-height: 1.2;
    }
    
    .header-subtitle {
        margin-bottom: var(--spacing-lg);
    }
    
    .breadcrumb {
        font-size: 14px;
        color: var(--color-text-secondary);
        margin-bottom: var(--spacing-sm);
        font-weight: 500;
    }
    
    /* Botones mejorados */
    .stButton > button {
        border-radius: var(--radius) !important;
        font-weight: 600 !important;
        transition: all 0.2s ease !important;
        border: none !important;
        box-shadow: var(--shadow) !important;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px) !important;
        box-shadow: var(--shadow-hover) !important;
    }
    
    .stButton > button[kind="primary"] {
        background: linear-gradient(135deg, var(--color-primary) 0%, var(--color-primary-light) 100%) !important;
    }
    
    /* Dividers sutiles */
    .divider {
        height: 1px;
        background: linear-gradient(90deg, transparent 0%, var(--color-border) 50%, transparent 100%);
        margin: var(--spacing-xl) 0;
    }
    
    /* Sidebar mejorado */
    .css-1d391kg {
        background: var(--color-surface) !important;
        border-right: 1px solid var(--color-border) !important;
    }
    
    /* Main content spacing */
    .main .block-container {
        padding-top: var(--spacing-xl) !important;
        max-width: 1200px !important;
    }
    
    /* Download section */
    .download-section {
        margin-top: var(--spacing-xl);
    }
    
    .download-card {
        text-align: center;
    }
    
    .download-card .card-body {
        display: flex;
        flex-direction: column;
        gap: var(--spacing-md);
        align-items: center;
    }
    
    /* Toast improvements */
    .stAlert {
        border-radius: var(--radius) !important;
        border: none !important;
        box-shadow: var(--shadow) !important;
    }
    </style>
    """