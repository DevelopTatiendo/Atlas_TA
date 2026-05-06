# fix_entorno.ps1  —  Atlas TA
# Restaura versiones compatibles de numpy/pandas y verifica el entorno.
# Ejecutar en PowerShell desde la raíz del proyecto (con venv activo):
#   .\.venv\Scripts\Activate.ps1
#   .\fix_entorno.ps1

Write-Host "`n=== FIX ENTORNO ATLAS TA ===" -ForegroundColor Cyan

# 1. Versiones actuales
Write-Host "`n[1] Versiones actuales:" -ForegroundColor Yellow
python -c "import numpy, pandas, streamlit; print(f'  numpy   {numpy.__version__}'); print(f'  pandas  {pandas.__version__}'); print(f'  streamlit {streamlit.__version__}')"

# 2. Detectar incompatibilidad numpy 2.x + pandas <2
$numpy_major = python -c "import numpy; print(numpy.__version__.split('.')[0])"
$pandas_major = python -c "import pandas; print(pandas.__version__.split('.')[0])"

if ($numpy_major -ge 2 -and $pandas_major -lt 2) {
    Write-Host "`n[!] INCOMPATIBILIDAD: numpy $numpy_major.x + pandas $pandas_major.x" -ForegroundColor Red
    Write-Host "    Fijando numpy a 1.25.0 (requirements.txt)..." -ForegroundColor Yellow
    pip install "numpy==1.25.0" --force-reinstall --quiet
    Write-Host "    numpy fijado." -ForegroundColor Green
} else {
    Write-Host "`n[OK] numpy/pandas compatibles" -ForegroundColor Green
}

# 3. Verificar onnxruntime (puede haber actualizado numpy)
Write-Host "`n[2] Verificando onnxruntime..." -ForegroundColor Yellow
$ort_installed = python -c "import onnxruntime; print('ok')" 2>&1
if ($ort_installed -eq "ok") {
    Write-Host "    onnxruntime importable — OK" -ForegroundColor Green
} else {
    Write-Host "    onnxruntime NO importable (se usa _HashEmbeddingFunction, no afecta app principal)" -ForegroundColor DarkYellow
}

# 4. Limpiar cache Streamlit
Write-Host "`n[3] Limpiando cache Streamlit..." -ForegroundColor Yellow
$cache_dirs = @(
    "$env:USERPROFILE\.streamlit\cache",
    "$env:LOCALAPPDATA\streamlit\cache",
    ".streamlit_cache"
)
foreach ($d in $cache_dirs) {
    if (Test-Path $d) {
        Remove-Item -Recurse -Force $d
        Write-Host "    Eliminado: $d" -ForegroundColor Green
    }
}
python -m streamlit cache clear 2>$null
Write-Host "    Cache limpio." -ForegroundColor Green

# 5. Verificar versiones finales
Write-Host "`n[4] Versiones finales:" -ForegroundColor Yellow
python -c "import numpy, pandas; print(f'  numpy  {numpy.__version__}'); print(f'  pandas {pandas.__version__}')"

Write-Host "`n=== LISTO — correr: streamlit run app.py ===" -ForegroundColor Cyan
