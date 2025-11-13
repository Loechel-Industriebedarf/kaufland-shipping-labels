# Script zum Erstellen einer .exe-Datei aus getLabel.py
# Verwendet PyInstaller

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Kaufland Label Generator - EXE Builder" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Pruefe ob Python installiert ist
try {
    $pythonVersion = python --version 2>&1
    Write-Host "Python gefunden: $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "FEHLER: Python ist nicht installiert oder nicht im PATH!" -ForegroundColor Red
    Read-Host "Druecke Enter zum Beenden"
    exit 1
}

# Pruefe ob PyInstaller installiert ist
try {
    python -c "import PyInstaller" 2>&1 | Out-Null
    Write-Host "PyInstaller ist installiert" -ForegroundColor Green
} catch {
    Write-Host "PyInstaller wird installiert..." -ForegroundColor Yellow
    pip install pyinstaller
    if ($LASTEXITCODE -ne 0) {
        Write-Host "FEHLER: PyInstaller konnte nicht installiert werden!" -ForegroundColor Red
        Read-Host "Druecke Enter zum Beenden"
        exit 1
    }
}

# Pruefe ob getLabel.py existiert
if (-not (Test-Path "getLabel.py")) {
    Write-Host "FEHLER: getLabel.py wurde nicht gefunden!" -ForegroundColor Red
    Read-Host "Druecke Enter zum Beenden"
    exit 1
}

Write-Host ""
Write-Host "Erstelle .exe-Datei..." -ForegroundColor Yellow
Write-Host ""

# Erstelle die .exe mit PyInstaller
# --onefile: Eine einzelne .exe-Datei
# --console: Konsolenfenster anzeigen (fuer Argumente und Ausgaben)
# --name: Name der .exe-Datei
# --clean: Alte Build-Dateien loeschen
pyinstaller --onefile --console --clean --name getLabel getLabel.py

if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "FEHLER: Erstellung der .exe fehlgeschlagen!" -ForegroundColor Red
    Read-Host "Druecke Enter zum Beenden"
    exit 1
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host "Erfolgreich abgeschlossen!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "Die .exe-Datei befindet sich in: dist\getLabel.exe" -ForegroundColor Cyan
Write-Host ""
Write-Host "WICHTIG: Vergiss nicht, config.json in das gleiche Verzeichnis wie die .exe zu kopieren!" -ForegroundColor Yellow
Write-Host ""
Read-Host "Druecke Enter zum Beenden"

