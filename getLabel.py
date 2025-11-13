"""
Kaufland Versandlabel-Generator

Dieses Skript erstellt Versandlabels ueber die Kaufland Seller API.
Es liest Auftragsdaten aus einer MSSQL-Datenbank, erstellt Labels,
extrahiert Tracking-Nummern aus PDFs und speichert diese in der Datenbank.
"""

import argparse
import hashlib
import hmac
import json
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from time import time
from urllib.parse import urlsplit

import pyodbc
import requests
from PyPDF2 import PdfReader

# Konfiguration mit API-Keys und Pfaden aus config.json einlesen.
config_path = Path('config.json')
config = json.loads(config_path.read_text(encoding='utf-8'))

# Shop-Credentials fuer die Kaufland API.
shop_client_key = config['shop_client_key']
shop_secret_key = config['shop_secret_key']

# Pfad fuer gespeicherte Label-Dateien (wird automatisch erstellt falls nicht vorhanden).
label_path = Path(config.get('label_path', '.'))
label_path.mkdir(parents=True, exist_ok=True)

# Logging-System: Sammelt alle Ausgaben fuer spaetere Speicherung in MEMO-Feld und Log-Datei.
log_messages: list[str] = []
log_file_path = Path('log.log')


def log(message: str) -> None:
    """
    Zentrale Logging-Funktion: Schreibt Nachrichten in Konsole, Log-Datei und sammelt sie fuer MEMO-Feld.
    
    Args:
        message: Die zu loggende Nachricht
    """
    print(message)
    log_messages.append(message)
    # Zusaetzlich in die Log-Datei mit Timestamp schreiben.
    try:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(log_file_path, 'a', encoding='utf-8') as log_file:
            log_file.write(f"[{timestamp}] {message}\n")
    except Exception as exc:
        # Fehler beim Schreiben in die Log-Datei nicht abbrechen, nur in Konsole ausgeben.
        print(f"Warnung: Fehler beim Schreiben in log.log: {exc}")


# API-Konfiguration: Sandbox-Modus bestimmt, ob Test- oder Live-API verwendet wird.
sandbox_enabled = bool(config.get('sandbox', True))
test_uri = config['test_uri']
live_uri = config['live_uri']

# SQL-Datenbank-Konfiguration.
sql_config = config['sql']

# Kommandozeilen-Argumente einlesen: Auftragsnummer und Benutzername.
parser = argparse.ArgumentParser(description='Erzeugt Kaufland-Versandlabels.')
parser.add_argument('order_number', help='Auftragsnummer (Order Unit ID)')
parser.add_argument('username', help='Benutzername fuer die Dateibenennung')
args = parser.parse_args()

order_number_input = args.order_number.strip()
username_input = args.username.strip()

# API-Endpoint abhaengig von Sandbox-Konfiguration waehlen.
uri = test_uri if sandbox_enabled else live_uri


def sign_request(method: str, uri: str, body: str, timestamp: str, secret_key: str) -> str:
    """
    Berechnet die Kaufland API-Signatur via HMAC-SHA256.
    
    Die Signatur wird aus Methode, URI, Body und Timestamp erstellt.
    Dies ist erforderlich fuer die Authentifizierung bei der Kaufland API.
    
    Args:
        method: HTTP-Methode (z.B. 'POST')
        uri: Vollstaendige URI des Endpoints
        body: JSON-Body des Requests
        timestamp: Unix-Timestamp als String
        secret_key: Secret Key fuer HMAC
        
    Returns:
        Hexadezimale Signatur-String
    """
    message = "\n".join([method, uri, body, timestamp])
    return hmac.new(secret_key.encode('utf-8'), message.encode('utf-8'), hashlib.sha256).hexdigest()


def find_download_url(payload):
    """
    Durchsucht rekursiv verschachtelte JSON-Strukturen nach einem download_url-Feld.
    
    Die Kaufland API kann die download_url in verschiedenen Ebenen der Antwort platzieren.
    Diese Funktion findet sie unabhaengig von der Verschachtelungstiefe.
    
    Args:
        payload: JSON-Objekt (dict oder list) zum Durchsuchen
        
    Returns:
        Download-URL als String oder None wenn nicht gefunden
    """
    if isinstance(payload, dict):
        if payload.get('download_url'):
            return payload['download_url']
        for value in payload.values():
            result = find_download_url(value)
            if result:
                return result
    elif isinstance(payload, list):
        for item in payload:
            result = find_download_url(item)
            if result:
                return result
    return None


def sanitize_filename_segment(value: str) -> str:
    """
    Bereinigt einen String fuer die Verwendung in Dateinamen.
    Entfernt alle nicht-alphanumerischen Zeichen ausser Bindestrich und Unterstrich.
    
    Args:
        value: Zu bereinigender String
        
    Returns:
        Bereinigter String in Kleinbuchstaben oder 'unbekannt' wenn leer
    """
    cleaned = ''.join(ch for ch in value if ch.isalnum() or ch in ('-', '_')).strip('-_')
    return cleaned.lower() if cleaned else 'unbekannt'


def ensure_int(value, field_name: str) -> int:
    """
    Konvertiert einen Wert zu einem Integer mit Fehlerbehandlung.
    
    Args:
        value: Der zu konvertierende Wert
        field_name: Feldname fuer Fehlermeldungen
        
    Returns:
        Integer-Wert
        
    Raises:
        ValueError: Wenn Konvertierung fehlschlaegt
    """
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f'Ungueltiger numerischer Wert fuer {field_name}: {value}') from exc


def ensure_decimal(value, field_name: str) -> Decimal:
    """
    Konvertiert einen Wert zu einem Decimal mit Fehlerbehandlung.
    Wichtig fuer praezise Berechnungen bei Gewicht und Massen.
    
    Args:
        value: Der zu konvertierende Wert
        field_name: Feldname fuer Fehlermeldungen
        
    Returns:
        Decimal-Wert
        
    Raises:
        ValueError: Wenn Konvertierung fehlschlaegt
    """
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f'Ungueltiger Dezimalwert fuer {field_name}: {value}') from exc


def get_available_odbc_drivers() -> list[str]:
    """
    Gibt eine Liste aller auf dem System verfuegbaren ODBC-Treiber zurueck.
    
    Returns:
        Liste von Treibernamen
    """
    try:
        drivers = pyodbc.drivers()
        return drivers
    except Exception:
        return []


def build_sql_connection_string(sql_conf: dict) -> str:
    """
    Erstellt einen ODBC-Connection-String aus der Konfiguration.
    
    Args:
        sql_conf: SQL-Konfigurationsdictionary aus config.json
        
    Returns:
        Connection-String fuer pyodbc
    """
    parts = [
        f"DRIVER={sql_conf.get('driver', '{ODBC Driver 18 for SQL Server}')}",
        f"SERVER={sql_conf['server']}",
        f"DATABASE={sql_conf['database']}"
    ]
    if sql_conf.get('username'):
        parts.append(f"UID={sql_conf['username']}")
    if sql_conf.get('password'):
        parts.append(f"PWD={sql_conf['password']}")
    if 'encrypt' in sql_conf:
        parts.append(f"Encrypt={'yes' if sql_conf['encrypt'] else 'no'}")
    else:
        parts.append('Encrypt=yes')
    if sql_conf.get('trust_server_certificate'):
        parts.append('TrustServerCertificate=yes')
    if sql_conf.get('connection_options'):
        parts.append(sql_conf['connection_options'])
    return ';'.join(parts)


def fetch_order_rows(order_number: str) -> list[dict]:
    """
    Ruft Auftragsdaten aus der MSSQL-Datenbank ab.
    
    Args:
        order_number: Auftragsnummer (BELEGNR) fuer die SQL-Abfrage
        
    Returns:
        Liste von Dictionaries mit den Auftragsdaten (Spaltennamen in Kleinbuchstaben)
        
    Raises:
        pyodbc.Error: Bei Datenbankfehlern, mit verbesserter Fehlermeldung bei Treiberproblemen
    """
    connection_string = build_sql_connection_string(sql_config)
    query = sql_config['order_query']
    try:
        with pyodbc.connect(connection_string) as connection:
            cursor = connection.cursor()
            cursor.execute(query, order_number)
            rows = cursor.fetchall()
            if not rows:
                return []
            # Spaltennamen in Kleinbuchstaben konvertieren fuer einheitlichen Zugriff.
            columns = [col[0].lower() for col in cursor.description]
            result = []
            for row in rows:
                row_dict = {columns[idx]: row[idx] for idx in range(len(columns))}
                result.append(row_dict)
            return result
    except pyodbc.Error as exc:
        # Pruefe, ob es ein Treiber-Problem ist (IM002 = Treiber nicht gefunden).
        if exc.args and len(exc.args) > 0 and 'IM002' in str(exc.args[0]):
            configured_driver = sql_config.get('driver', '{ODBC Driver 18 for SQL Server}')
            available_drivers = get_available_odbc_drivers()
            error_msg = (
                f"ODBC-Treiber nicht gefunden!\n"
                f"Konfigurierter Treiber: {configured_driver}\n"
                f"Verfuegbare Treiber auf diesem System:\n"
            )
            if available_drivers:
                for driver in available_drivers:
                    error_msg += f"  - {driver}\n"
                error_msg += (
                    f"\nBitte installieren Sie den Treiber '{configured_driver}' oder "
                    f"passen Sie die config.json an, um einen der verfuegbaren Treiber zu verwenden."
                )
            else:
                error_msg += "  (Keine Treiber gefunden)\n"
                error_msg += (
                    f"\nBitte installieren Sie einen ODBC-Treiber fuer SQL Server "
                    f"(z.B. 'ODBC Driver 18 for SQL Server' oder 'ODBC Driver 17 for SQL Server')."
                )
            raise pyodbc.Error(error_msg) from exc
        raise


def insert_additional_field_value(fsrowid: str, value_string: str) -> None:
    """
    Fuegt die Tracking-Nummer in die AdditionalFieldValue-Tabelle ein.
    
    Args:
        fsrowid: FSROWID des Auftrags (TableRowID)
        value_string: Tracking-Nummer (12-stellige Nummer aus PDF)
    """
    connection_string = build_sql_connection_string(sql_config)
    insert_query = sql_config['insert_tracking_query']
    with pyodbc.connect(connection_string) as connection:
        cursor = connection.cursor()
        cursor.execute(insert_query, fsrowid, value_string)
        connection.commit()


def insert_carrier_field_value(fsrowid: str) -> None:
    """
    Fuegt den Carrier-Wert (GLS) in die AdditionalFieldValue-Tabelle ein.
    
    Args:
        fsrowid: FSROWID des Auftrags (TableRowID)
    """
    connection_string = build_sql_connection_string(sql_config)
    insert_query = sql_config['insert_carrier_query']
    with pyodbc.connect(connection_string) as connection:
        cursor = connection.cursor()
        cursor.execute(insert_query, fsrowid)
        connection.commit()


def update_order_memo(order_number: str, memo_text: str) -> None:
    """
    Schreibt alle gesammelten Log-Meldungen in das MEMO-Feld des Auftrags.
    Dies ermoeglicht die Nachverfolgung des Label-Erstellungsprozesses.
    
    Args:
        order_number: BELEGNR des Auftrags
        memo_text: Alle Log-Meldungen als mehrzeiliger Text
    """
    connection_string = build_sql_connection_string(sql_config)
    update_query = sql_config['update_memo_query']
    with pyodbc.connect(connection_string) as connection:
        cursor = connection.cursor()
        cursor.execute(update_query, memo_text, order_number)
        connection.commit()


def aggregate_order_values(rows: list[dict], fallback_order_number: str) -> dict:
    """
    Aggregiert Auftragsdaten aus mehreren Zeilen:
    - Summiert Gewicht und Masse ueber alle Zeilen
    - Sammelt alle CODE1-Werte fuer ids_order_units
    - Wendet Multiplikatoren an (Gewicht * 1000, Masse * 10)
    - Ersetzt NULL-Werte durch Standardwert 10
    
    Args:
        rows: Liste von Datensaetzen aus der SQL-Abfrage
        fallback_order_number: Fallback-Auftragsnummer (wird nicht verwendet)
        
    Returns:
        Dictionary mit aggregierten Werten
        
    Raises:
        ValueError: Wenn keine Daten oder CODE1-Werte vorhanden sind
    """
    if not rows:
        raise ValueError('Keine Auftragsdaten vorhanden')

    def sum_field(field_names: tuple[str, ...], field_label: str, multiplier: int = 1) -> int:
        """
        Summiert ein Feld ueber alle Zeilen und wendet Multiplikator an.
        Verwendet Decimal fuer praezise Berechnungen.
        """
        total = Decimal('0')
        for row in rows:
            value = None
            # Suche nach Feld in verschiedenen moeglichen Spaltennamen.
            for key in field_names:
                if key in row:
                    value = row[key]
                    break
            # NULL-Werte werden durch Standardwert 10 ersetzt.
            if value is None:
                value = 10
            try:
                decimal_value = ensure_decimal(value, field_label)
            except ValueError as exc:
                raise
            total += decimal_value
        # Multiplikator anwenden und auf Integer runden.
        scaled = total * Decimal(multiplier)
        return int(scaled.to_integral_value(rounding=ROUND_HALF_UP))

    # Sammle alle CODE1-Werte (Order Unit IDs) aus allen Zeilen.
    code_values = []
    for row in rows:
        if 'code1' in row and row['code1'] is not None:
            code_value = str(row['code1']).strip()
            if code_value:
                code_values.append(code_value)
    if not code_values:
        raise ValueError('Keine CODE1-Werte in den Auftragsdaten gefunden')

    return {
        'ids_order_units': code_values,  # Alle CODE1-Werte als Liste.
        'weight_gram': sum_field(('weight_gram', 'bgewicht'), 'weight_gram', multiplier=1000),  # kg -> g
        'width_cm': sum_field(('width_cm', 'bbreite'), 'width_cm', multiplier=10),  # dm -> cm
        'height_cm': sum_field(('height_cm', 'bhoehe'), 'height_cm', multiplier=10),  # dm -> cm
        'length_cm': sum_field(('length_cm', 'btiefe'), 'length_cm', multiplier=10),  # dm -> cm
    }


# Hauptablauf: Auftragsdaten verarbeiten und Label erstellen.
order_number_db = None

try:
    # Schritt 1: Auftragsdaten aus der Datenbank abrufen.
    try:
        order_rows = fetch_order_rows(order_number_input)
    except pyodbc.Error as exc:
        log(f"SQL-Fehler beim Abrufen der Auftragsdaten: {exc}")
        raise SystemExit(1)

    if not order_rows:
        log(f"Keine Daten fuer Auftragsnummer {order_number_input} gefunden.")
        raise SystemExit(1)

    # Schritt 2: Daten aggregieren (Summierung, CODE1-Werte sammeln, Multiplikatoren anwenden).
    try:
        aggregated = aggregate_order_values(order_rows, order_number_input)
    except ValueError as exc:
        log(str(exc))
        raise SystemExit(1)

    # Schritt 3: BELEGNR aus den Datenbankdaten extrahieren (fuer MEMO-Update).
    order_number_db = next(
        (
            str(row['belegnr']).strip()
            for row in order_rows
            if 'belegnr' in row and row['belegnr'] is not None and str(row['belegnr']).strip()
        ),
        order_number_input,
    )

    # Schritt 4: FSROWID extrahieren (wird fuer Datenbank-Inserts benoetigt).
    fsrowid = None
    for row in order_rows:
        if 'fsrowid' in row and row['fsrowid'] is not None:
            fsrowid = str(row['fsrowid']).strip()
            break

    if not fsrowid:
        log('Warnung: Keine FSROWID in den Auftragsdaten gefunden.')

    # Schritt 5: Aggregierte Werte fuer API-Request vorbereiten.
    ids_order_units = aggregated['ids_order_units']
    weight_gram = aggregated['weight_gram']
    width_cm = aggregated['width_cm']
    height_cm = aggregated['height_cm']
    length_cm = aggregated['length_cm']

    # Schritt 6: JSON-Body fuer Kaufland API erstellen.
    json_data = {
        'ids_order_units': ids_order_units,  # Alle CODE1-Werte als Liste.
        'carriers': ['GLS'],  # Versanddienstleister.
        'package_measurements': {
            'weight_gram': weight_gram,
            'width_cm': width_cm,
            'height_cm': height_cm,
            'length_cm': length_cm,
        },
    }

    # Schritt 7: Request signieren (HMAC-SHA256 mit Timestamp).
    json_body = json.dumps(json_data, separators=(',', ':'), ensure_ascii=False)  # Kompakte JSON-Serialisierung.
    shop_timestamp = str(int(time()))  # Aktueller Unix-Timestamp.
    shop_signature = sign_request('POST', uri, json_body, shop_timestamp, shop_secret_key)

    # Schritt 8: Dateiname fuer gespeichertes Label generieren (Format: ddMMyyyy-HHmmss-username-GLS-auftragsnummer).
    timestamp_for_filename = datetime.now().strftime('%d%m%Y-%H%M%S')
    username_for_filename = sanitize_filename_segment(username_input)
    order_identifier_raw = ids_order_units[0] if ids_order_units else order_number_input
    order_for_filename = sanitize_filename_segment(str(order_identifier_raw))
    filename_base = f"{timestamp_for_filename}-{username_for_filename}-GLS-{order_for_filename}"

    # Schritt 9: HTTP-Header fuer API-Request zusammenstellen.
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json',
        'shop-client-key': shop_client_key,
        'shop-signature': shop_signature,
        'shop-timestamp': shop_timestamp,
    }

    # Schritt 10: API-Request an Kaufland senden.
    response = requests.post(uri, headers=headers, data=json_body)

    try:
        response.raise_for_status()
        response_json = response.json()
    except (requests.RequestException, ValueError) as exc:
        log(f"Fehler bei der Anfrage oder beim Parsen der Antwort: {exc}")
        log(response.text)
        raise SystemExit(1)

    # Schritt 11: Download-URL aus der API-Antwort extrahieren.
    download_url = find_download_url(response_json)
    if not download_url:
        log('Keine Download-URL in der Antwort gefunden:')
        log(response.text)
        raise SystemExit(1)

    log(f"Download-URL: {download_url}")
    
    # Schritt 12: Label-PDF von der Download-URL herunterladen.
    try:
        download_response = requests.get(download_url, timeout=30)
        download_response.raise_for_status()
    except requests.RequestException as exc:
        log(f"Download fehlgeschlagen: {exc}")
        log('Verwendete Request-Daten:')
        log(json.dumps(json_data, indent=2, ensure_ascii=False))
        raise SystemExit(1)

    # Schritt 13: Heruntergeladenes PDF lokal speichern.
    original_path = Path(urlsplit(download_url).path)
    suffix = original_path.suffix or ''
    target_path = label_path / f"{filename_base}{suffix}"
    target_path.write_bytes(download_response.content)
    log(f"Dokument gespeichert als {target_path}")

    # Schritt 14: PDF auslesen und 12-stellige Tracking-Nummer extrahieren.
    if suffix.lower() == '.pdf' or target_path.suffix.lower() == '.pdf':
        try:
            pdf_reader = PdfReader(str(target_path))
            pdf_text = ''.join((page.extract_text() or '') for page in pdf_reader.pages)
            match = re.search(r'\b\d{12}\b', pdf_text)  # Suche nach 12-stelliger Zahl.
        except Exception as exc:
            log(f"Fehler beim Auslesen der PDF: {exc}")
            raise SystemExit(1)

        if not match:
            log('Keine 12-stellige Nummer in der PDF gefunden.')
            raise SystemExit(1)

        tracking_number = match.group(0)
        log(f"Gefundene 12-stellige Nummer: {tracking_number}")

        # Schritt 15: Tracking-Nummer und Carrier-Wert in die Datenbank schreiben.
        if fsrowid:
            try:
                insert_additional_field_value(fsrowid, tracking_number)
                log(f"Tracking-Nummer {tracking_number} erfolgreich in die Datenbank geschrieben.")
                insert_carrier_field_value(fsrowid)
                log('Carrier-Wert (GLS) erfolgreich in die Datenbank geschrieben.')
            except pyodbc.Error as exc:
                log(f"Fehler beim Schreiben in die Datenbank: {exc}")
                raise SystemExit(1)
        else:
            log('Warnung: Keine FSROWID verfuegbar, Datenbankeintraege uebersprungen.')
            raise SystemExit(1)

except SystemExit:
    # SystemExit wird ignoriert, damit das Skript sauber beendet wird.
    pass
except Exception as exc:
    # Unerwartete Fehler loggen und weiterwerfen.
    log(f"Unerwarteter Fehler: {exc}")
    raise
finally:
    # Schritt 16: Alle Log-Meldungen in das MEMO-Feld der Datenbank schreiben.
    # Dies wird immer ausgefuehrt, auch bei Fehlern, um den Ablauf nachvollziehbar zu machen.
    if order_number_db:
        memo_text = "\n".join(log_messages)
        try:
            update_order_memo(order_number_db, memo_text)
        except pyodbc.Error as exc:
            log(f"Fehler beim Aktualisieren des Memos: {exc}")
