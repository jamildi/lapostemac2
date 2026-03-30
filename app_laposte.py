import os
import re
import sys
import time
import queue
import asyncio
import shutil
import subprocess
import threading
import platform
import unicodedata
import webbrowser
from pathlib import Path
from datetime import datetime

import pandas as pd
from flask import Flask, request, redirect, url_for, render_template_string, jsonify
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError


# ──────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────
SYSTEME = platform.system()
HOME = Path.home()
APP_NAME = "La Poste Bot"

# Détecte si on est dans un exe PyInstaller
if getattr(sys, 'frozen', False):
    APP_DIR = Path(sys.executable).parent
else:
    APP_DIR = Path(__file__).parent

if getattr(sys, 'frozen', False) and SYSTEME == "Darwin":
    DATA_DIR = HOME / "Library" / "Application Support" / APP_NAME
    BUNDLED_BROWSERS_ARCHIVE = APP_DIR / "playwright_browsers.zip"
else:
    DATA_DIR = APP_DIR
    BUNDLED_BROWSERS_ARCHIVE = None

PROFIL = DATA_DIR / "profil_laposte_web"

# Navigateurs Playwright stockés à côté de l'app (portable)
BROWSERS_DIR = DATA_DIR / "playwright_browsers"
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(BROWSERS_DIR)

EMAIL_PAR_DEFAUT = "siwaksmile@gmail.com"
MOT_DE_PASSE_PAR_DEFAUT = "Marseille28***"
POIDS_PAR_DEFAUT = "0.25"
PAYS_ORIGINE_DOUANE_PAR_DEFAUT = "FR"

HOST = "127.0.0.1"
PORT = 5000

UPLOAD_DIR = DATA_DIR / "uploads"
SCREENSHOTS_DIR = DATA_DIR / "screenshots_erreurs"
DATA_DIR.mkdir(parents=True, exist_ok=True)
UPLOAD_DIR.mkdir(exist_ok=True)
PROFIL.mkdir(exist_ok=True)
SCREENSHOTS_DIR.mkdir(exist_ok=True)
BROWSERS_DIR.mkdir(exist_ok=True)

VERSION = "2.4.0"

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 20 * 1024 * 1024  # 20 MB

for _stream in (sys.stdout, sys.stderr):
    if hasattr(_stream, "reconfigure"):
        try:
            _stream.reconfigure(errors="replace")
        except Exception:
            pass


# ──────────────────────────────────────────────────────
# AUTO-INSTALLATION NAVIGATEUR
# ──────────────────────────────────────────────────────
def navigateur_installe() -> bool:
    """Vérifie si Chromium est déjà installé localement."""
    if not BROWSERS_DIR.exists():
        return False
    return any(BROWSERS_DIR.glob("chromium-*"))


def extraire_navigateurs_embarques() -> bool:
    """Extrait Chromium embarque depuis une archive incluse dans l'app."""
    if not BUNDLED_BROWSERS_ARCHIVE or not BUNDLED_BROWSERS_ARCHIVE.exists():
        return False

    try:
        print("  Extraction de Chromium embarque...", flush=True)
        shutil.unpack_archive(str(BUNDLED_BROWSERS_ARCHIVE), extract_dir=str(DATA_DIR))
        return navigateur_installe()
    except Exception as e:
        print(f"  Erreur extraction Chromium embarque : {e}", flush=True)
        return False


def installer_navigateur():
    """Installe Chromium automatiquement au premier lancement."""
    if navigateur_installe():
        return True

    if extraire_navigateurs_embarques():
        print("  Chromium embarque extrait avec succes !", flush=True)
        return True

    print("=" * 50, flush=True)
    print("  PREMIER LANCEMENT : Installation de Chromium", flush=True)
    print("  Cela peut prendre 1-2 minutes...", flush=True)
    print("=" * 50, flush=True)

    try:
        # Utiliser le driver Playwright directement (marche dans PyInstaller)
        from playwright._impl._driver import compute_driver_executable
        driver = compute_driver_executable()

        if isinstance(driver, tuple):
            # (node_exe, cli_js)
            cmd = [str(driver[0]), str(driver[1]), "install", "chromium"]
        else:
            cmd = [str(driver), "install", "chromium"]

        env = os.environ.copy()
        env["PLAYWRIGHT_BROWSERS_PATH"] = str(BROWSERS_DIR)

        result = subprocess.run(
            cmd, env=env, capture_output=True, text=True, timeout=300
        )

        if result.returncode == 0:
            print("  Chromium installe avec succes !", flush=True)
            return True
        else:
            print(f"  Erreur installation : {result.stderr[:300]}", flush=True)
            # Fallback : essayer via python -m playwright
            result2 = subprocess.run(
                [sys.executable, "-m", "playwright", "install", "chromium"],
                env=env, capture_output=True, text=True, timeout=300
            )
            if result2.returncode == 0:
                print("  Chromium installe (fallback) !", flush=True)
                return True
            print(f"  Echec fallback : {result2.stderr[:300]}", flush=True)
            return False

    except Exception as e:
        print(f"  Erreur : {e}", flush=True)
        return False


async def browser_smoke_test():
    """Valide que Playwright et Chromium fonctionnent reellement."""
    if not navigateur_installe() and not installer_navigateur():
        raise RuntimeError(f"Chromium introuvable dans {BROWSERS_DIR}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            page = await browser.new_page()
            await page.goto("data:text/html,<html><title>OK</title><body>OK</body></html>")
            title = await page.title()
            if title != "OK":
                raise RuntimeError(f"Titre inattendu: {title!r}")
        finally:
            await browser.close()

    print(f"BROWSER_SMOKE_OK version={VERSION}", flush=True)
    print(f"BROWSERS_DIR={BROWSERS_DIR}", flush=True)


# ──────────────────────────────────────────────────────
# ETAT GLOBAL
# ──────────────────────────────────────────────────────
log_queue = queue.Queue()
worker_thread = None
worker_running = False
stop_requested = False
last_uploaded_file = None


def log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    full = f"[{ts}] {msg}"
    try:
        print(full, flush=True)
    except UnicodeEncodeError:
        encoding = getattr(sys.stdout, "encoding", None) or "utf-8"
        safe = full.encode(encoding, errors="replace").decode(encoding, errors="replace")
        print(safe, flush=True)
    log_queue.put(full)


def afficher_alerte(message: str, titre: str = APP_NAME):
    """Affiche une alerte native quand l'app tourne sans terminal."""
    texte = str(message or "").replace('"', '\\"')
    titre = str(titre or APP_NAME).replace('"', '\\"')

    if SYSTEME == "Darwin":
        try:
            subprocess.run(
                [
                    "/usr/bin/osascript",
                    "-e",
                    f'display alert "{titre}" message "{texte}" as critical',
                ],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return
        except Exception:
            pass

    try:
        print(f"{titre}: {message}", flush=True)
    except Exception:
        pass


# ──────────────────────────────────────────────────────
# HTML
# ──────────────────────────────────────────────────────
PAGE_HTML = """
<!doctype html>
<html lang="fr">
<head>
    <meta charset="utf-8">
    <title>La Poste Bot v{{ version }}</title>
    <style>
        body { font-family: Arial, sans-serif; max-width: 900px; margin: 30px auto; padding: 0 20px; background: #f7f7f7; }
        .card { background: white; border-radius: 14px; padding: 22px; box-shadow: 0 4px 20px rgba(0,0,0,0.08); margin-bottom: 20px; }
        h1 { margin-top: 0; }
        label { display: block; margin-top: 14px; font-weight: bold; }
        input[type=text], input[type=password], input[type=file] { width: 100%; padding: 10px; margin-top: 6px; border: 1px solid #ccc; border-radius: 8px; box-sizing: border-box; }
        .btn { margin-top: 18px; background: #1f6feb; color: white; border: none; padding: 12px 18px; border-radius: 8px; cursor: pointer; font-size: 15px; display: inline-block; }
        .btn:disabled { background: #9bbcf2; cursor: not-allowed; }
        .btn-stop { background: #d32f2f; margin-left: 10px; }
        .btn-stop:hover { background: #b71c1c; }
        .small { color: #555; font-size: 14px; }
        .ok { color: green; font-weight: bold; }
        .warn { color: #b06a00; font-weight: bold; }
        .err { color: #b00020; font-weight: bold; }
        .version { float: right; color: #999; font-size: 12px; }
        #logs { background: #111; color: #d7ffd7; padding: 14px; border-radius: 10px; min-height: 280px; white-space: pre-wrap; font-family: Consolas, monospace; overflow-y: auto; max-height: 500px; font-size: 13px; }
        .row { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
        .actions { display: flex; align-items: center; gap: 10px; margin-top: 18px; }
    </style>
</head>
<body>
    <div class="card">
        <span class="version">v{{ version }}</span>
        <h1>La Poste Bot</h1>
        <div class="small">Importe ton fichier, puis clique sur <b>Lancer le bot</b>.</div>
        <form method="post" action="/upload" enctype="multipart/form-data">
            <label>Fichier CSV ou Excel</label>
            <input type="file" name="file" accept=".csv,.xlsx,.xls" required>
            <button type="submit" class="btn">Importer le fichier</button>
        </form>
        {% if fichier %}
            <p class="ok">Fichier actuel : {{ fichier }}</p>
        {% else %}
            <p class="warn">Aucun fichier importé.</p>
        {% endif %}
    </div>

    <div class="card">
        <form method="post" action="/start">
            <div class="row">
                <div>
                    <label>Email La Poste</label>
                    <input type="text" name="email" value="{{ email }}">
                </div>
                <div>
                    <label>Mot de passe</label>
                    <input type="password" name="mot_de_passe" value="{{ mot_de_passe }}">
                </div>
            </div>
            <label>Poids (kg)</label>
            <input type="text" name="poids" value="{{ poids }}">
            <div class="actions">
                {% if running %}
                    <button type="submit" class="btn" disabled>Bot en cours...</button>
                {% else %}
                    <button type="submit" class="btn">Lancer le bot</button>
                {% endif %}
            </div>
        </form>
        {% if running %}
        <form method="post" action="/stop" style="display:inline; margin-top:10px;">
            <button type="submit" class="btn btn-stop">Arrêter le bot</button>
        </form>
        {% endif %}
    </div>

    <div class="card">
        <h2>Logs</h2>
        <div id="logs"></div>
    </div>

<script>
async function refreshLogs() {
    try {
        const res = await fetch("/logs");
        const data = await res.json();
        const box = document.getElementById("logs");
        box.textContent = data.logs.join("\\n");
        box.scrollTop = box.scrollHeight;
    } catch (e) {}
}
setInterval(refreshLogs, 1000);
refreshLogs();
</script>
</body>
</html>
"""


# ──────────────────────────────────────────────────────
# DONNEES
# ──────────────────────────────────────────────────────
def est_nan(val) -> bool:
    """Vérifie si une valeur est NaN/vide/None sous toutes ses formes."""
    if val is None:
        return True
    s = str(val).strip().lower()
    return s in ('', 'nan', 'none', 'nat', 'null')


def charger_commandes_depuis_fichier(filepath: str) -> pd.DataFrame:
    ext = Path(filepath).suffix.lower()

    if ext == ".csv":
        df = pd.read_csv(filepath, dtype=str)
    elif ext in [".xlsx", ".xls"]:
        df = pd.read_excel(filepath, dtype=str)
    else:
        raise ValueError("Format non supporté. Utilise CSV, XLSX ou XLS.")

    colonnes_attendues = [
        'Name', 'Email', 'Shipping Name', 'Shipping Address1', 'Shipping Address2',
        'Shipping City', 'Shipping Zip', 'Shipping Country', 'Shipping Phone'
    ]
    colonnes_douane = [
        'Lineitem name', 'Lineitem price', 'Lineitem quantity', 'Subtotal'
    ]
    colonnes_min = [
        'Name', 'Shipping Name', 'Shipping Address1',
        'Shipping City', 'Shipping Zip', 'Shipping Country'
    ]

    for col in colonnes_min:
        if col not in df.columns:
            raise ValueError(f"Colonne manquante dans le fichier : {col}")

    if 'Fulfillment Status' in df.columns:
        mask = df['Fulfillment Status'].fillna('').str.strip().str.lower() == 'unfulfilled'
        df = df[mask]

    df = df.drop_duplicates(subset='Name', keep='first')

    if 'Shipping Address2' not in df.columns:
        df['Shipping Address2'] = ''
    if 'Shipping Phone' not in df.columns:
        df['Shipping Phone'] = ''
    if 'Email' not in df.columns:
        df['Email'] = ''
    for col in colonnes_douane:
        if col not in df.columns:
            df[col] = ''

    df['Email'] = df['Email'].fillna('').astype(str).str.strip()
    df['Shipping Zip'] = df['Shipping Zip'].fillna('').astype(str).str.replace("'", "").str.strip()
    df['Shipping Country'] = df['Shipping Country'].fillna('').astype(str).str.strip()
    df['Shipping Address2'] = df['Shipping Address2'].fillna('').astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    df['Shipping Phone'] = df['Shipping Phone'].fillna('').astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    df['Lineitem name'] = df['Lineitem name'].fillna('').astype(str).str.strip()
    df['Lineitem price'] = df['Lineitem price'].fillna('').astype(str).str.strip()
    df['Lineitem quantity'] = df['Lineitem quantity'].fillna('').astype(str).str.strip()
    df['Subtotal'] = df['Subtotal'].fillna('').astype(str).str.strip()

    return df[colonnes_attendues + colonnes_douane].reset_index(drop=True)


def split_nom(full_name):
    """Sépare prénom et nom. Si un seul mot, il est utilisé comme nom de famille."""
    parts = str(full_name).strip().split(' ', 1)
    if len(parts) == 1:
        return '', parts[0]  # un seul mot → nom de famille
    return parts[0], parts[1]


def formater_telephone(telephone):
    """Normalise tous les formats vers 10 chiffres français.
    Gère : +33612345678 / 0033612345678 / 33612345678 / 612345678 / 0612345678
    """
    t = str(telephone).strip()
    # Nettoyer tous les séparateurs
    t = re.sub(r'[\s\-\.\(\)]+', '', t)

    if t.startswith('+33'):
        t = '0' + t[3:]
    elif t.startswith('0033'):
        t = '0' + t[4:]
    elif t.startswith('33') and len(t) == 11:
        t = '0' + t[2:]
    elif len(t) == 9 and t[0] in ('6', '7'):
        t = '0' + t

    # Ne garder que les chiffres
    t = re.sub(r'[^\d]', '', t)

    return t


def valider_telephone(telephone: str) -> tuple:
    """Valide un numéro de téléphone français. Retourne (est_valide, message)."""
    if not telephone:
        return False, "vide"
    if len(telephone) != 10:
        return False, f"{len(telephone)} chiffres au lieu de 10"
    if not telephone.startswith(('01', '02', '03', '04', '05', '06', '07', '09')):
        return False, f"commence par {telephone[:2]} (attendu 06/07)"
    return True, "ok"


def normaliser_valeur_champ(valeur) -> str:
    """Normalise une valeur de champ pour comparer ce qui a réellement été saisi."""
    if valeur is None:
        return ""
    return re.sub(r"\s+", " ", str(valeur).strip()).casefold()


def normaliser_valeur_souple(valeur) -> str:
    """Normalise une valeur en ignorant accents et ponctuation légère."""
    if valeur is None:
        return ""
    texte = unicodedata.normalize("NFKD", str(valeur))
    texte = "".join(car for car in texte if not unicodedata.combining(car))
    texte = re.sub(r"[^0-9a-zA-Z]+", " ", texte.casefold())
    return re.sub(r"\s+", " ", texte).strip()


def normaliser_code_pays(pays) -> str:
    """Ramene les variantes connues vers un code ISO simple."""
    brut = normaliser_valeur_champ(pays).upper()
    mapping = {
        "": "FR",
        "FR": "FR",
        "FRA": "FR",
        "FRANCE": "FR",
        "CH": "CH",
        "CHE": "CH",
        "SUISSE": "CH",
        "SWITZERLAND": "CH",
        "SCHWEIZ": "CH",
        "SVIZZERA": "CH",
    }
    if brut in mapping:
        return mapping[brut]
    return brut[:3] if len(brut) > 3 else brut


def est_destination_france(pays) -> bool:
    return normaliser_code_pays(pays) == "FR"


def libelle_pays_caracteristiques(pays) -> str:
    """Retourne le libelle attendu dans le dropdown pays de la page Caracteristiques."""
    code = normaliser_code_pays(pays)
    mapping = {
        "FR": "France",
        "CH": "Suisse",
    }
    return mapping.get(code, "")


def codes_postaux_ville_francaise(code_postal, ville):
    """Retourne les codes postaux que La Poste peut utiliser pour la suggestion de ville."""
    cp = re.sub(r"\D", "", str(code_postal or ""))[:5]
    ville_normalisee = normaliser_valeur_champ(ville)
    candidats = []

    def ajouter(candidat):
        candidat = re.sub(r"\D", "", str(candidat or ""))[:5]
        if candidat and candidat not in candidats:
            candidats.append(candidat)

    ajouter(cp)

    # Paris peut exposer une suggestion "ville" en 750xx alors que la commande contient 751xx.
    if "paris" in ville_normalisee and len(cp) == 5 and cp.startswith("751"):
        suffixe = cp[-2:]
        if "01" <= suffixe <= "20":
            ajouter(f"750{suffixe}")

    return candidats


def adapter_telephone_pour_pays(telephone, pays) -> str:
    """Adapte le numero au champ visible sur La Poste selon le pays."""
    code_pays = normaliser_code_pays(pays)
    texte = str(telephone or "").strip()
    chiffres = re.sub(r"\D", "", texte)

    if code_pays == "FR":
        return formater_telephone(texte)

    if code_pays == "CH":
        if chiffres.startswith("0041"):
            chiffres = chiffres[4:]
        elif chiffres.startswith("41"):
            chiffres = chiffres[2:]
        if chiffres and not chiffres.startswith("0"):
            chiffres = "0" + chiffres
        return chiffres

    return chiffres


def convertir_decimal(valeur, default=0.0) -> float:
    """Convertit une valeur texte en float simple."""
    if est_nan(valeur):
        return default

    texte = str(valeur).strip()
    if not texte:
        return default

    texte = texte.replace("'", "").replace("€", "").replace("\u00a0", " ")
    texte = re.sub(r"\s+", "", texte)

    if "," in texte and "." not in texte:
        texte = texte.replace(",", ".")
    elif "," in texte and "." in texte:
        if texte.rfind(",") > texte.rfind("."):
            texte = texte.replace(".", "").replace(",", ".")
        else:
            texte = texte.replace(",", "")

    try:
        return float(texte)
    except Exception:
        return default


def formater_decimal_douane(valeur: float, decimals=2) -> str:
    """Formate une valeur numerique pour les champs douane."""
    texte = f"{float(valeur):.{decimals}f}"
    if decimals > 0:
        texte = texte.rstrip("0").rstrip(".")
    return texte or "0"


def quantite_article_commande(row) -> int:
    """Retourne la quantite de la ligne commande pour la douane."""
    quantite = convertir_decimal(row.get("Lineitem quantity", 1), default=1)
    return max(1, int(round(quantite or 1)))


def description_article_douane(row) -> str:
    """Construit une description courte et exploitable pour la douane."""
    texte = str(row.get("Lineitem name", "") or "").strip()
    texte = re.sub(r"\s+", " ", texte)
    return texte[:60] or "Produit"


def code_sh_douane(row) -> str:
    """Deduit un code SH pour les produits actuels du catalogue."""
    texte = normaliser_valeur_souple(row.get("Lineitem name", ""))
    correspondances = (
        (("dentifrice", "toothpaste"), "330610"),
        (("blanchiment", "whitening", "siwak", "brush", "brosse"), "330690"),
    )

    for mots_cles, code in correspondances:
        if any(mot in texte for mot in mots_cles):
            return code

    raise RuntimeError(f"Code SH introuvable pour l'article '{row.get('Lineitem name', '')}'")


def poids_unitaire_douane(poids_total, row) -> str:
    """Calcule le poids unitaire a declarer en douane."""
    total = convertir_decimal(poids_total, default=0.0)
    quantite = quantite_article_commande(row)
    if total <= 0:
        raise RuntimeError("Poids total invalide pour la declaration douane")
    return formater_decimal_douane(total / quantite, decimals=3)


def valeur_unitaire_douane(row) -> str:
    """Calcule la valeur unitaire HT de l'objet."""
    valeur = convertir_decimal(row.get("Lineitem price", 0), default=0.0)
    if valeur <= 0:
        sous_total = convertir_decimal(row.get("Subtotal", 0), default=0.0)
        quantite = quantite_article_commande(row)
        valeur = sous_total / quantite if sous_total > 0 else 0.01
    return formater_decimal_douane(valeur, decimals=2)


# ──────────────────────────────────────────────────────
# HELPERS PLAYWRIGHT
# ──────────────────────────────────────────────────────
async def screenshot_erreur(page, nom: str, selectors=None):
    """Sauvegarde un screenshot et un état textuel pour faciliter le debug."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = SCREENSHOTS_DIR / f"{ts}_{nom}.png"
    txt_path = SCREENSHOTS_DIR / f"{ts}_{nom}.txt"

    try:
        await page.screenshot(path=str(path), full_page=False)
        log(f"📸 Screenshot : {path.name}")
    except Exception:
        path = None

    try:
        active = await page.evaluate(
            """() => {
                const el = document.activeElement;
                if (!el) return null;
                return {
                    tag: el.tagName || '',
                    id: el.id || '',
                    name: el.name || '',
                    type: el.type || '',
                    value: typeof el.value === 'string' ? el.value : ''
                };
            }"""
        )
    except Exception:
        active = None

    lignes = [
        f"url={getattr(page, 'url', '')}",
        f"active={active}",
    ]

    if selectors:
        for label, selector in selectors.items():
            try:
                loc = page.locator(selector).first
                exists = await loc.count() > 0
                visible = exists and await loc.is_visible()
                valeur = ""
                if exists:
                    try:
                        valeur = await loc.input_value()
                    except Exception:
                        try:
                            valeur = " ".join((await loc.inner_text()).split())
                        except Exception:
                            valeur = ""
                lignes.append(f"{label} | selector={selector} | visible={visible} | value={valeur}")
            except Exception as err:
                lignes.append(f"{label} | selector={selector} | error={err}")

    try:
        etat_enregistrer = await lire_etat_bouton(page, "Enregistrer")
        if etat_enregistrer.get("visible"):
            lignes.append(f"bouton_enregistrer={etat_enregistrer}")
    except Exception:
        pass

    try:
        etat_panier = await lire_etat_bouton(page, "Ajouter au panier")
        if etat_panier.get("visible"):
            lignes.append(f"bouton_panier={etat_panier}")
    except Exception:
        pass

    try:
        messages = await relever_messages_utiles(page, limit=20)
    except Exception:
        messages = []
    lignes.append("messages=" + " | ".join(messages))

    try:
        txt_path.write_text("\n".join(lignes), encoding="utf-8")
        log(f"🧾 Diagnostic : {txt_path.name}")
    except Exception:
        pass


async def fermer_popups(page):
    """Ferme toutes les popups/bannieres connues qui peuvent bloquer."""
    popups = [
        # Cookies
        "button:has-text('Accepter et fermer')",
        "button:has-text('Tout accepter')",
        "#onetrust-accept-btn-handler",
        # Popup pro/particulier
        "button:has-text('Rester sur le site Particulier')",
        # Popups génériques La Poste (pas trop large pour éviter de fermer les modals utiles)
        ".modal-close:visible",
        "button[data-dismiss='modal']:visible",
    ]
    for sel in popups:
        try:
            el = page.locator(sel).first
            if await el.count() > 0 and await el.is_visible():
                await el.click()
                await asyncio.sleep(0.3)
        except Exception:
            pass


async def attendre_et_cliquer(page, selector: str, timeout=10000, description="élément"):
    """Attend qu'un élément soit visible et clique dessus."""
    try:
        el = page.locator(selector).first
        await el.wait_for(state="visible", timeout=timeout)
        await el.evaluate("node => node.scrollIntoView({ block: 'center', inline: 'center' })")
        try:
            await el.click(timeout=min(timeout, 5000), force=True)
        except Exception:
            log(f"⚠️ Clic fallback sur {description}")
            box = await el.bounding_box()
            if box:
                await page.mouse.click(
                    box["x"] + box["width"] / 2,
                    box["y"] + box["height"] / 2,
                )
            else:
                await el.dispatch_event("click")
        return True
    except PlaywrightTimeoutError:
        await screenshot_erreur(page, f"timeout_{description.replace(' ', '_')}")
        raise RuntimeError(f"Timeout : '{description}' introuvable après {timeout // 1000}s")


async def attendre_url(page, pattern: str, timeout=20000):
    """Attend une navigation vers une URL correspondant au pattern."""
    try:
        await page.wait_for_url(pattern, timeout=timeout)
    except PlaywrightTimeoutError:
        url_actuelle = page.url
        await screenshot_erreur(page, f"timeout_url_{pattern.replace('*', '').replace('/', '_')}")
        raise RuntimeError(f"Navigation timeout : attendu '{pattern}', actuellement sur '{url_actuelle}'")


async def fermer_autocompletion(page):
    """Ferme les suggestions d'autocomplétion qui peuvent bloquer le champ suivant."""
    for _ in range(2):
        suggestion_visible = False
        for sel in ("[role='listbox']", "li[role='option']", ".suggestions__item"):
            try:
                el = page.locator(sel).first
                if await el.count() > 0 and await el.is_visible():
                    suggestion_visible = True
                    break
            except Exception:
                continue

        if not suggestion_visible:
            return

        try:
            await page.keyboard.press("Escape")
        except Exception:
            return
        await asyncio.sleep(0.15)


async def choisir_suggestion_autocomplete(
    page,
    listbox_id: str,
    valeurs_cibles,
    description="suggestion",
    timeout=5000,
    fallback_principal=True,
    fallback_unique=True,
):
    """Sélectionne une suggestion visible qui correspond aux valeurs attendues."""
    valeurs = [normaliser_valeur_champ(v) for v in valeurs_cibles if normaliser_valeur_champ(v)]
    if not valeurs:
        return False

    options = page.locator(f"#{listbox_id} [role='option']")
    deadline = asyncio.get_event_loop().time() + timeout / 1000

    while asyncio.get_event_loop().time() < deadline:
        try:
            count = await options.count()
        except Exception:
            count = 0

        if count:
            visibles = []
            for idx in range(count):
                opt = options.nth(idx)
                try:
                    if not await opt.is_visible():
                        continue
                    texte = await opt.inner_text()
                except Exception:
                    continue

                texte_normalise = normaliser_valeur_champ(texte)
                visibles.append((opt, texte_normalise))

            for opt, texte_normalise in visibles:
                if all(valeur in texte_normalise for valeur in valeurs):
                    await opt.click()
                    await asyncio.sleep(0.18)
                    log(f"✅ Suggestion {description} : {texte_normalise}")
                    return True

            cible_principale = valeurs[-1]
            if fallback_principal:
                for opt, texte_normalise in visibles:
                    if cible_principale in texte_normalise:
                        await opt.click()
                        await asyncio.sleep(0.18)
                        log(f"✅ Suggestion {description} : {texte_normalise}")
                        return True

            if fallback_unique and len(visibles) == 1:
                opt, texte_normalise = visibles[0]
                await opt.click()
                await asyncio.sleep(0.18)
                log(f"✅ Suggestion {description} (fallback unique) : {texte_normalise}")
                return True

        await asyncio.sleep(0.12)

    return False


async def relever_messages_utiles(page, limit=8):
    """Recupere quelques messages visibles utiles pour debug."""
    try:
        body = await page.locator("body").inner_text()
    except Exception:
        return []

    messages = []
    patterns = (
        "obligatoire",
        "veuillez",
        "erreur",
        "format attendu",
        "introuvable",
        "merci de",
    )
    for line in body.splitlines():
        propre = " ".join(line.split())
        if not propre:
            continue
        if any(motif in propre.casefold() for motif in patterns):
            if propre not in messages:
                messages.append(propre)
        if len(messages) >= limit:
            break
    return messages


async def lire_etat_bouton(page, texte: str):
    """Retourne l'etat visible/actif d'un bouton de page."""
    try:
        bouton = page.locator(f"button:has-text('{texte}')").first
        count = await bouton.count()
        if count == 0:
            return {"visible": False, "enabled": False}

        visible = await bouton.is_visible()
        if not visible:
            return {"visible": False, "enabled": False}

        disabled = await bouton.get_attribute("disabled")
        aria = await bouton.get_attribute("aria-disabled")
        classes = await bouton.get_attribute("class") or ""
        texte_bouton = " ".join((await bouton.inner_text()).split())
        enabled = disabled is None and aria != "true" and "disabled" not in classes.lower()
        return {
            "visible": True,
            "enabled": enabled,
            "disabled_attr": disabled,
            "aria_disabled": aria,
            "classes": classes,
            "text": texte_bouton,
        }
    except Exception as err:
        return {"visible": False, "enabled": False, "error": str(err)}


async def stabiliser_formulaire(page, tabs=1, delay=0.12):
    """Ferme les suggestions et force une vraie sortie de champ."""
    await fermer_autocompletion(page)

    try:
        active = await page.evaluate(
            """() => {
                const el = document.activeElement;
                if (!el) return null;
                return { tag: el.tagName || '', id: el.id || '', name: el.name || '' };
            }"""
        )
    except Exception:
        active = None

    if active and active.get("tag") in {"INPUT", "TEXTAREA", "SELECT"}:
        for _ in range(max(0, tabs)):
            try:
                await page.keyboard.press("Tab")
                await asyncio.sleep(delay)
            except Exception:
                break

    try:
        await page.evaluate(
            """() => {
                const el = document.activeElement;
                if (el && typeof el.blur === 'function') {
                    el.blur();
                }
            }"""
        )
    except Exception:
        pass

    await asyncio.sleep(delay)
    await fermer_autocompletion(page)


async def lire_destination_caracteristiques(page) -> str:
    """Lit la destination actuellement visible sur la premiere page du parcours."""
    for selector in (
        "#destination .lp-dropdown__value",
        "#destination .lp-dropdown__combobox",
    ):
        try:
            loc = page.locator(selector).first
            if await loc.count() == 0 or not await loc.is_visible():
                continue
            texte = " ".join((await loc.inner_text()).split())
            if texte:
                return texte
        except Exception:
            continue
    return ""


async def definir_destination_caracteristiques(page, code_pays: str):
    """Change le pays de destination sur la page Caracteristiques avant l'etape suivante."""
    code_pays = normaliser_code_pays(code_pays)
    cible = libelle_pays_caracteristiques(code_pays)

    if not cible:
        if code_pays == "FR":
            return
        raise RuntimeError(f"Pays destination non gere sur la page Caracteristiques : {code_pays}")

    combobox = page.locator("#destination .lp-dropdown__combobox").first
    await combobox.wait_for(state="visible", timeout=10000)

    destination_actuelle = await lire_destination_caracteristiques(page)
    if normaliser_valeur_souple(destination_actuelle) == normaliser_valeur_souple(cible):
        log(f"✅ Destination colis : {cible}")
        return

    last_error = None

    for tentative in range(1, 4):
        try:
            await combobox.scroll_into_view_if_needed()
            await combobox.click(force=True)

            listbox = page.locator("#destination .lp-dropdown__listbox").first
            await listbox.wait_for(state="visible", timeout=4000)

            option = page.locator(
                "#destination .lp-dropdown__listbox [role='option']",
                has_text=cible,
            ).first
            await option.wait_for(state="visible", timeout=4000)
            await option.scroll_into_view_if_needed()
            await option.click(force=True)

            deadline = asyncio.get_event_loop().time() + 6
            while asyncio.get_event_loop().time() < deadline:
                destination_actuelle = await lire_destination_caracteristiques(page)
                if normaliser_valeur_souple(destination_actuelle) == normaliser_valeur_souple(cible):
                    await asyncio.sleep(0.35)
                    log(f"✅ Destination colis : {cible}")
                    return
                await asyncio.sleep(0.12)

            last_error = RuntimeError(f"destination lue '{destination_actuelle}' au lieu de '{cible}'")
        except Exception as err:
            last_error = err
        finally:
            try:
                await page.keyboard.press("Escape")
            except Exception:
                pass
            await asyncio.sleep(0.12)

    await screenshot_erreur(
        page,
        f"destination_caracteristiques_{code_pays.lower()}",
        selectors={
            "combobox_destination": "#destination .lp-dropdown__combobox",
            "valeur_destination": "#destination .lp-dropdown__value",
            "recherche_destination": "#destination .lp-dropdown__listbox input[type='text']",
        },
    )
    raise RuntimeError(f"Impossible de selectionner la destination {cible} sur la page Caracteristiques ({last_error})")


async def remplir_champ(page, selector: str, valeur: str, description="champ"):
    """Remplit un champ texte de façon fiable avec fallback."""
    valeur = "" if valeur is None else str(valeur)
    last_error = None

    for tentative in range(1, 4):
        try:
            el = page.locator(selector).first
            await el.wait_for(state="visible", timeout=8000)
            await el.scroll_into_view_if_needed()
            await fermer_autocompletion(page)

            try:
                await el.fill(valeur)
            except Exception as fill_err:
                last_error = fill_err
                await el.evaluate(
                    """(node, value) => {
                        node.focus();
                        node.value = value;
                        node.dispatchEvent(new Event('input', { bubbles: true }));
                        node.dispatchEvent(new Event('change', { bubbles: true }));
                    }""",
                    valeur,
                )

            await asyncio.sleep(0.2)
            actual = await el.input_value()
            if normaliser_valeur_champ(actual) == normaliser_valeur_champ(valeur):
                return True

            await el.focus()
            await el.press("Control+A" if SYSTEME != "Darwin" else "Meta+A")
            await el.press("Backspace")
            await el.type(valeur, delay=30)
            await asyncio.sleep(0.2)

            actual = await el.input_value()
            if normaliser_valeur_champ(actual) == normaliser_valeur_champ(valeur):
                return True

            last_error = RuntimeError(f"valeur lue '{actual}' au lieu de '{valeur}'")
        except Exception as e:
            last_error = e

        await fermer_autocompletion(page)
        await asyncio.sleep(0.2 * tentative)

    log(f"⚠️ Impossible de remplir {description} ({selector}) : {last_error}")
    return False


async def remplir_champ_obligatoire(page, selector: str, valeur: str, description="champ"):
    """Remplit un champ qui doit être correct avant de poursuivre."""
    ok = await remplir_champ(page, selector, valeur, description)
    if ok:
        return

    await screenshot_erreur(page, f"champ_{description.replace(' ', '_')}")
    raise RuntimeError(f"Champ obligatoire impossible à remplir : {description}")


async def remplir_autocomplete_obligatoire(
    page,
    selector: str,
    valeur: str,
    listbox_id: str,
    description="champ",
    suggestions=None,
    suggestion_requise=True,
    fallback_principal=True,
    fallback_unique=True,
):
    """Remplit un champ d'autocomplétion de façon fiable avec une saisie clavier semi-lente."""
    valeur = "" if valeur is None else str(valeur)
    last_error = None

    for tentative in range(1, 4):
        try:
            el = page.locator(selector).first
            await el.wait_for(state="visible", timeout=10000)
            await el.scroll_into_view_if_needed()
            await fermer_autocompletion(page)
            await el.focus()
            await el.press("Control+A" if SYSTEME != "Darwin" else "Meta+A")
            await el.press("Backspace")
            await asyncio.sleep(0.08)

            # Les champs La Poste d'autocomplétion réagissent mieux à de vraies frappes.
            await el.type(valeur, delay=32 if tentative == 1 else 42)
            await asyncio.sleep(0.18 + 0.08 * tentative)

            actual = await el.input_value()
            if normaliser_valeur_champ(actual) != normaliser_valeur_champ(valeur):
                try:
                    await el.fill(valeur)
                    await asyncio.sleep(0.15)
                    actual = await el.input_value()
                except Exception as fill_err:
                    last_error = fill_err
                if normaliser_valeur_champ(actual) != normaliser_valeur_champ(valeur):
                    last_error = RuntimeError(f"valeur lue '{actual}' au lieu de '{valeur}'")
                    continue

            if suggestions:
                suggestion_ok = await choisir_suggestion_autocomplete(
                    page,
                    listbox_id,
                    suggestions,
                    description=description,
                    timeout=1800 + 600 * tentative,
                    fallback_principal=fallback_principal,
                    fallback_unique=fallback_unique,
                )
                if not suggestion_ok:
                    await asyncio.sleep(0.15)
                    suggestion_ok = await choisir_suggestion_autocomplete(
                        page,
                        listbox_id,
                        suggestions,
                        description=description,
                        timeout=1000 + 400 * tentative,
                        fallback_principal=fallback_principal,
                        fallback_unique=fallback_unique,
                    )
                if not suggestion_ok and suggestion_requise:
                    last_error = RuntimeError(f"aucune suggestion valide pour {description}")
                    continue

            return
        except Exception as e:
            last_error = e
            await fermer_autocompletion(page)
            await asyncio.sleep(0.2 * tentative)

    await screenshot_erreur(page, f"champ_{description.replace(' ', '_')}")
    raise RuntimeError(f"Champ obligatoire impossible à remplir : {description} ({last_error})")


async def remplir_ville_francaise(page, code_postal, ville):
    """Tape la ville puis choisit une suggestion cohérente avec le code postal France."""
    await remplir_autocomplete_obligatoire(
        page,
        "#city-addressForm",
        ville,
        "city-addressForm-listbox",
        description="ville",
        suggestions=None,
    )

    for code_ville in codes_postaux_ville_francaise(code_postal, ville):
        if await choisir_suggestion_autocomplete(
            page,
            "city-addressForm-listbox",
            [code_ville, ville],
            description="ville",
            timeout=2200,
            fallback_principal=False,
            fallback_unique=False,
        ):
            return code_ville

    return None


async def selectionner_tuile(page, input_id: str, selectors, description="option", timeout=8000):
    """Sélectionne une tuile radio/choix et vérifie que l'input correspondant est coché."""
    input_selector = f"#{input_id}"

    for _ in range(3):
        for sel in selectors:
            try:
                el = page.locator(sel).first
                if await el.count() > 0 and await el.is_visible():
                    await el.scroll_into_view_if_needed()
                    try:
                        await el.click(timeout=3000, force=True)
                    except Exception:
                        await el.evaluate("(node) => node.click()")
                    await asyncio.sleep(0.3)
                    break
            except Exception:
                continue

        try:
            coche = await page.evaluate(
                """sel => {
                    const input = document.querySelector(sel);
                    if (!input) return false;
                    return input.checked === true || input.getAttribute('aria-checked') === 'true';
                }""",
                input_selector,
            )
        except Exception:
            coche = False

        if coche:
            return

        try:
            await page.evaluate(
                """inputId => {
                    const input = document.getElementById(inputId);
                    if (!input) return false;
                    input.click();
                    input.checked = true;
                    input.dispatchEvent(new Event('input', { bubbles: true }));
                    input.dispatchEvent(new Event('change', { bubbles: true }));
                    return true;
                }""",
                input_id,
            )
            await asyncio.sleep(0.3)
        except Exception:
            pass

        try:
            coche = await page.evaluate(
                """sel => {
                    const input = document.querySelector(sel);
                    if (!input) return false;
                    return input.checked === true || input.getAttribute('aria-checked') === 'true';
                }""",
                input_selector,
            )
        except Exception:
            coche = False

        if coche:
            return

    await screenshot_erreur(page, f"{description.replace(' ', '_')}")
    raise RuntimeError(f"{description} introuvable ou non sélectionné")


async def attendre_bouton_actif(page, texte: str, timeout=12000):
    """Attend qu'un bouton avec ce texte soit actif (non disabled)."""
    bouton = page.locator(f"button:has-text('{texte}')").first
    await bouton.wait_for(state="visible", timeout=timeout)

    deadline = asyncio.get_event_loop().time() + timeout / 1000
    while asyncio.get_event_loop().time() < deadline:
        disabled = await bouton.get_attribute("disabled")
        aria = await bouton.get_attribute("aria-disabled")
        classes = await bouton.get_attribute("class") or ""
        if disabled is None and aria != "true" and "disabled" not in classes.lower():
            return bouton
        await asyncio.sleep(0.2)

    log(f"⚠️ Bouton '{texte}' toujours désactivé après {timeout // 1000}s, tentative de clic quand même")
    return bouton


async def cliquer_etape_suivante(page, description=""):
    """Clique sur le bouton Étape suivante avec fallback JS."""
    bouton = await attendre_bouton_actif(page, "tape suivante")
    try:
        await bouton.click(timeout=5000)
    except Exception:
        log(f"⚠️ Clic JS fallback Étape suivante {description}")
        await page.evaluate("""
            () => {
                const btns = [...document.querySelectorAll('button')];
                const visibles = btns.filter(b => b.offsetParent !== null && b.innerText && b.innerText.includes('tape suivante'));
                if (visibles.length) visibles[visibles.length - 1].click();
            }
        """)


async def attendre_zone_arrivee_pret(page, timeout=15000):
    """Attend que la zone téléphone / livraison soit exploitable après l'adresse."""
    selectors = [
        "#phone",
        "#email",
        "input[name='phone']",
        "input[name='email']",
        "input[type='tel']",
        "label[for='card-input-id-L_BAL']",
        "label:has-text('boîte aux lettres')",
        "label:has-text('Boîte aux lettres')",
        "label[for='card-input-id-L_CS']",
        "label:has-text('Avec signature')",
        "text=Notifier votre destinataire",
    ]
    deadline = asyncio.get_event_loop().time() + timeout / 1000

    while asyncio.get_event_loop().time() < deadline:
        await fermer_autocompletion(page)

        for sel in selectors:
            try:
                el = page.locator(sel).first
                if await el.count() > 0 and await el.is_visible():
                    return
            except Exception:
                continue

        await asyncio.sleep(0.2)

    raise RuntimeError("La zone téléphone / livraison n'est pas apparue après validation de l'adresse")


async def retour_formulaire(page, email, mot_de_passe):
    """Retourne au formulaire caractéristiques. Gère la reconnexion si nécessaire."""
    await page.goto("https://www.laposte.fr/colissimo-en-ligne", wait_until="domcontentloaded")
    await asyncio.sleep(1)
    await fermer_popups(page)

    # Vérifier si on est encore connecté
    try:
        await page.wait_for_selector("text=SE CONNECTER", timeout=2000)
        log("ℹ️ Session expirée, reconnexion...")
        await connecter(page, email, mot_de_passe)
        await page.goto("https://www.laposte.fr/colissimo-en-ligne", wait_until="domcontentloaded")
        await asyncio.sleep(1)
        await fermer_popups(page)
    except PlaywrightTimeoutError:
        pass  # toujours connecté

    await attendre_et_cliquer(page, "button:has-text('Envoyez votre colis')", timeout=12000, description="Envoyez votre colis")
    await attendre_url(page, "**/parcours/caracteristiques**", timeout=15000)
    await asyncio.sleep(0.8)
    await fermer_popups(page)
    await page.wait_for_selector("#weightInput", state="visible", timeout=10000)


async def selectionner_mode_livraison_arrivee(page, code_pays: str):
    """Choisit un mode de livraison compatible avec la destination."""
    code_pays = normaliser_code_pays(code_pays)
    modes = []

    if est_destination_france(code_pays):
        modes.append((
            "card-input-id-L_BAL",
            [
                "label[for='card-input-id-L_BAL']",
                "label:has-text('boîte aux lettres')",
                "label:has-text('Boîte aux lettres')",
            ],
            "mode de livraison boîte aux lettres",
            "boîte aux lettres",
        ))

    modes.append((
        "card-input-id-L_CS",
        [
            "label[for='card-input-id-L_CS']",
            "label:has-text('Avec signature')",
        ],
        "mode de livraison avec signature",
        "avec signature",
    ))

    if not est_destination_france(code_pays):
        modes.append((
            "card-input-id-L_BAL",
            [
                "label[for='card-input-id-L_BAL']",
                "label:has-text('boîte aux lettres')",
                "label:has-text('Boîte aux lettres')",
            ],
            "mode de livraison boîte aux lettres",
            "boîte aux lettres",
        ))

    last_error = None
    for input_id, selectors, description, libelle in modes:
        try:
            await selectionner_tuile(
                page,
                input_id,
                selectors,
                description=description,
                timeout=8000,
            )
            await asyncio.sleep(0.3)
            log(f"✅ Mode de livraison : {libelle}")
            return
        except Exception as err:
            last_error = err
            continue

    raise last_error or RuntimeError("Aucun mode de livraison compatible trouvé")


async def remplir_declaration_douane(page, row, poids_total):
    """Renseigne la declaration douane pour les destinations internationales."""
    code_pays = normaliser_code_pays(row.get("Shipping Country", "FR"))
    if est_destination_france(code_pays):
        return

    if "/parcours/douanes" not in page.url:
        await cliquer_etape_suivante(page, "(douanes)")
        await attendre_url(page, "**/parcours/douanes**", timeout=15000)
        await asyncio.sleep(0.6)
        await fermer_popups(page)

    start_button = page.locator("button.customs-content__start-button").first
    if await start_button.count() > 0 and await start_button.is_visible():
        await start_button.click(force=True)
        await asyncio.sleep(0.5)

    await page.wait_for_selector("#parcelContent", state="visible", timeout=10000)
    await page.select_option("#parcelContent", value="envoi-commercial")
    await asyncio.sleep(0.35)

    add_article = page.locator("button.parcel-content__add-article").first
    await add_article.wait_for(state="visible", timeout=10000)
    await add_article.click(force=True)

    await page.locator("input[name='description']").first.wait_for(state="visible", timeout=10000)

    description = description_article_douane(row)
    code_sh = code_sh_douane(row)
    poids_unitaire = poids_unitaire_douane(poids_total, row)
    valeur_unitaire = valeur_unitaire_douane(row)
    quantite = str(quantite_article_commande(row))

    await remplir_champ_obligatoire(page, "input[name='description']", description, "description douane")
    await page.select_option("select[name='originIso']", value=PAYS_ORIGINE_DOUANE_PAR_DEFAUT)
    await remplir_champ_obligatoire(page, "input[name='SHNumber']", code_sh, "code SH")
    await remplir_champ_obligatoire(page, "input[name='unitWeight']", poids_unitaire, "poids unitaire douane")
    await remplir_champ_obligatoire(page, "input[name='unitValue']", valeur_unitaire, "valeur unitaire douane")

    quantite_input = page.locator("input[type='number']").first
    await quantite_input.wait_for(state="visible", timeout=8000)
    await quantite_input.fill(quantite)

    await stabiliser_formulaire(page, tabs=1)

    save_button = page.locator("button.customs-article__save").first
    await save_button.wait_for(state="visible", timeout=8000)
    await save_button.click(force=True)

    deadline_save = asyncio.get_event_loop().time() + 8
    while asyncio.get_event_loop().time() < deadline_save:
        description_visible = False
        try:
            description_visible = await page.locator("input[name='description']").first.is_visible()
        except Exception:
            description_visible = False

        validate_visible = False
        try:
            validate_visible = await page.locator("button.customs-content__button--validate").first.is_visible()
        except Exception:
            validate_visible = False

        if not description_visible and validate_visible:
            break

        await asyncio.sleep(0.2)
    else:
        await screenshot_erreur(
            page,
            "douanes_enregistrement_objet",
            selectors={
                "description_douane": "input[name='description']",
                "code_sh": "input[name='SHNumber']",
                "save_objet": "button.customs-article__save",
            },
        )
        raise RuntimeError("L'objet douane n'a pas ete enregistre correctement")

    validate_button = page.locator("button.customs-content__button--validate").first
    await validate_button.wait_for(state="visible", timeout=8000)
    await validate_button.click(force=True)

    deadline_validate = asyncio.get_event_loop().time() + 8
    while asyncio.get_event_loop().time() < deadline_validate:
        try:
            body = await page.locator("body").inner_text()
        except Exception:
            body = ""

        if "Vous devez compléter les formalités douanières" not in body:
            log(f"✅ Douanes : déclaration validée ({code_sh})")
            return

        await asyncio.sleep(0.25)

    await screenshot_erreur(
        page,
        "douanes_validation",
        selectors={
            "nature_colis": "#parcelContent",
            "valider_douane": "button.customs-content__button--validate",
            "ajouter_panier": "button:has-text('Ajouter au panier')",
        },
    )
    raise RuntimeError("La declaration douane n'a pas ete validee par La Poste")


async def selectionner_civilite(page, valeur_preferee="MALE"):
    """Selectionne une civilite de maniere fiable et la verifie."""
    valeurs = [valeur_preferee]
    if valeur_preferee != "MALE":
        valeurs.append("MALE")
    if valeur_preferee != "FEMALE":
        valeurs.append("FEMALE")

    for valeur in valeurs:
        selector = f"input[name='sex'][value='{valeur}']"
        try:
            radio = page.locator(selector).first
            if await radio.count() == 0:
                continue
            await radio.wait_for(state="attached", timeout=5000)
            try:
                await radio.check(force=True)
            except Exception:
                await page.evaluate(
                    """sel => {
                        const el = document.querySelector(sel);
                        if (!el) return false;
                        el.checked = true;
                        el.dispatchEvent(new Event('input', { bubbles: true }));
                        el.dispatchEvent(new Event('change', { bubbles: true }));
                        return el.checked;
                    }""",
                    selector,
                )
            checked = await page.evaluate(
                "sel => !!document.querySelector(sel)?.checked",
                selector,
            )
            if checked:
                return valeur
        except Exception:
            continue

    raise RuntimeError("Impossible de selectionner la civilite")


async def definir_pays_modal(page, code_pays: str):
    """Positionne le pays du destinataire, meme si le select est verrouille."""
    code_pays = normaliser_code_pays(code_pays)
    resultat = await page.evaluate(
        """code => {
            const select = document.querySelector('#country-addressForm');
            if (!select) return { ok: false, reason: 'missing' };
            const option = [...select.options].find(opt => opt.value === code);
            if (!option) {
                return {
                    ok: false,
                    reason: 'unknown-option',
                    current: select.value,
                    selectedText: select.options[select.selectedIndex]?.textContent?.trim() || ''
                };
            }
            select.disabled = false;
            select.removeAttribute('disabled');
            select.value = code;
            select.dispatchEvent(new Event('input', { bubbles: true }));
            select.dispatchEvent(new Event('change', { bubbles: true }));
            return {
                ok: select.value === code,
                current: select.value,
                selectedText: select.options[select.selectedIndex]?.textContent?.trim() || ''
            };
        }""",
        code_pays,
    )

    if not resultat or not resultat.get("ok"):
        raise RuntimeError(f"Impossible de selectionner le pays {code_pays}")

    log(f"✅ Pays : {resultat.get('selectedText') or code_pays}")


async def traiter_popups_confirmation_adresse(page):
    """Confirme les popups d'adresse quand La Poste en affiche une."""
    for _ in range(3):
        clique = False
        for texte_btn in [
            "Confirmer cette adresse",
            "Utiliser l'adresse saisie",
            "Confirmer",
            "Utiliser l'adresse selectionnee",
        ]:
            try:
                btn = page.locator(f"button:has-text('{texte_btn}')").first
                if await btn.count() > 0 and await btn.is_visible():
                    await btn.click()
                    await asyncio.sleep(0.3)
                    log(f"✅ Popup adresse : {texte_btn}")
                    clique = True
                    break
            except Exception:
                continue
        if not clique:
            return


async def attendre_adresse_enregistree(page, row, timeout=12000):
    """Valide que la carte destinataire existe vraiment apres sauvegarde."""
    cp = str(row['Shipping Zip']).strip().replace("'", "") if not est_nan(row['Shipping Zip']) else ''
    ville = str(row['Shipping City']).strip() if not est_nan(row['Shipping City']) else ''
    nom = str(row['Shipping Name']).strip() if not est_nan(row['Shipping Name']) else ''
    deadline = asyncio.get_event_loop().time() + timeout / 1000

    while asyncio.get_event_loop().time() < deadline:
        try:
            modal = page.locator("#firstName").first
            modal_visible = await modal.count() > 0 and await modal.is_visible()
        except Exception:
            modal_visible = False

        try:
            body = await page.locator("body").inner_text()
        except Exception:
            body = ""

        if not modal_visible:
            body_normalise = normaliser_valeur_souple(body)
            if (
                cp in body
                and normaliser_valeur_souple(ville) in body_normalise
                and normaliser_valeur_souple(nom.split()[-1] if nom else "") in body_normalise
            ):
                return True

        await asyncio.sleep(0.25)

    return False


async def verifier_adresse_depart_chargee(page):
    """Detecte un etat ou La Poste a perdu l'adresse d'expedition."""
    try:
        body = await page.locator("body").inner_text()
    except Exception:
        body = ""

    if "Vous devez remplir l'adresse d'expédition" in body:
        return False
    if "Une erreur est survenue lors de la récupération de votre adresse" in body:
        return False

    try:
        bouton_carnet = page.locator("button:has-text('Choisir une adresse dans le carnet')").first
        if await bouton_carnet.count() > 0 and await bouton_carnet.is_visible():
            return False
    except Exception:
        pass

    return True


async def enregistrer_adresse_destinataire(page, row):
    """Valide le modal adresse avec retries et diagnostics utiles."""
    debug_selectors = {
        "prenom": "#firstName",
        "nom": "#lastName",
        "code_postal": "#zipCode-addressForm",
        "ville": "#city-addressForm",
        "adresse": "#streetName-addressForm",
        "adresse2": "#additionalStreetNameaddressForm",
        "enregistrer": "button:has-text('Enregistrer')",
    }
    bouton = page.locator("button:has-text('Enregistrer')").first
    await bouton.wait_for(state="visible", timeout=12000)

    for tentative in range(1, 4):
        await stabiliser_formulaire(page, tabs=1)
        etat = await lire_etat_bouton(page, "Enregistrer")

        if tentative >= 2:
            await screenshot_erreur(page, f"avant_enregistrer_tentative_{tentative}", selectors=debug_selectors)

        if not etat.get("enabled"):
            log(f"⚠️ Bouton Enregistrer encore inactif (tentative {tentative}), nouvelle stabilisation")
            await asyncio.sleep(0.25)
            await stabiliser_formulaire(page, tabs=1)

        try:
            await bouton.click(timeout=3000)
        except Exception:
            log("⚠️ Clic JS fallback sur Enregistrer")
            await bouton.evaluate("(node) => node.click()")

        await asyncio.sleep(0.35)
        await traiter_popups_confirmation_adresse(page)

        if await attendre_adresse_enregistree(page, row, timeout=4500):
            return

    await screenshot_erreur(page, "echec_enregistrement_adresse", selectors=debug_selectors)
    erreurs = await relever_messages_utiles(page, limit=12)
    detail = f" ({'; '.join(erreurs)})" if erreurs else ""
    raise RuntimeError(f"L'adresse de destination n'a pas été enregistrée{detail}")


async def attendre_resultat_ajout_panier(page, timeout=20000):
    """Attend un vrai resultat de l'ajout au panier."""
    deadline = asyncio.get_event_loop().time() + timeout / 1000

    while asyncio.get_event_loop().time() < deadline:
        try:
            body = await page.locator("body").inner_text()
        except Exception:
            body = ""

        if "/parcours/recapitulatif" in page.url:
            return "success"
        if "Ajouter un autre colis" in body or "Envoyer un autre colis" in body:
            return "success"
        if "Erreur lors de l’ajout au panier" in body or "Erreur lors de l'ajout au panier" in body:
            return "cart_error"
        if "Vous devez remplir l'adresse de destination" in body:
            return "address_missing"

        await asyncio.sleep(0.3)

    return "timeout"


# ──────────────────────────────────────────────────────
# PLAYWRIGHT — FLOW PRINCIPAL
# ──────────────────────────────────────────────────────
async def connecter(page, email, mot_de_passe):
    log("🔐 Connexion en cours...")
    await fermer_popups(page)

    try:
        await page.wait_for_selector("text=SE CONNECTER", timeout=3000)
    except PlaywrightTimeoutError:
        log("✅ Déjà connecté !")
        return

    log("ℹ️ Connexion nécessaire...")
    await page.click("text=SE CONNECTER")
    await asyncio.sleep(0.5)

    # Attendre le bouton "Se connecter" dans le panneau
    try:
        await attendre_et_cliquer(page, "button:has-text('Se connecter')", timeout=8000, description="Bouton Se connecter")
    except RuntimeError:
        # Peut-être redirigé directement vers la page de login
        pass

    # Attendre la page de login (URL peut varier)
    try:
        await attendre_url(page, "**/moncompte-auth/**", timeout=15000)
    except RuntimeError:
        # Essayer un pattern alternatif
        await page.wait_for_selector("input[name='username']", state="visible", timeout=10000)

    await asyncio.sleep(0.5)
    await page.fill("input[name='username']", email)
    await asyncio.sleep(0.2)
    await page.fill("input[name='password']", mot_de_passe)
    await asyncio.sleep(0.2)
    await page.click("button[type='submit']")

    # Attendre que la connexion soit effective
    try:
        await page.wait_for_url("**/colissimo-en-ligne**", timeout=20000)
    except PlaywrightTimeoutError:
        # Peut-être redirigé ailleurs, on continue
        await asyncio.sleep(3)

    await fermer_popups(page)
    log("✅ Connecté !")


async def etape_caracteristiques(page, poids, code_pays="FR"):
    """Remplit le poids et passe à /parcours/depart."""
    await page.wait_for_selector("#weightInput", state="visible", timeout=10000)
    await definir_destination_caracteristiques(page, code_pays)
    await page.wait_for_selector("#weightInput", state="visible", timeout=10000)
    poids_input = page.locator("#weightInput")

    try:
        unite_kg = page.locator("input[name='unit'][value='kg']").first
        if await unite_kg.count() > 0 and not await unite_kg.is_checked():
            await unite_kg.check(force=True)
    except Exception:
        pass

    await poids_input.click()
    await asyncio.sleep(0.1)
    await page.keyboard.press("Control+A" if SYSTEME != "Darwin" else "Meta+A")
    await asyncio.sleep(0.05)
    await page.keyboard.press("Backspace")
    await asyncio.sleep(0.05)
    await poids_input.type(poids, delay=40)
    await asyncio.sleep(0.2)
    log("✅ Poids rempli")

    await cliquer_etape_suivante(page, "(caractéristiques)")
    await attendre_url(page, "**/parcours/depart**", timeout=15000)
    await asyncio.sleep(0.2)
    await fermer_popups(page)
    log("✅ Étape Départ atteinte")


async def etape_depart(page):
    """Sélectionne le mode d'envoi et passe à /parcours/arrivee."""
    selectors_depart = [
        "label[for='card-input-id-D_BP']",
        "label:has-text('Bureau de poste')",
        "label:has-text('bureau de poste')",
    ]
    await selectionner_tuile(
        page,
        "card-input-id-D_BP",
        selectors_depart,
        description="mode d'envoi bureau de poste",
        timeout=8000,
    )

    await asyncio.sleep(0.3)
    log("✅ Mode d'envoi : bureau de poste")

    if not await verifier_adresse_depart_chargee(page):
        await screenshot_erreur(
            page,
            "depart_expediteur_absent",
            selectors={
                "choisir_adresse_depart": "button:has-text('Choisir une adresse dans le carnet')",
                "etape_suivante": "button:has-text('tape suivante')",
            },
        )
        raise RuntimeError("L'adresse d'expédition n'est pas chargée sur cette session")

    await cliquer_etape_suivante(page, "(départ)")
    try:
        await attendre_url(page, "**/parcours/arrivee**", timeout=15000)
    except RuntimeError:
        if not await verifier_adresse_depart_chargee(page):
            await screenshot_erreur(
                page,
                "depart_expediteur_absent_apres_clic",
                selectors={
                    "choisir_adresse_depart": "button:has-text('Choisir une adresse dans le carnet')",
                    "etape_suivante": "button:has-text('tape suivante')",
                },
            )
            raise RuntimeError("L'adresse d'expédition a disparu au moment de passer à la destination")
        raise
    await asyncio.sleep(0.5)
    await fermer_popups(page)
    log("✅ Étape Arrivée atteinte")


async def etape_arrivee(page, row):
    """Remplit l'adresse destinataire, le téléphone et le mode de livraison."""
    prenom, nom = split_nom(row['Shipping Name'])
    code_pays = normaliser_code_pays(row.get('Shipping Country', 'FR'))

    # ── Ouvrir le modal adresse ──────────────────────────────────────────
    # Essayer plusieurs variantes du bouton
    btn_adresse_selectors = [
        "button:has-text('Renseigner une adresse')",
        "button:has-text('Ajouter une adresse')",
        "button:has-text('adresse')",
    ]
    for sel in btn_adresse_selectors:
        try:
            await attendre_et_cliquer(page, sel, timeout=6000, description="Bouton adresse")
            break
        except RuntimeError:
            continue
    else:
        raise RuntimeError("Bouton d'adresse destinataire introuvable")

    await page.wait_for_selector("#firstName", state="visible", timeout=10000)
    await asyncio.sleep(0.2)
    log("✅ Modal adresse ouvert")

    civilite = await selectionner_civilite(page, "MALE")
    log(f"✅ Civilité : {civilite}")
    await definir_pays_modal(page, code_pays)
    await asyncio.sleep(0.1)

    # Champs adresse
    prenom_val = str(prenom).strip() if not est_nan(prenom) else ""
    nom_val = str(nom).strip() if not est_nan(nom) else ""
    # Si un seul des deux est rempli, s'assurer qu'au moins nom est rempli
    if not nom_val and prenom_val:
        nom_val = prenom_val
        prenom_val = ""
    # Fallback si tout est vide
    if not nom_val:
        nom_val = "Destinataire"

    await remplir_champ_obligatoire(page, "#firstName", prenom_val, "prénom")
    log(f"✅ Prénom : {prenom_val or '(vide)'}")

    await remplir_champ_obligatoire(page, "#lastName", nom_val, "nom")
    log(f"✅ Nom : {nom_val}")

    cp = str(row['Shipping Zip']).strip().replace("'", "")[:10] if not est_nan(row['Shipping Zip']) else ''
    await remplir_champ_obligatoire(page, "#zipCode-addressForm", cp, "code postal")
    log(f"✅ Code postal : {cp}")

    ville = str(row['Shipping City']).strip() if not est_nan(row['Shipping City']) else ''

    adresse1 = str(row['Shipping Address1']).strip() if not est_nan(row['Shipping Address1']) else ''
    if est_destination_france(code_pays):
        await choisir_suggestion_autocomplete(
            page,
            "zipCode-addressForm-listbox",
            [cp, ville],
            description="code postal",
            timeout=3000,
        )
        await asyncio.sleep(0.15)
        city_visible = False
        street_visible = False
        for _ in range(24):
            try:
                city_visible = await page.locator("#city-addressForm").first.is_visible()
            except Exception:
                city_visible = False
            try:
                street_visible = await page.locator("#streetName-addressForm").first.is_visible()
            except Exception:
                street_visible = False
            if city_visible or street_visible:
                break
            await asyncio.sleep(0.1)

        code_postal_adresse = cp
        if city_visible:
            ville_actuelle = ""
            try:
                ville_actuelle = await page.locator("#city-addressForm").first.input_value()
            except Exception:
                ville_actuelle = ""

            if normaliser_valeur_souple(ville_actuelle) == normaliser_valeur_souple(ville):
                log(f"✅ Ville : {ville} (préremplie)")
            else:
                code_ville = await remplir_ville_francaise(page, cp, ville)
                try:
                    code_postal_adresse = await page.locator("#zipCode-addressForm").first.input_value()
                except Exception:
                    code_postal_adresse = cp

                codes_acceptes = {normaliser_valeur_champ(x) for x in codes_postaux_ville_francaise(cp, ville)}
                if code_ville and normaliser_valeur_champ(code_postal_adresse) not in codes_acceptes:
                    raise RuntimeError(
                        f"La sélection de ville a modifié le code postal ({code_postal_adresse})"
                    )

                if code_ville and normaliser_valeur_champ(code_postal_adresse) != normaliser_valeur_champ(cp):
                    log(f"ℹ️ Code ville La Poste : {code_postal_adresse}")
                log(f"✅ Ville : {ville}")
        elif street_visible:
            log("ℹ️ Ville gérée via le code postal")
        else:
            await page.locator("#city-addressForm").first.wait_for(state="visible", timeout=3000)
            code_ville = await remplir_ville_francaise(page, cp, ville)
            if code_ville:
                code_postal_adresse = await page.locator("#zipCode-addressForm").first.input_value()
            log(f"✅ Ville : {ville}")

        await remplir_autocomplete_obligatoire(
            page,
            "#streetName-addressForm",
            adresse1,
            "streetName-addressForm-listbox",
            description="adresse",
            suggestions=[code_postal_adresse, ville, adresse1],
        )
    else:
        await remplir_champ_obligatoire(page, "#city-addressForm", ville, "ville")
        await remplir_champ_obligatoire(page, "#streetName-addressForm", adresse1, "adresse")
        log(f"✅ Ville : {ville}")

    log(f"✅ Adresse : {adresse1}")

    adresse2 = str(row['Shipping Address2']).strip().replace('.0', '') if not est_nan(row['Shipping Address2']) else ''
    if adresse2 and adresse2 not in ('0',):
        await remplir_champ(page, "#additionalStreetNameaddressForm", adresse2, "adresse 2")
        log(f"✅ Adresse 2 : {adresse2}")

    # ── Valider le modal ─────────────────────────────────────────────────
    await stabiliser_formulaire(page, tabs=1)
    await enregistrer_adresse_destinataire(page, row)

    log("✅ Adresse enregistrée")

    await attendre_zone_arrivee_pret(page)

    # ── Mode de livraison ────────────────────────────────────────────────
    await selectionner_mode_livraison_arrivee(page, code_pays)

    # ── Téléphone destinataire ────────────────────────────────────────────
    telephone = str(row['Shipping Phone']).strip() if not est_nan(row['Shipping Phone']) else ''
    email_dest = str(row.get('Email', '')).strip() if not est_nan(row.get('Email', '')) else ''
    if telephone:
        tel_formate = adapter_telephone_pour_pays(telephone, code_pays)
        if est_destination_france(code_pays):
            valide, msg_tel = valider_telephone(tel_formate)
            if not valide:
                log(f"⚠️ Téléphone '{telephone}' → '{tel_formate}' invalide ({msg_tel}), envoi quand même")

        phone_filled = False
        phone_selectors = ["#phone", "input[name='phone']", "input[type='tel']"]
        for sel in phone_selectors:
            try:
                el = page.locator(sel).first
                if await el.count() > 0 and await el.is_visible():
                    ok = await remplir_champ(page, sel, tel_formate, "téléphone")
                    if ok:
                        phone_filled = True
                        log(f"✅ Téléphone : {tel_formate}")
                        break
            except Exception:
                continue

        if not phone_filled:
            ok = await remplir_champ(page, "#phone", tel_formate, "téléphone (retry)")
            if ok:
                log(f"✅ Téléphone : {tel_formate} (après retry)")
            else:
                log("ℹ️ Téléphone non demandé après le choix de livraison")
    else:
        log("ℹ️ Pas de téléphone destinataire")

    if email_dest:
        for sel in ("#email", "input[name='email']", "input[placeholder*='mail']", "input[placeholder*='Email']"):
            try:
                el = page.locator(sel).first
                if await el.count() > 0 and await el.is_visible():
                    ok = await remplir_champ(page, sel, email_dest, "email")
                    if ok:
                        log(f"✅ Email : {email_dest}")
                        break
            except Exception:
                continue

    await stabiliser_formulaire(page, tabs=1)


async def ajouter_au_panier(page, numero):
    log(f"🛒 Ajout au panier du colis {numero}...")

    async def cliquer_ajout():
        bouton = page.locator("button:has-text('Ajouter au panier')").first
        await bouton.wait_for(state="visible", timeout=15000)
        await bouton.scroll_into_view_if_needed()

        for tentative in range(1, 4):
            etat = await lire_etat_bouton(page, "Ajouter au panier")
            if not etat.get("enabled"):
                await stabiliser_formulaire(page, tabs=1)
                await asyncio.sleep(0.25)
                etat = await lire_etat_bouton(page, "Ajouter au panier")

            if tentative == 2 and not etat.get("enabled"):
                await screenshot_erreur(
                    page,
                    f"avant_panier_colis_{numero}_tentative_{tentative}",
                    selectors={
                        "telephone": "#phone",
                        "email": "#email",
                        "ajouter_panier": "button:has-text('Ajouter au panier')",
                    },
                )

            if not etat.get("enabled") and tentative < 3:
                continue

            try:
                await bouton.click(timeout=4000, force=True)
            except Exception:
                log("⚠️ Clic JS fallback sur Ajouter au panier")
                await bouton.evaluate("(node) => node.click()")

            resultat = await attendre_resultat_ajout_panier(page, timeout=12000)
            if resultat == "success":
                return
            if resultat == "address_missing":
                raise RuntimeError("La Poste indique que l'adresse destinataire est incomplete")
            if resultat == "cart_error":
                raise RuntimeError("La Poste a retourne une erreur technique lors de l'ajout au panier")
            if tentative < 3:
                log("⚠️ Clic panier sans effet, nouvelle stabilisation puis retry")
                await stabiliser_formulaire(page, tabs=1)
                await asyncio.sleep(0.4)
                continue

            await screenshot_erreur(
                page,
                f"panier_sans_reponse_colis_{numero}",
                selectors={
                    "telephone": "#phone",
                    "email": "#email",
                    "ajouter_panier": "button:has-text('Ajouter au panier')",
                },
            )
            raise RuntimeError("Ajout au panier non confirme par La Poste")

    try:
        await cliquer_ajout()
    except Exception:
        if "/parcours/arrivee" not in page.url:
            raise

        log("⚠️ Ajout bloqué en boîte aux lettres, essai avec 'Avec signature'")
        await selectionner_tuile(
            page,
            "card-input-id-L_CS",
            [
                "label[for='card-input-id-L_CS']",
                "label:has-text('Avec signature')",
            ],
            description="mode de livraison avec signature",
            timeout=8000,
        )
        await asyncio.sleep(0.5)
        await cliquer_ajout()

    await asyncio.sleep(1)
    log(f"✅ Colis {numero} ajouté au panier !")


async def passer_au_suivant(page, email, mot_de_passe):
    """Revient au formulaire pour le colis suivant."""
    log("🔄 Passage au colis suivant...")
    await retour_formulaire(page, email, mot_de_passe)
    log("✅ Nouveau formulaire prêt")


# ──────────────────────────────────────────────────────
# WORKER
# ──────────────────────────────────────────────────────
async def traiter_colis(page, row, numero, poids, email, mot_de_passe, max_retries=2):
    """Traite un colis avec retry automatique en cas d'erreur."""
    for tentative in range(1, max_retries + 2):
        try:
            log(f"📦 Colis {numero} — {row['Shipping Name']} ({row['Name']})" +
                (f" [tentative {tentative}]" if tentative > 1 else ""))

            code_pays = normaliser_code_pays(row.get('Shipping Country', 'FR'))
            await etape_caracteristiques(page, poids, code_pays)
            await etape_depart(page)
            await etape_arrivee(page, row)
            await remplir_declaration_douane(page, row, poids)
            await ajouter_au_panier(page, numero)
            return True

        except Exception as e:
            log(f"❌ Erreur colis {numero} (tentative {tentative}) : {e}")
            await screenshot_erreur(page, f"erreur_colis_{numero}_tentative_{tentative}")

            if tentative <= max_retries:
                log(f"🔁 Nouvelle tentative dans 3s...")
                await asyncio.sleep(3)
                try:
                    await retour_formulaire(page, email, mot_de_passe)
                except Exception as retry_err:
                    log(f"⚠️ Erreur retour formulaire : {retry_err}")
            else:
                log(f"💀 Colis {numero} abandonné après {max_retries + 1} tentatives")
                return False


async def run_bot(filepath, email, mot_de_passe, poids):
    global worker_running, stop_requested
    browser = None

    try:
        log(f"🚀 Démarrage du bot v{VERSION}")
        commandes = charger_commandes_depuis_fichier(filepath)
        log(f"📬 {len(commandes)} commande(s) à traiter")

        if len(commandes) == 0:
            log("⚠️ Aucune commande à traiter (toutes déjà fulfilled ?)")
            return

        async with async_playwright() as p:
            browser = await p.chromium.launch_persistent_context(
                user_data_dir=str(PROFIL),
                headless=False,
                args=["--disable-blink-features=AutomationControlled"],
            )
            log("✅ Navigateur lancé")
            page = browser.pages[0] if browser.pages else await browser.new_page()

            # Timeout global par défaut
            page.set_default_timeout(15000)

            # Connexion initiale
            await page.goto("https://www.laposte.fr/colissimo-en-ligne", wait_until="domcontentloaded")
            await asyncio.sleep(1)
            await fermer_popups(page)
            await connecter(page, email, mot_de_passe)

            # Ouvrir le parcours
            await page.goto("https://www.laposte.fr/colissimo-en-ligne", wait_until="domcontentloaded")
            await asyncio.sleep(1)
            await fermer_popups(page)
            await attendre_et_cliquer(page, "button:has-text('Envoyez votre colis')", timeout=12000, description="Envoyez votre colis")
            await attendre_url(page, "**/parcours/caracteristiques**", timeout=15000)
            await asyncio.sleep(0.8)
            await fermer_popups(page)
            await page.wait_for_selector("#weightInput", state="visible", timeout=10000)
            log("✅ Parcours Colissimo ouvert")

            succes = 0
            echecs = 0

            for i, row in commandes.iterrows():
                if stop_requested:
                    log("🛑 Arrêt demandé par l'utilisateur")
                    break

                log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
                log(f"🚀 Colis {i + 1}/{len(commandes)}")
                log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

                ok = await traiter_colis(page, row, i + 1, poids, email, mot_de_passe)

                if ok:
                    succes += 1
                else:
                    echecs += 1

                # Passer au colis suivant sauf pour le dernier
                if ok and i < len(commandes) - 1 and not stop_requested:
                    await passer_au_suivant(page, email, mot_de_passe)
                elif not ok and i < len(commandes) - 1 and not stop_requested:
                    # Après un échec, on doit retourner au formulaire
                    try:
                        await retour_formulaire(page, email, mot_de_passe)
                    except Exception:
                        log("⚠️ Impossible de revenir au formulaire après échec")

            log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            log(f"✅ Terminé : {succes} ajouté(s) au panier, {echecs} échec(s)")
            if echecs > 0:
                log(f"⚠️ Screenshots des erreurs dans 'screenshots_erreurs/'")
            log("👉 Finalise le paiement dans le navigateur")

    except Exception as e:
        log(f"❌ Erreur fatale : {e}")
        import traceback
        log(f"📋 {traceback.format_exc()[:500]}")
    finally:
        if browser is not None:
            try:
                await browser.close()
                log("🧹 Navigateur fermé proprement")
            except Exception as close_err:
                if "has been closed" not in str(close_err):
                    log(f"⚠️ Fermeture navigateur impossible : {close_err}")
        worker_running = False
        stop_requested = False
        log("🏁 Fin du traitement")


def worker_runner(filepath, email, mot_de_passe, poids):
    try:
        asyncio.run(run_bot(filepath, email, mot_de_passe, poids))
    except Exception as e:
        log(f"❌ Erreur thread : {e}")


# ──────────────────────────────────────────────────────
# ROUTES FLASK
# ──────────────────────────────────────────────────────
@app.route("/", methods=["GET"])
def index():
    global last_uploaded_file, worker_running
    return render_template_string(
        PAGE_HTML,
        fichier=Path(last_uploaded_file).name if last_uploaded_file else None,
        email=EMAIL_PAR_DEFAUT,
        mot_de_passe=MOT_DE_PASSE_PAR_DEFAUT,
        poids=POIDS_PAR_DEFAUT,
        running=worker_running,
        version=VERSION
    )


@app.route("/upload", methods=["POST"])
def upload():
    global last_uploaded_file
    if "file" not in request.files:
        return redirect(url_for("index"))
    f = request.files["file"]
    if not f or f.filename == "":
        return redirect(url_for("index"))
    ext = Path(f.filename).suffix.lower()
    if ext not in [".csv", ".xlsx", ".xls"]:
        log("❌ Format refusé. Utilise CSV, XLSX ou XLS.")
        return redirect(url_for("index"))

    filepath = UPLOAD_DIR / f"import_{int(time.time())}{ext}"
    f.save(filepath)
    last_uploaded_file = str(filepath)
    log(f"✅ Fichier importé : {filepath.name}")

    try:
        df = charger_commandes_depuis_fichier(str(filepath))
        log(f"✅ {len(df)} commande(s) détectée(s)")
        # Afficher un aperçu rapide
        for _, row in df.iterrows():
            tel = str(row['Shipping Phone']).strip() if not est_nan(row['Shipping Phone']) else ''
            if tel:
                tel_f = formater_telephone(tel)
                valide, msg = valider_telephone(tel_f)
                status = "✅" if valide else f"⚠️ {msg}"
            else:
                tel_f = "(aucun)"
                status = "ℹ️"
            log(f"   {row['Name']} — {row['Shipping Name']} — {row['Shipping City']} — Tél: {tel_f} {status}")
    except Exception as e:
        log(f"❌ Erreur lecture fichier : {e}")

    return redirect(url_for("index"))


@app.route("/start", methods=["POST"])
def start():
    global worker_thread, worker_running, last_uploaded_file

    if worker_running:
        log("⚠️ Un traitement est déjà en cours")
        return redirect(url_for("index"))
    if not last_uploaded_file:
        log("❌ Aucun fichier importé")
        return redirect(url_for("index"))

    email = request.form.get("email", EMAIL_PAR_DEFAUT).strip()
    mot_de_passe = request.form.get("mot_de_passe", MOT_DE_PASSE_PAR_DEFAUT).strip()
    poids = request.form.get("poids", POIDS_PAR_DEFAUT).strip()

    log(f"📧 Email : {email} | ⚖️ Poids : {poids} kg")
    log(f"📄 Fichier : {last_uploaded_file}")

    worker_running = True
    worker_thread = threading.Thread(
        target=worker_runner,
        args=(last_uploaded_file, email, mot_de_passe, poids),
        daemon=True
    )
    worker_thread.start()
    log("🚀 Bot lancé")

    return redirect(url_for("index"))


@app.route("/stop", methods=["POST"])
def stop():
    global stop_requested
    if worker_running:
        stop_requested = True
        log("🛑 Arrêt demandé — le bot s'arrêtera après le colis en cours")
    return redirect(url_for("index"))


@app.route("/logs", methods=["GET"])
def logs_route():
    temp = []
    while not log_queue.empty():
        try:
            temp.append(log_queue.get_nowait())
        except queue.Empty:
            break

    log_history_path = DATA_DIR / "log_history.txt"
    old_logs = []
    if log_history_path.exists():
        try:
            old_logs = log_history_path.read_text(encoding="utf-8").splitlines()
        except Exception:
            old_logs = []

    if temp:
        old_logs.extend(temp)
        old_logs = old_logs[-500:]
        try:
            log_history_path.write_text("\n".join(old_logs), encoding="utf-8")
        except Exception:
            pass

    return jsonify({"logs": old_logs[-500:]})


def open_browser():
    webbrowser.open(f"http://{HOST}:{PORT}")


if __name__ == "__main__":
    try:
        if "--smoke-test" in sys.argv:
            print(f"SMOKE_OK version={VERSION}", flush=True)
            print(f"APP_DIR={APP_DIR}", flush=True)
            print(f"DATA_DIR={DATA_DIR}", flush=True)
            sys.exit(0)

        if "--browser-smoke-test" in sys.argv:
            asyncio.run(browser_smoke_test())
            sys.exit(0)

        print(f"➡️ La Poste Bot v{VERSION}", flush=True)
        print(f"  Dossier app : {APP_DIR}", flush=True)
        print(f"  Dossier donnees : {DATA_DIR}", flush=True)

        # Vérifier / installer Chromium au premier lancement
        if not navigateur_installe():
            if getattr(sys, 'frozen', False) and SYSTEME == "Darwin":
                afficher_alerte(
                    "Premier lancement : Chromium va être téléchargé automatiquement. Cela peut prendre 1 à 2 minutes.",
                    titre=APP_NAME,
                )
            ok = installer_navigateur()
            if not ok:
                print("", flush=True)
                print("❌ Impossible d'installer Chromium automatiquement.", flush=True)
                print("   Ferme l'application puis réessaie avec une connexion internet active.", flush=True)
                if getattr(sys, 'frozen', False) and SYSTEME == "Darwin":
                    afficher_alerte(
                        "Impossible d'installer Chromium automatiquement. Fermez l'application puis réessayez avec une connexion internet active.",
                        titre=APP_NAME,
                    )
                else:
                    input("Appuie sur Entree pour fermer...")
                sys.exit(1)
        else:
            print("  Chromium : OK", flush=True)

        threading.Timer(1.5, open_browser).start()
        print(f"  Serveur : http://{HOST}:{PORT}", flush=True)
        app.run(host=HOST, port=PORT, debug=False)
    except Exception as e:
        print(f"❌ Erreur au démarrage : {e}", flush=True)
        if "--smoke-test" in sys.argv or "--browser-smoke-test" in sys.argv:
            sys.exit(1)
        if getattr(sys, 'frozen', False) and SYSTEME == "Darwin":
            afficher_alerte(f"Erreur au démarrage : {e}", titre=APP_NAME)
        else:
            input("Appuie sur Entree pour fermer...")
        sys.exit(1)
