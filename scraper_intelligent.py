"""
PokéFolio — Scraper intelligent (version Render.com)
=====================================================
Différence vs version locale :
  - L'état est stocké dans Supabase (table scraper_etat)
    car le filesystem Render est éphémère
  - Variables d'environnement via Render dashboard

Budget : 60 appels/jour
  1 appel  → liste des éditions
  59 appels → mise à jour des prix par priorité
"""

import os
import json
import time
import requests
from datetime import date, datetime, timedelta
from supabase import create_client

# ============================================================
#  Configuration
# ============================================================
API_BASE     = "https://api.tcggo.com/v1"
API_KEY      = os.environ.get("TCGGO_API_KEY", "")
SUPABASE_URL = os.environ.get("SUPABASE_URL_STATS", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY_STATS", "")

BUDGET_TOTAL     = 60
BUDGET_CATALOGUE = 1
BUDGET_PRIX      = BUDGET_TOTAL - BUDGET_CATALOGUE
DELAI_APPELS     = 2  # secondes entre chaque appel

# ============================================================
#  Mapping noms EN → FR
# ============================================================
NOMS_FR = {
    "Mega Evolution": "Méga-Évolution", "Phantasmal Flames": "Flammes Fantasmagoriques",
    "Ascended Heroes": "Héros Ascendants", "Perfect Order": "Ordre Parfait",
    "Chaos Rising": "Chaos Montant", "Paldea Evolved": "Évolutions à Paldea",
    "Obsidian Flames": "Flammes Obsidiennes", "151": "Écarlate et Violet 151",
    "Paradox Rift": "Faille Paradoxe", "Paldean Fates": "Destinée de Paldea",
    "Temporal Forces": "Forces Temporelles", "Twilight Masquerade": "Mascarade Crépusculaire",
    "Shrouded Fable": "Destins de Paldea", "Stellar Crown": "Couronne Stellaire",
    "Surging Sparks": "Tempête Argentée", "Prismatic Evolutions": "Évolutions Prismatiques",
    "Journey Together": "Aventures Ensemble", "Destined Rivals": "Rivalités Destinées",
    "Black Bolt": "Foudre Noire", "White Flare": "Flamme Blanche",
    "Sword & Shield": "Épée et Bouclier", "Rebel Clash": "Clash des Rebelles",
    "Darkness Ablaze": "Ténèbres Embrasées", "Champion's Path": "La Voie du Maître",
    "Vivid Voltage": "Chants du Tonnerre", "Shining Fates": "Shining Fates",
    "Battle Styles": "Styles de Combat", "Chilling Reign": "Règne de Glace",
    "Evolving Skies": "Évolution Céleste", "Fusion Strike": "Poing de Fusion",
    "Brilliant Stars": "Étoiles Brillantes", "Astral Radiance": "Astres Radieux",
    "Lost Origin": "Origine Perdue", "Silver Tempest": "Tempête Argentée",
    "Crown Zenith": "Zénith Suprême",
}

SERIES_FR = {
    "Scarlet & Violet": "Écarlate-Violet",
    "Sword & Shield":   "Épée-Bouclier",
    "Mega Evolution":   "Méga-Évolution",
    "Sun & Moon":       "Soleil-Lune",
    "XY":               "XY",
}

# ============================================================
#  Supabase — état persisté en base
# ============================================================
def get_sb():
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def charger_etat(sb):
    """Charge l'état depuis la table scraper_etat en Supabase."""
    try:
        r = sb.table("scraper_etat").select("*").eq("id", 1).execute()
        if r.data:
            etat = r.data[0]
            # Reset compteur si nouveau jour
            if etat.get("date_compteur") != str(date.today()):
                etat["appels_aujourd_hui"] = 0
                etat["date_compteur"]      = str(date.today())
                print("  Nouveau jour — compteur remis à zéro")
            return etat
    except Exception as e:
        print(f"  Erreur chargement état: {e}")

    # État initial
    return {
        "id":                 1,
        "appels_aujourd_hui": 0,
        "date_compteur":      str(date.today()),
        "dernieres_maj":      {},
    }

def sauvegarder_etat(sb, etat):
    """Sauvegarde l'état dans Supabase."""
    try:
        sb.table("scraper_etat").upsert({
            "id":                 1,
            "appels_aujourd_hui": etat["appels_aujourd_hui"],
            "date_compteur":      etat["date_compteur"],
            "dernieres_maj":      json.dumps(etat.get("dernieres_maj", {})),
            "derniere_execution": datetime.now().isoformat(),
        }).execute()
    except Exception as e:
        print(f"  Erreur sauvegarde état: {e}")

def get_produits_en_base(sb):
    """Récupère tous les produits de la base stats."""
    r = sb.table("produits_catalogue").select(
        "cardmarket_id, nom_fr, print_run_status, type_produit"
    ).execute()
    return {str(p["cardmarket_id"]): p for p in (r.data or [])}

def get_dernieres_maj(sb):
    """Récupère la date de dernière MAJ par produit."""
    r = sb.table("prix_historique").select(
        "cardmarket_id, date_releve"
    ).order("date_releve", desc=True).execute()

    maj = {}
    for row in (r.data or []):
        cid = str(row["cardmarket_id"])
        if cid not in maj:
            maj[cid] = row["date_releve"]
    return maj

# ============================================================
#  Priorités
# ============================================================
def calculer_priorite(produit, dernieres_maj):
    score   = 0
    cid_str = str(produit.get("cardmarket_id"))
    derniere_maj = dernieres_maj.get(cid_str)

    if not derniere_maj:
        score += 100
    else:
        try:
            jours = (date.today() - date.fromisoformat(derniere_maj)).days
            if jours > 7:   score += 50
            elif jours > 3: score += 25
            elif jours > 1: score += 10
            elif jours == 0: return -1  # Déjà mis à jour aujourd'hui
        except:
            score += 100

    status = produit.get("print_run_status", "arrete")
    if status == "en_impression":   score += 30
    elif status == "arret_annonce": score += 20
    elif status == "arrete":        score += 5

    type_p = produit.get("type_produit", "")
    if type_p in ("display_36", "display_18", "etb"):
        score += 15

    return score

def trier_par_priorite(produits, dernieres_maj):
    return sorted(
        produits,
        key=lambda p: calculer_priorite(p, dernieres_maj),
        reverse=True
    )

# ============================================================
#  Appels API
# ============================================================
appels_effectues = 0

def appel_api(endpoint, params=None):
    global appels_effectues
    if appels_effectues >= BUDGET_TOTAL:
        raise Exception("Budget épuisé")
    try:
        r = requests.get(
            f"{API_BASE}/{endpoint}",
            params=params or {},
            headers={"Authorization": f"Bearer {API_KEY}"},
            timeout=15
        )
        r.raise_for_status()
        appels_effectues += 1
        time.sleep(DELAI_APPELS)
        return r.json()
    except Exception as e:
        print(f"  Erreur API {endpoint}: {e}")
        return None

def get_print_status(released_at_str):
    if not released_at_str:
        return "en_impression"
    try:
        released = date.fromisoformat(released_at_str)
        diff = (date.today() - released).days
        if released > date.today(): return "en_impression"
        elif diff < 180:            return "en_impression"
        elif diff < 365:            return "arret_annonce"
        else:                       return "arrete"
    except:
        return "en_impression"

def inserer_edition(sb, ed):
    nom_en   = ed.get("name", "")
    series   = ed.get("series") or {}
    serie_en = series.get("name", "")
    sb.table("produits_catalogue").upsert({
        "cardmarket_id":    ed["id"],
        "nom_fr":           NOMS_FR.get(nom_en, nom_en),
        "edition_code":     ed.get("code", ""),
        "edition_nom":      NOMS_FR.get(nom_en, nom_en),
        "serie":            SERIES_FR.get(serie_en, serie_en),
        "type_produit":     "display_36",
        "langue":           "FR",
        "date_sortie_fr":   ed.get("released_at"),
        "print_run_status": get_print_status(ed.get("released_at", "")),
    }).execute()
    print(f"  + Nouvelle édition : {nom_en} (id: {ed['id']})")

def inserer_prix(sb, cid, prix_total):
    if not prix_total:
        return False
    sb.table("prix_historique").upsert({
        "cardmarket_id": cid,
        "date_releve":   str(date.today()),
        "prix_median":   prix_total,
        "source":        "tcggo_api",
    }).execute()
    try:
        sb.rpc("calculer_tendances", {"p_cardmarket_id": cid}).execute()
    except:
        pass
    return True

# ============================================================
#  Main
# ============================================================
def main():
    global appels_effectues

    print("=" * 55)
    print(f"PokéFolio Scraper — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 55)

    if not API_KEY or not SUPABASE_URL:
        print("ERREUR : Variables d'environnement manquantes")
        print("  TCGGO_API_KEY, SUPABASE_URL_STATS, SUPABASE_KEY_STATS")
        return

    sb    = get_sb()
    etat  = charger_etat(sb)
    appels_effectues = etat["appels_aujourd_hui"]

    budget_restant = BUDGET_TOTAL - appels_effectues
    print(f"Budget : {budget_restant}/{BUDGET_TOTAL} appels disponibles")

    if budget_restant <= 1:
        print("Budget épuisé pour aujourd'hui.")
        return

    # ---- ÉTAPE 1 : Nouvelles éditions ----
    print(f"\n[1/3] Détection nouvelles éditions...")
    data = appel_api("sets", {"game": "pokemon", "per_page": 100})
    editions_api  = data.get("data", []) if data else []
    produits_base = get_produits_en_base(sb)
    ids_base      = {int(k) for k in produits_base.keys()}

    nouveaux = [ed for ed in editions_api if ed["id"] not in ids_base and ed.get("name")]
    if nouveaux:
        print(f"  {len(nouveaux)} nouvelle(s) édition(s)")
        for ed in nouveaux:
            inserer_edition(sb, ed)
        produits_base = get_produits_en_base(sb)
    else:
        print(f"  Aucune nouveauté — {len(ids_base)} éditions en base")

    # ---- ÉTAPE 2 : Priorités ----
    print(f"\n[2/3] Calcul des priorités...")
    dernieres_maj  = get_dernieres_maj(sb)
    produits_tries = trier_par_priorite(list(produits_base.values()), dernieres_maj)

    print(f"  Top 5 priorités :")
    for p in produits_tries[:5]:
        cid  = str(p["cardmarket_id"])
        last = dernieres_maj.get(cid, "jamais")
        pts  = calculer_priorite(p, dernieres_maj)
        print(f"    [{pts:3}pts] {p.get('nom_fr','?')[:35]} — MAJ: {last}")

    # ---- ÉTAPE 3 : Mise à jour prix ----
    budget_restant = BUDGET_TOTAL - appels_effectues
    print(f"\n[3/3] Mise à jour des prix ({budget_restant - 1} appels dispo)...")

    maj_ok = maj_skip = maj_error = 0

    for produit in produits_tries:
        if appels_effectues >= BUDGET_TOTAL - 1:
            print(f"  Budget atteint — arrêt")
            break

        cid   = produit["cardmarket_id"]
        nom   = produit.get("nom_fr", f"ID {cid}")
        score = calculer_priorite(produit, dernieres_maj)

        # Déjà mis à jour aujourd'hui ou priorité trop faible
        if score < 0:
            maj_skip += 1
            continue
        if score <= 3:
            print(f"  Skip (score faible) : {nom[:40]}")
            maj_skip += 1
            continue

        print(f"  [{score:3}pts] {nom[:40]}", end=" → ")
        data = appel_api(f"sets/{cid}")
        if not data:
            print("erreur API")
            maj_error += 1
            continue

        item       = data.get("data", data)
        prix_total = item.get("prices", {}).get("cardmarket", {}).get("total", 0)

        if inserer_prix(sb, cid, prix_total):
            dernieres_maj[str(cid)] = str(date.today())
            print(f"{prix_total} € ✓")
            maj_ok += 1
        else:
            print("prix vide")
            maj_error += 1

    # ---- Résumé ----
    print(f"\n{'='*55}")
    print(f"  Nouvelles éditions : {len(nouveaux)}")
    print(f"  Prix mis à jour    : {maj_ok}")
    print(f"  Ignorés            : {maj_skip}")
    print(f"  Erreurs            : {maj_error}")
    print(f"  Appels utilisés    : {appels_effectues}/{BUDGET_TOTAL}")

    # Sauvegarder
    etat["appels_aujourd_hui"] = appels_effectues
    etat["date_compteur"]      = str(date.today())
    etat["dernieres_maj"]      = dernieres_maj
    sauvegarder_etat(sb, etat)
    print("État sauvegardé dans Supabase ✓")

if __name__ == "__main__":
    main()
