"""
PokéFolio — Scraper intelligent v2
====================================
API : cardmarket-api-tcg.p.rapidapi.com
Budget : 60 appels/jour

Stratégie :
  Appel 1  → GET /pokemon/episodes (liste toutes les éditions)
  Appels 2+ → GET /pokemon/episodes/{id}/products (produits scellés)

1 appel par édition = tous ses produits scellés avec prix FR, 7j, 30j
"""

import os
import json
import time
import requests
from datetime import date, datetime
from supabase import create_client

# ============================================================
#  Configuration
# ============================================================
RAPIDAPI_KEY  = os.environ.get("TCGGO_API_KEY", "")
RAPIDAPI_HOST = "cardmarket-api-tcg.p.rapidapi.com"
API_BASE      = f"https://{RAPIDAPI_HOST}"

SUPABASE_URL  = os.environ.get("SUPABASE_URL_STATS", "")
SUPABASE_KEY  = os.environ.get("SUPABASE_KEY_STATS", "")

BUDGET_TOTAL = 30
DELAI_APPELS = 2

HEADERS = {
    "Content-Type":    "application/json",
    "x-rapidapi-host": RAPIDAPI_HOST,
    "x-rapidapi-key":  RAPIDAPI_KEY,
}

# ============================================================
#  Mapping EN → FR
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
#  Helpers
# ============================================================
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

def detecter_type_produit(nom):
    n = nom.lower()
    if "display" in n:                               return "display_36"
    elif "elite trainer box" in n or "etb" in n:     return "etb"
    elif "ultra-premium" in n or "ultra premium" in n: return "coffret"
    elif "bundle" in n:                              return "bundle"
    elif "tin" in n:                                 return "tin"
    elif "blister" in n or "tripack" in n:           return "blister"
    elif "booster" in n:                             return "booster_unitaire"
    elif "collection" in n:                          return "coffret"
    else:                                            return "autre"

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
            headers=HEADERS,
            params=params or {},
            timeout=15
        )
        r.raise_for_status()
        appels_effectues += 1
        time.sleep(DELAI_APPELS)
        return r.json()
    except Exception as e:
        print(f"  Erreur API {endpoint}: {e}")
        appels_effectues += 1
        return None

# ============================================================
#  Supabase
# ============================================================
def get_sb():
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def charger_etat(sb):
    try:
        r = sb.table("scraper_etat").select("*").eq("id", 1).execute()
        if r.data:
            etat = r.data[0]
            if etat.get("date_compteur") != str(date.today()):
                etat["appels_aujourd_hui"] = 0
                etat["date_compteur"]      = str(date.today())
                print("  Nouveau jour — compteur remis à zéro")
            if isinstance(etat.get("dernieres_maj"), str):
                etat["dernieres_maj"] = json.loads(etat["dernieres_maj"])
            elif not etat.get("dernieres_maj"):
                etat["dernieres_maj"] = {}
            return etat
    except Exception as e:
        print(f"  Erreur chargement état: {e}")
    return {
        "id": 1, "appels_aujourd_hui": 0,
        "date_compteur": str(date.today()), "dernieres_maj": {},
    }

def sauvegarder_etat(sb, etat):
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

def get_dernieres_maj(sb):
    """Dernière MAJ par episode_id depuis prix_historique."""
    try:
        r = sb.table("prix_historique").select(
            "episode_id, date_releve"
        ).order("date_releve", desc=True).execute()
        maj = {}
        for row in (r.data or []):
            eid = str(row.get("episode_id", ""))
            if eid and eid not in maj:
                maj[eid] = row["date_releve"]
        return maj
    except:
        return {}

def inserer_produits_episode(sb, episode_api, produits):
    """Insère tous les produits d'une édition et leurs prix."""
    nom_en    = episode_api.get("name", "")
    serie_en  = (episode_api.get("series") or {}).get("name", "")
    episode_id = episode_api["id"]
    nb_ok = 0

    for prod in produits:
        cm_id = prod.get("cardmarket_id")
        if not cm_id:
            continue

        prix = prod.get("prices", {}).get("cardmarket", {})
        prix_fr  = prix.get("lowest_FR")
        avg_30j  = prix.get("30d_average")
        avg_7j   = prix.get("7d_average")
        prix_min = prix.get("lowest")

        # Upsert produit
        try:
            sb.table("produits_catalogue").upsert({
                "cardmarket_id":    cm_id,
                "episode_id":       episode_id,
                "nom_fr":           prod.get("name", ""),
                "edition_code":     episode_api.get("code", ""),
                "edition_nom":      NOMS_FR.get(nom_en, nom_en),
                "serie":            SERIES_FR.get(serie_en, serie_en),
                "type_produit":     detecter_type_produit(prod.get("name", "")),
                "langue":           "FR",
                "date_sortie_fr":   episode_api.get("released_at"),
                "print_run_status": get_print_status(episode_api.get("released_at", "")),
                "slug":             prod.get("slug", ""),
            }).execute()
        except Exception as e:
            print(f"    Erreur upsert produit {cm_id}: {e}")
            continue

        # Insérer prix si disponible
        if prix_fr or avg_30j:
            try:
                sb.table("prix_historique").upsert({
                    "cardmarket_id": cm_id,
                    "episode_id":    episode_id,
                    "date_releve":   str(date.today()),
                    "prix_median":   avg_30j or prix_fr,
                    "prix_min":      prix_min,
                    "prix_fr":       prix_fr,
                    "avg_7j":        avg_7j,
                    "avg_30j":       avg_30j,
                    "source":        "rapidapi_tcggo",
                }).execute()
                nb_ok += 1
            except Exception as e:
                print(f"    Erreur prix {cm_id}: {e}")

    return nb_ok

# ============================================================
#  Priorités par édition
# ============================================================
def score_edition(episode, dernieres_maj):
    eid      = str(episode["id"])
    derniere = dernieres_maj.get(eid)
    score    = 0

    if not derniere:
        score += 100
    else:
        try:
            jours = (date.today() - date.fromisoformat(derniere)).days
            if jours == 0:   return -1  # Déjà faite aujourd'hui
            elif jours > 7:  score += 50
            elif jours > 3:  score += 25
            else:            score += 10
        except:
            score += 100

    status = get_print_status(episode.get("released_at", ""))
    if status == "en_impression":   score += 30
    elif status == "arret_annonce": score += 20
    else:                           score += 5

    if episode.get("prices", {}).get("cardmarket", {}).get("total", 0) > 0:
        score += 10

    return score

# ============================================================
#  Main
# ============================================================
def main():
    global appels_effectues

    print("=" * 55)
    print(f"PokéFolio Scraper v2 — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("API : cardmarket-api-tcg.p.rapidapi.com")
    print("=" * 55)

    if not RAPIDAPI_KEY or not SUPABASE_URL:
        print("ERREUR : Variables d'environnement manquantes")
        return

    sb   = get_sb()
    etat = charger_etat(sb)
    appels_effectues = etat["appels_aujourd_hui"]

    budget = BUDGET_TOTAL - appels_effectues
    print(f"Budget : {budget}/{BUDGET_TOTAL} appels disponibles")

    if budget <= 1:
        print("Budget épuisé.")
        return

    # ---- Étape 1 : Liste des éditions (1 appel) ----
    print(f"\n[1/3] Liste des éditions...")
    data = appel_api("pokemon/episodes")
    if not data:
        print("Erreur API — arrêt")
        return

    episodes = [e for e in data.get("data", []) if e.get("id") and e.get("name")]
    print(f"  {len(episodes)} éditions trouvées")

    # ---- Étape 2 : Priorités ----
    print(f"\n[2/3] Calcul des priorités...")
    dernieres_maj = get_dernieres_maj(sb)

    episodes_tries = sorted(episodes, key=lambda e: score_edition(e, dernieres_maj), reverse=True)

    print("  Top 5 :")
    for ep in episodes_tries[:5]:
        s   = score_edition(ep, dernieres_maj)
        nom = NOMS_FR.get(ep["name"], ep["name"])
        maj = dernieres_maj.get(str(ep["id"]), "jamais")
        print(f"    [{s:3}pts] {nom[:35]:35} — MAJ: {maj}")

    # ---- Étape 3 : Scraping ----
    budget = BUDGET_TOTAL - appels_effectues
    print(f"\n[3/3] Scraping ({budget - 1} appels dispo)...")

    ep_ok = ep_skip = ep_error = 0

    for episode in episodes_tries:
        if appels_effectues >= BUDGET_TOTAL - 1:
            print(f"  Budget atteint — {ep_ok} éditions traitées")
            break

        s   = score_edition(episode, dernieres_maj)
        nom = NOMS_FR.get(episode["name"], episode["name"])

        if s < 0:
            ep_skip += 1
            continue
        if s <= 3:
            ep_skip += 1
            continue

        print(f"  [{s:3}pts] {nom[:40]}", end=" → ")

        data_prod = appel_api(f"pokemon/episodes/{episode['id']}/products",
                              {"sort": "price_highest"})
        if not data_prod:
            print("erreur API")
            ep_error += 1
            continue

        produits = [p for p in data_prod.get("data", []) if p.get("cardmarket_id")]
        if not produits:
            print("aucun produit")
            ep_skip += 1
            continue

        nb = inserer_produits_episode(sb, episode, produits)
        dernieres_maj[str(episode["id"])] = str(date.today())
        print(f"{nb}/{len(produits)} prix insérés ✓ ({appels_effectues}/{BUDGET_TOTAL})")
        ep_ok += 1

    # ---- Résumé ----
    print(f"\n{'='*55}")
    print(f"  Éditions traitées : {ep_ok}")
    print(f"  Ignorées          : {ep_skip}")
    print(f"  Erreurs           : {ep_error}")
    print(f"  Appels utilisés   : {appels_effectues}/{BUDGET_TOTAL}")

    etat["appels_aujourd_hui"] = appels_effectues
    etat["date_compteur"]      = str(date.today())
    etat["dernieres_maj"]      = dernieres_maj
    sauvegarder_etat(sb, etat)
    print("État sauvegardé ✓")

if __name__ == "__main__":
    main()
