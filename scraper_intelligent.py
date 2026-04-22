"""
PokéFolio — Scraper intelligent v4
====================================
API : cardmarket-api-tcg.p.rapidapi.com
Budget : 60 appels/jour

Règles fondamentales :
  - /episodes sert UNIQUEMENT à détecter les nouvelles éditions
  - On n'insère dans prix_historique QUE si prix_fr est présent
  - La priorité est basée sur les éditions sans prix_fr récent
  - Une ligne sans prix_fr = donnée inutile, on ne l'insère pas
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

BUDGET_TOTAL = 40
DELAI_APPELS = 6

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
    "Scarlet & Violet": "Écarlate-Violet", "Sword & Shield": "Épée-Bouclier",
    "Mega Evolution": "Méga-Évolution", "Sun & Moon": "Soleil-Lune", "XY": "XY",
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

def detecter_type(nom):
    n = nom.lower()
    if "display" in n:                                 return "display_36"
    elif "elite trainer box" in n or "etb" in n:       return "etb"
    elif "ultra-premium" in n or "ultra premium" in n: return "coffret"
    elif "bundle" in n:                                return "bundle"
    elif "tin" in n:                                   return "tin"
    elif "blister" in n or "tripack" in n:             return "blister"
    elif "booster" in n:                               return "booster_unitaire"
    elif "collection" in n:                            return "coffret"
    else:                                              return "autre"

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

def get_episodes_en_base(sb):
    """Retourne l'ensemble des episode_id déjà connus en base."""
    try:
        r = sb.table("produits_catalogue").select("episode_id").execute()
        return {str(p["episode_id"]) for p in (r.data or []) if p.get("episode_id")}
    except:
        return set()

def get_dernieres_maj_avec_prix_fr(sb):
    """
    Retourne la date de dernière MAJ par episode_id
    UNIQUEMENT pour les lignes ayant un prix_fr non nul.
    Une ligne sans prix_fr ne compte pas comme une vraie MAJ.
    """
    try:
        r = sb.table("prix_historique").select(
            "episode_id, date_releve"
        ).not_.is_("prix_fr", "null"
        ).order("date_releve", desc=True).execute()

        maj = {}
        for row in (r.data or []):
            eid = str(row.get("episode_id", ""))
            if eid and eid not in maj:
                maj[eid] = row["date_releve"]
        return maj
    except Exception as e:
        print(f"  Erreur lecture dernieres_maj: {e}")
        return {}

def upsert_produit(sb, data):
    """Upsert propre — update si existe, insert sinon."""
    cm_id = data["cardmarket_id"]
    try:
        existant = sb.table("produits_catalogue").select(
            "cardmarket_id"
        ).eq("cardmarket_id", cm_id).execute()
        if existant.data:
            sb.table("produits_catalogue").update(data).eq("cardmarket_id", cm_id).execute()
        else:
            sb.table("produits_catalogue").insert(data).execute()
        return True
    except Exception as e:
        print(f"    Erreur produit {cm_id}: {e}")
        return False

def inserer_produits_episode(sb, episode, produits):
    """
    Insère les produits d'une édition dans prix_historique.
    RÈGLE FONDAMENTALE : on n'insère QUE si prix_fr est présent.
    Sans prix_fr = donnée inutile qui fausse les tendances.
    """
    nom_en     = episode.get("name", "")
    serie_en   = (episode.get("series") or {}).get("name", "")
    episode_id = episode["id"]
    nb_ok      = 0
    nb_skip    = 0

    for prod in produits:
        cm_id = prod.get("cardmarket_id")
        if not cm_id:
            continue

        prix    = prod.get("prices", {}).get("cardmarket", {})
        prix_fr = prix.get("lowest_FR")
        avg_7j  = prix.get("7d_average")
        avg_30j = prix.get("30d_average")

        # RÈGLE CRITIQUE : skip si pas de prix_fr
        if not prix_fr:
            nb_skip += 1
            continue

        # Upsert dans produits_catalogue
        ok = upsert_produit(sb, {
            "cardmarket_id":    cm_id,
            "episode_id":       episode_id,
            "nom_fr":           prod.get("name", ""),
            "edition_code":     episode.get("code", ""),
            "edition_nom":      NOMS_FR.get(nom_en, nom_en),
            "serie":            SERIES_FR.get(serie_en, serie_en),
            "type_produit":     detecter_type(prod.get("name", "")),
            "langue":           "FR",
            "date_sortie_fr":   episode.get("released_at"),
            "print_run_status": get_print_status(episode.get("released_at", "")),
            "slug":             prod.get("slug", ""),
        })

        if not ok:
            continue

        # Insérer prix — UNIQUEMENT avec prix_fr
        try:
            sb.table("prix_historique").upsert({
                "cardmarket_id": cm_id,
                "episode_id":    episode_id,
                "date_releve":   str(date.today()),
                "prix_fr":       prix_fr,
                "avg_7j":        avg_7j,
                "avg_30j":       avg_30j,
                "prix_median":   avg_30j,  # Pour compatibilité vue
                "source":        "rapidapi_tcggo",
            }).execute()

            # Recalculer tendances (utilise prix_fr en priorité)
            try:
                sb.rpc("calculer_tendances", {"p_cardmarket_id": cm_id}).execute()
            except:
                pass

            nb_ok += 1
        except Exception as e:
            print(f"    Erreur prix {cm_id}: {e}")

    if nb_skip > 0:
        print(f"    (skip {nb_skip} produits sans prix_fr)")

    return nb_ok

# ============================================================
#  Priorités par édition
# ============================================================
def score_edition(episode, dernieres_maj_prix_fr, episodes_en_base):
    """
    Score basé sur la présence ou l'ancienneté du prix_fr.
    Une édition sans aucun prix_fr est prioritaire sur tout.
    """
    eid      = str(episode["id"])
    derniere = dernieres_maj_prix_fr.get(eid)
    score    = 0

    if not derniere:
        score += 100  # Jamais eu de prix_fr
    else:
        try:
            jours = (date.today() - date.fromisoformat(derniere)).days
            if jours == 0:   return -1  # MAJ prix_fr faite aujourd'hui
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
    print(f"PokéFolio Scraper v4 — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"API : {RAPIDAPI_HOST}")
    print("=" * 55)

    if not RAPIDAPI_KEY or not SUPABASE_URL:
        print("ERREUR : Variables d'environnement manquantes")
        print(f"  TCGGO_API_KEY     : {'OK' if RAPIDAPI_KEY else 'MANQUANT'}")
        print(f"  SUPABASE_URL_STATS: {'OK' if SUPABASE_URL else 'MANQUANT'}")
        print(f"  SUPABASE_KEY_STATS: {'OK' if SUPABASE_KEY else 'MANQUANT'}")
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
    print(f"\n[1/3] Détection des éditions (toutes pages)...")
    episodes = []
    page     = 1

    while True:
        if appels_effectues >= BUDGET_TOTAL - 1:
            print(f"  Budget atteint pendant la pagination — arrêt à la page {page - 1}")
            break

        data = appel_api("pokemon/episodes", {"page": page})
        if not data:
            print(f"  Erreur API page {page} — arrêt pagination")
            break

        items       = [e for e in data.get("data", []) if e.get("id") and e.get("name")]
        paging      = data.get("paging", {})
        total_pages = paging.get("total", 1)

        episodes.extend(items)
        print(f"  Page {page}/{total_pages} — {len(items)} éditions ({appels_effectues}/{BUDGET_TOTAL} appels)")

        if page >= total_pages:
            break
        page += 1

    print(f"  Total : {len(episodes)} éditions récupérées sur {page} page(s)")

    episodes_en_base = get_episodes_en_base(sb)
    nouveaux = [e for e in episodes if str(e["id"]) not in episodes_en_base]
    if nouveaux:
        print(f"  {len(nouveaux)} nouvelle(s) édition(s) détectée(s)")
    else:
        print(f"  Aucune nouvelle édition")

    # ---- Étape 2 : Priorités basées sur prix_fr ----
    print(f"\n[2/3] Priorités (basées sur prix_fr uniquement)...")
    dernieres_maj = get_dernieres_maj_avec_prix_fr(sb)

    episodes_tries = sorted(
        episodes,
        key=lambda e: score_edition(e, dernieres_maj, episodes_en_base),
        reverse=True
    )

    print("  Top 5 :")
    for ep in episodes_tries[:5]:
        s   = score_edition(ep, dernieres_maj, episodes_en_base)
        nom = NOMS_FR.get(ep["name"], ep["name"])
        maj = dernieres_maj.get(str(ep["id"]), "jamais")
        print(f"    [{s:3}pts] {nom[:35]:35} — MAJ prix_fr: {maj}")

    # ---- Étape 3 : Scraping ----
    budget = BUDGET_TOTAL - appels_effectues
    print(f"\n[3/3] Scraping ({budget - 1} appels dispo)...")
    print(f"  Règle : insertion UNIQUEMENT si prix_fr présent")

    ep_ok = ep_skip = ep_error = 0

    for episode in episodes_tries:
        if appels_effectues >= BUDGET_TOTAL - 1:
            print(f"  Budget atteint — {ep_ok} éditions traitées")
            break

        s   = score_edition(episode, dernieres_maj, episodes_en_base)
        nom = NOMS_FR.get(episode["name"], episode["name"])

        if s < 0 or s <= 3:
            ep_skip += 1
            continue

        print(f"  [{s:3}pts] {nom[:40]}", end=" → ")

        data_prod = appel_api(
            f"pokemon/episodes/{episode['id']}/products",
            {"sort": "price_highest"}
        )

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

        if nb > 0:
            dernieres_maj[str(episode["id"])] = str(date.today())
            print(f"{nb}/{len(produits)} avec prix_fr ✓ ({appels_effectues}/{BUDGET_TOTAL})")
            ep_ok += 1
        else:
            print(f"0 prix_fr disponible pour cette édition")
            ep_skip += 1

    # ---- Résumé ----
    print(f"\n{'='*55}")
    print(f"  Éditions avec prix_fr : {ep_ok}")
    print(f"  Ignorées/skip         : {ep_skip}")
    print(f"  Erreurs API           : {ep_error}")
    print(f"  Appels utilisés       : {appels_effectues}/{BUDGET_TOTAL}")
    print(f"  Budget restant        : {BUDGET_TOTAL - appels_effectues}")

    etat["appels_aujourd_hui"] = appels_effectues
    etat["date_compteur"]      = str(date.today())
    etat["dernieres_maj"]      = dernieres_maj
    sauvegarder_etat(sb, etat)
    print("État sauvegardé ✓")

if __name__ == "__main__":
    main()
