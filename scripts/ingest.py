"""Script d'ingestion des matches depuis l'API `balldontlie`.

Ce script récupère, page par page, les matches pour les saisons listées dans
`SEASONS` et écrit les résultats bruts (JSON) dans `data/raw/games_<season>.json`.

Comportement et bonnes pratiques :
    - Respect des limites de l'API : le script pause 12 secondes entre pages et
        gère le code HTTP 429 (Too Many Requests).
    - Limitation de pages : `MAX_PAGES` protège contre des boucles infinies si
        l'API renvoie un curseur indéfiniment.
    - Le header Authorization est actuellement présent dans `headers` — considère
        le déplacer dans une variable d'environnement ou un fichier de configuration
        si ce token est sensible.

Usage : lancer depuis la racine du dépôt :
    python scripts/ingest.py

Remarque : ce script s'exécute directement lorsqu'il est importé. Si tu veux
l'utiliser comme module, ajouter un `main()` et le garde
`if __name__ == '__main__'` (je peux le faire si tu veux).
"""

import requests
import json
import time
import os


# URL de l'API et paramètres généraux
url = "https://api.balldontlie.io/v1/games"
# NOTE: le token dans `headers` devrait idéalement venir d'une variable d'environnement
# pour des raisons de sécurité / partage du dépôt.
headers = {"Authorization": "214925d6-ddeb-48fe-af81-f3c27b3a56af"}

# Saisons à ingérer et limite de pages par saison
SEASONS = [2020, 2021, 2022, 2023, 2024]
MAX_PAGES = 60


# Répertoires de sortie
os.makedirs("data/raw", exist_ok=True)


for years in SEASONS:
        """Boucle d'ingestion par saison.

        Pour chaque saison :
            - on récupère la première page,
            - on itère ensuite tant que l'API renvoie un `next_cursor` et que la
                limite de pages n'est pas atteinte,
            - on respecte un délai entre les pages pour éviter d'être bloqué.
        """
        page = 1
        params = {"per_page": 100, "seasons[]": years}
        all_games = []

        output_path = f"data/raw/games_{years}.json"

        # Première requête (sans curseur)
        resp = requests.get(url, headers=headers, params=params)

        # Si on a dépassé le rate limit côté serveur, attendre et ré-essayer une fois
        if resp.status_code == 429:
                time.sleep(12)
                resp = requests.get(url, headers=headers, params=params)

        resp.raise_for_status()
        payload = resp.json()

        page_items = payload.get('data', [])
        cursor = payload.get("meta", {}).get("next_cursor")

        all_games.extend(page_items)
        print(f"Saison={years} page {page} count={len(page_items)} next_cursor={cursor} total_games={len(all_games)}")

        # Pagination suivante
        while cursor and page < MAX_PAGES:
                page += 1
                params = {"per_page": 100, "seasons[]": years, "cursor": cursor}
                resp = requests.get(url, headers=headers, params=params)

                if resp.status_code == 429:
                        print("⚠️ Trop de requêtes — pause de 12 secondes...")
                        time.sleep(12)
                        continue

                resp.raise_for_status()
                payload = resp.json()

                page_items = payload.get('data', [])
                if not page_items:
                        print("⚠️ Page vide reçue, arrêt propre.")
                        break

                cursor = payload.get("meta", {}).get("next_cursor")
                all_games.extend(page_items)
                print(f"Saison={years} page={page} count={len(page_items)} next_cursor={cursor} total_games={len(all_games)}")

                # Pause courte pour respecter les limites de l'API
                print("⏳ Attente de 12 secondes avant la prochaine page...")
                time.sleep(12)

        # Écrire le fichier JSON brut pour la saison
        with open(output_path, "w", encoding="utf-8") as f:
                json.dump(all_games, f, indent=2, ensure_ascii=False)
        print(f"SUMMARY season={years} pages={page} games={len(all_games)} file={output_path}")
