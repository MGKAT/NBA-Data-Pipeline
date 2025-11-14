"""Validation et nettoyage des fichiers de matches NBA.

Ce script lit des fichiers JSON bruts (données de matches), valide chaque
enregistrement avec le modèle `schema.Game` (Pydantic), écrit les enregistrements
valides dans `data/validated/` et consigne les erreurs dans `data/errors/`.

Ensuite, il agrège les fichiers validés pour produire des fichiers "clean"
au format Parquet dans `data/clean/` et génère un rapport de qualité dans
`data/reports/`.

Usage (à lancer depuis la racine du projet) :
  python scripts/validate.py

Remarques :
  - Le script ajoute dynamiquement la racine du projet au `sys.path` pour
    permettre l'import du package `models` (présent dans `models/`).
  - Les fichiers d'entrée attendus sont listés dans la variable `files`.
"""

from pydantic import ValidationError
import pandas as pd
import sys, os, json

# Ajouter la racine du projet au PYTHONPATH pour importer `models`.
# Cela évite les erreurs `ModuleNotFoundError` si le script est lancé via
# `python scripts/validate.py` depuis la racine du repo.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from models import schema


# Fichiers sources (chaque entrée correspond à une saison)
files = [
    "data/raw/games_2020.json",
    "data/raw/games_2021.json",
    "data/raw/games_2022.json",
    "data/raw/games_2023.json",
    "data/raw/games_2024.json",
]


# Créer les dossiers de sortie si nécessaire
os.makedirs("data/validated", exist_ok=True)
os.makedirs("data/errors", exist_ok=True)
os.makedirs("data/clean", exist_ok=True)


# Compteurs globaux d'erreurs rencontrées pendant la validation
counters = {"invalid_schema": 0, "same_team": 0}


def flatten(rec: dict) -> dict:
    """Aplatit un enregistrement de match validé.

    Le modèle Pydantic stocke des objets imbriqués pour les équipes; cette
    fonction extrait les champs utiles dans une structure plate adaptée à
    la conversion en DataFrame / Parquet.

    Args:
        rec: dictionnaire JSON représentant un match (tel que produit par
             `schema.Game.model_dump_json()` et rechargé via `json.loads`).

    Returns:
        dict: dictionnaire plat contenant des colonnes clés (game_id, date,
              season, home_team_id, visitor_team_score, ...).
    """
    home_team = rec.get("home_team", {})
    visitor_team = rec.get("visitor_team", {})

    return {
        "game_id": rec.get("id"),
        "date": rec.get("date"),
        "season": rec.get("season"),
        "status": rec.get("status"),
        "periode": rec.get("period"),
        "postseason": rec.get("postseason"),
        "home_team_id": home_team.get("id"),
        "home_team_full_name": home_team.get("full_name"),
        "home_team_score": rec.get("home_team_score"),
        "visitor_team_id": visitor_team.get("id"),
        "visitor_team_full_name": visitor_team.get("full_name"),
        "visitor_team_score": rec.get("visitor_team_score"),
    }


for file in files:
    # Boucle principale : traiter chaque fichier de saison
    print(f"Processing file: {file}")

    # Extraire l'année depuis le nom du fichier (ex: games_2020.json)
    year = int(os.path.basename(file).split("_")[1].split(".")[0])

    # Chemins de sortie pour cette saison
    valid = f"data/validated/games_{year}_validated.json"
    errors = f"data/errors/games_{year}_errors.json"

    # Charger le JSON source
    with open(file, "r", encoding="utf-8") as f:
        games_data = json.load(f)

    # Valider chaque enregistrement via le modèle Pydantic
    for game in games_data:
        try:
            g = schema.Game.model_validate(game)
            # Écrire l'enregistrement validé en JSON ligne par ligne
            with open(valid, "a", encoding="utf-8") as vf:
                vf.write(g.model_dump_json() + "\n")
        except ValidationError as e:
            # Classifier les erreurs pour rapport
            msg = str(e)
            if "Home and visitor teams cannot be identical" in msg:
                counters["same_team"] += 1
                err_type = "same_team"
            else:
                counters["invalid_schema"] += 1
                err_type = "invalid_schema"

            er_s = {
                "type": err_type,
                "game_id_hint": game.get("id"),
                "season": game.get("season"),
                "reason": msg[:300],
            }
            # Journaliser l'erreur
            with open(errors, "a", encoding="utf-8") as f:
                f.write(json.dumps(er_s, ensure_ascii=False) + "\n")
            continue


# Liste des fichiers validés attendus (une par saison)
validate_file = [f"data/validated/games_{year}_validated.json" for year in range(2020, 2025)]

for path in validate_file:
    # Si le fichier validé n'existe pas, on passe la génération "clean"
    if not os.path.exists(path):
        print(f"File {path} does not exist, skipping clean generation.")
        continue

    year = int(os.path.basename(path).split("_")[1])
    row = []

    # Recharger les JSON ligne par ligne puis aplatir
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            rec = json.loads(line)
            row.append(flatten(rec))

    # Construire DataFrame et écrire Parquet
    df = pd.DataFrame(row)
    outpath = f"data/clean/games_{year}_clean.parquet"
    df.to_parquet(outpath, index=False)

    # Rapport qualité pour cette saison
    os.makedirs("data/reports", exist_ok=True)

    quality_report = {
        "total_valid": sum(
            1
            for year in range(2020, 2025)
            if os.path.exists(f"data/validated/games_{year}_validated.json")
        ),
        "error_counts": counters,
    }

    with open(f"data/reports/games_{year}_quality_report.json", "w", encoding="utf-8") as f:
        json.dump(quality_report, f, ensure_ascii=False, indent=2)

