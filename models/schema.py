"""Schémas Pydantic pour les objets utilisés dans le pipeline.

Ce module définit les modèles Pydantic pour représenter une équipe (`TeamInfo`)
et un match (`Game`). Il expose aussi un alias de type réutilisable
`IntGE0` (entier >= 0) pour factoriser les contraintes de validation.

Conventions :
  - Les objets JSON récupérés depuis l'API sont validés avec `Game.model_validate()`.
  - Les champs inconnus sont ignorés (`extra='ignore'`) pour tolérer les évolutions
    de l'API.

Exemple d'utilisation :
  from models import schema
  g = schema.Game.model_validate(json_dict)

Remarque : ce module est compatible avec Pydantic v2 (utilise `model_validator` et
`model_dump_json` côté utilisation).
"""

from typing import Optional, Annotated
from datetime import date, datetime
from pydantic import BaseModel, Field, ConfigDict, model_validator


# ---- Équipe
class TeamInfo(BaseModel):
    """Informations minimalistes décrivant une équipe.

    Les champs correspondent à ce que renvoie typiquement l'API (id, city,
    nom complet, abréviation, etc.). Le modèle ignore les champs inconnus
    afin d'être résilient aux ajouts de l'API.
    """

    id: int
    conference: str
    division: str
    city: str
    name: str
    full_name: str
    abbreviation: str

    model_config = ConfigDict(extra="ignore")  # ignore champs inconnus


# ---- Match
# Alias de type : entier vérifié >= 0. Utilisé pour les scores/compteurs.
# Annotated permet d'associer la métadonnée Pydantic `Field(ge=0)` au type.
IntGE0 = Annotated[int, Field(ge=0)]


class Game(BaseModel):
    """Modèle représentant un match.

    Champs principaux : identifiant, date, saison, status, période, scores et
    objets imbriqués `home_team` / `visitor_team`.

    Utilisation typique :
        g = Game.model_validate(json_dict)

    Validation personnalisée :
        - `check_team_different` : s'assure que les deux équipes ne sont pas
          identiques (même id), sinon lève une `ValueError`.
    """

    # Obligatoires
    id: int
    date: date
    season: int
    status: str
    period: IntGE0
    time: str
    postseason: bool
    home_team_score: IntGE0
    visitor_team_score: IntGE0
    datetime: datetime  # Pydantic parse ISO "2020-12-23T00:00:00.000Z"

    # Optionnels (nullable -> Optional)
    home_q1: Optional[IntGE0] = None
    home_q2: Optional[IntGE0] = None
    home_q3: Optional[IntGE0] = None
    home_q4: Optional[IntGE0] = None
    home_ot1: Optional[IntGE0] = None
    home_ot2: Optional[IntGE0] = None
    home_ot3: Optional[IntGE0] = None
    home_timeouts_remaining: Optional[IntGE0] = None
    home_in_bonus: Optional[bool] = None

    visitor_q1: Optional[IntGE0] = None
    visitor_q2: Optional[IntGE0] = None
    visitor_q3: Optional[IntGE0] = None
    visitor_q4: Optional[IntGE0] = None
    visitor_ot1: Optional[IntGE0] = None
    visitor_ot2: Optional[IntGE0] = None
    visitor_ot3: Optional[IntGE0] = None
    visitor_timeouts_remaining: Optional[IntGE0] = None
    visitor_in_bonus: Optional[bool] = None

    # Objets imbriqués
    home_team: TeamInfo
    visitor_team: TeamInfo

    model_config = ConfigDict(extra="ignore")  # si l'API ajoute des champs

    @model_validator(mode="after")
    def check_team_different(self):
        """Validator exécuté après parsing : interdit les mêmes équipes.

        Lève ValueError si `home_team.id == visitor_team.id`.
        Retourne `self` sinon (nécessaire pour `mode='after'`).
        """
        if self.home_team.id == self.visitor_team.id:
            raise ValueError("Home and visitor team cannot be identical")
        return self
