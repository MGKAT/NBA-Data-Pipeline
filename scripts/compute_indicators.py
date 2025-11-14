import pandas as pd
import glob, json, os


os.makedirs("data/indicators", exist_ok=True)
files = glob.glob("data/clean/games_*clean.parquet")

dfs = [pd.read_parquet(p) for p in files]

df = pd.concat(dfs, ignore_index=True)

print(df.head())

df["home_win"] = df["home_team_score"] > df["visitor_team_score"]
df["visitor_win"] = df["visitor_team_score"] > df["home_team_score"]

# Moyenne de point marquer par équipe à domicile et à l'extérieur
avg_points = df.groupby("home_team_full_name")["home_team_score"].mean().sort_values(ascending=False)
print("Moyenne des points à domicile par équipe (top 10) :")
print(avg_points.head(10))
avg_points_visitor = df.groupby("visitor_team_full_name")["visitor_team_score"].mean().sort_values(ascending=False)
print("\nMoyenne des points à l'extérieur par équipe (top 10) :")
print(avg_points_visitor.head(10))


# Meilleur attaque 
best_attacks = (
    df.groupby("home_team_full_name")["home_team_score"].mean()
    + df.groupby("visitor_team_full_name")["visitor_team_score"].mean()
).sort_values(ascending=False)

print("\nMoyenne des points à domicile par équipe (top 10) :")
print(best_attacks.head(10))

# Meilleur défense
best_defenses = (
    df.groupby("home_team_full_name")["visitor_team_score"].mean()
    + df.groupby("visitor_team_full_name")["home_team_score"].mean()
).sort_values()
print("\nMeilleure défense (points encaissés en moyenne, bas = mieux) :")
print(best_defenses.head(10))

# Victoire par équipe
wins = (
    df.groupby("home_team_full_name")["home_win"].sum()
    + df.groupby("visitor_team_full_name")["visitor_win"].sum()
).sort_values(ascending=False)

report = {
    "best_attacks": best_attacks.head(10).to_dict(),
    "best_defenses": best_defenses.head(10).to_dict(),
    "wins": wins.head(10).to_dict(),
}


with open("data/indicators/indicators.json", "w") as f:
    json.dump(report, f, indent=2)
