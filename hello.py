import streamlit as st
import pandas as pd
import glob


@st.cache_data
def load_data():
    all_files = glob.glob("data/clean/games_*clean.parquet")
    dfs = [pd.read_parquet(files) for files in all_files]
    return pd.concat(dfs, ignore_index=True)

df = load_data()

st.title("Dashboard NBA")

tab_resume, tab_details = st.tabs(["Résumé", "Détails des matchs"])

# Sidebar Filter
st.sidebar.header("Filtre")

# Filtre par saison
all_season = sorted(df["season"].unique())
selected_season = st.sidebar.multiselect(
    "Saison :", 
    all_season, 
    default=all_season
)

# Filtre par équipe
all_team = sorted(df["home_team_full_name"].unique())
selected_team = st.sidebar.selectbox(
    "Équipe :",
    all_team
)

# Appliquer les filtres
filtered = df.copy()

if selected_season:
    filtered = filtered[filtered["season"].isin(selected_season)]

if selected_team:
    filtered = filtered[
        (filtered["home_team_full_name"] == selected_team) | 
        (filtered["visitor_team_full_name"] == selected_team)
    ]

# Calcul des indicateurs
team_games = filtered.copy()

home_games = team_games[team_games["home_team_full_name"] == selected_team]
visitor_games = team_games[team_games["visitor_team_full_name"] == selected_team]

nb_games = len(home_games) + len(visitor_games)

points_home = home_games["home_team_score"].sum()
points_visitor = visitor_games["visitor_team_score"].sum()
points_total = points_home + points_visitor

if nb_games > 0:
    avg_points = points_total / nb_games
else:
    avg_points = 0.0

wins_home = (home_games["home_team_score"] > home_games["visitor_team_score"]).sum()
wins_visitor = (visitor_games["visitor_team_score"] > visitor_games["home_team_score"]).sum()
total_wins = wins_home + wins_visitor

if nb_games > 0:
    win_rate = total_wins / nb_games * 100
else:
    win_rate = 0.0

with tab_resume:
    # KPIs
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Matchs (équipe sélectionnée)", nb_games)

    with col2:
        st.metric("Points moyens", f"{avg_points:.1f}")

    with col3:
        st.metric("Taux de victoire", f"{win_rate:.1f} %")

    # Graphique
    if team_games.empty:
        st.warning("Aucun match ne correspond aux filtres sélectionnés.")
    else:
        plot_df = team_games.copy()

        def compute_points(row):
            if row["home_team_full_name"] == selected_team:
                return row["home_team_score"]
            else:
                return row["visitor_team_score"]

        plot_df["points_scored"] = plot_df.apply(compute_points, axis=1)
        plot_df = plot_df.sort_values("date")

        st.subheader("Évolution des points marqués")
        st.line_chart(
            plot_df.set_index("date")["points_scored"]
        )

with tab_details:
    st.subheader("Données filtrées")
    st.write(f"Nombre de matchs : {len(filtered)}")
    st.dataframe(filtered)
