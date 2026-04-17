## Projet : LinkedIn Job Market Analysis Dashboard

Ce projet présente une architecture de données complète sur **Snowflake** et une application **Streamlit** pour analyser les tendances du marché de l'emploi LinkedIn en 2026.

---

### Architecture des Données :

Les données sont organisées en trois étapes afin de garantir leur qualité pour l’utilisation par la suite.

#### Couche Bronze
Importation des données brutes au format `.JSON` et `.CSV` depuis un stage S3 vers Snowflake. Les données sont stockées telles qu’elles arrivent pour conserver l'historique brut.

#### Couche Argent (Silver)
Nettoyage de différentes données brutes :
* **Typage et conversion** : Utilisation de `TRY_TO_NUMBER` et `TRY_CAST` afin d’éviter les erreurs de conversion des données et garantir qu'elles soient utilisables.
* **Gestion temporelle** : Transformation des timestamps Unix en formats date/heure lisibles.
* **Traitement des valeurs nulles** et filtrage des offres incomplètes.
* **Dédoublonnage** : Suppression des doublons pour assurer l'unicité des offres.

#### Couche Or (Gold)
Création de tables et vues agrégées pour optimiser les performances de la partie applicative et rendre l’application bien plus rapide :
* **Tables de rapport** : Top titres et salaires déjà calculés par industrie.
* **Logique métier** : Utilisation des fonctions `QUALIFY` et `ROW_NUMBER` afin de dédoublonner les entreprises présentes dans plusieurs catégories de secteurs et n'en garder qu'une seule principale.

---

## Problèmes rencontrés et Solutions

| Problème | Solution Mise en Place |
| :--- | :--- |
| **Doublons d'offres** | Utilisation de `ROW_NUMBER()` pour garantir l'unicité par `job_id`. |
| **Espaces invisibles** | Application de la fonction `TRIM()` pour les filtres et les jointures SQL. |
| **Erreurs de syntaxe SQL** | Remplacement des guillemets simples par des doubles guillemets pour les noms de colonnes. |
| **ImportError (Python)** | Installation des packages `matplotlib` et `plotly` dans l'environnement Snowflake. |
| **Erreur de mapping** | Correction de la colonne `COMPANY_NAME` qui contenait en réalité le `COMPANY_ID`. |

---
## Script SQL Complet
```sql
<details>
<summary>Cliquez pour voir le code SQL (Bronze/Silver/Gold)</summary>

-- Création de la Database
CREATE DATABASE IF NOT EXISTS LINKEDIN;
-- Définition du Warehouse
USE WAREHOUSE COMPUTE_WH;

-- Création du Schéma bronze
CREATE SCHEMA IF NOT EXISTS LINKEDIN.BRONZE;

-- Création du Stage Externe (S3)
CREATE OR REPLACE STAGE LINKEDIN.BRONZE.LINKEDIN_STAGE
  URL = 's3://snowflake-lab-bucket/';

-- Configuration des Formats de fichiers
CREATE OR REPLACE FILE FORMAT LINKEDIN.BRONZE.CSV_FORMAT
  TYPE = 'CSV' 
  SKIP_HEADER = 1 
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('', 'NULL');

CREATE OR REPLACE FILE FORMAT LINKEDIN.BRONZE.JSON_FORMAT
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE;

-- Table Job Postings (CSV)
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.JOB_POSTINGS (
    job_id STRING, company_id STRING, title STRING, description STRING, 
    max_salary STRING, med_salary STRING, min_salary STRING, pay_period STRING, 
    formatted_work_type STRING, location STRING, applies STRING, original_listed_time STRING, 
    remote_allowed STRING, views STRING, job_posting_url STRING, application_url STRING, 
    application_type STRING, expiry STRING, closed_time STRING, formatted_experience_level STRING, 
    skills_desc STRING, listed_time STRING, posting_domain STRING, sponsored STRING, 
    work_type STRING, currency STRING, compensation_type STRING
);

COPY INTO LINKEDIN.BRONZE.JOB_POSTINGS 
FROM @LINKEDIN.BRONZE.LINKEDIN_STAGE/job_postings.csv 
FILE_FORMAT = (FORMAT_NAME = 'LINKEDIN.BRONZE.CSV_FORMAT');

select * from LINKEDIN.BRONZE.JOB_POSTINGS;

-- --- TABLES CSV ---

CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.BENEFITS (job_id STRING, inferred STRING, type STRING);
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.EMPLOYEE_COUNTS (company_id STRING, employee_count STRING, follower_count STRING, time_recorded STRING);
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.JOB_SKILLS (job_id STRING, skill_abr STRING);

-- --- TABLES JSON ---
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.COMPANIES (data VARIANT);
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.COMPANY_INDUSTRIES (data VARIANT);
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.COMPANY_SPECIALITIES (data VARIANT);
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.JOB_INDUSTRIES (data VARIANT);

-- --- CHARGEMENT ---
COPY INTO LINKEDIN.BRONZE.BENEFITS FROM @LINKEDIN.BRONZE.LINKEDIN_STAGE/benefits.csv FILE_FORMAT = (FORMAT_NAME = 'LINKEDIN.BRONZE.CSV_FORMAT');
COPY INTO LINKEDIN.BRONZE.EMPLOYEE_COUNTS FROM @LINKEDIN.BRONZE.LINKEDIN_STAGE/employee_counts.csv FILE_FORMAT = (FORMAT_NAME = 'LINKEDIN.BRONZE.CSV_FORMAT');
COPY INTO LINKEDIN.BRONZE.JOB_SKILLS FROM @LINKEDIN.BRONZE.LINKEDIN_STAGE/job_skills.csv FILE_FORMAT = (FORMAT_NAME = 'LINKEDIN.BRONZE.CSV_FORMAT');

COPY INTO LINKEDIN.BRONZE.COMPANIES FROM @LINKEDIN.BRONZE.LINKEDIN_STAGE/companies.json FILE_FORMAT = (FORMAT_NAME = 'LINKEDIN.BRONZE.JSON_FORMAT');
COPY INTO LINKEDIN.BRONZE.COMPANY_INDUSTRIES FROM @LINKEDIN.BRONZE.LINKEDIN_STAGE/company_industries.json FILE_FORMAT = (FORMAT_NAME = 'LINKEDIN.BRONZE.JSON_FORMAT');
COPY INTO LINKEDIN.BRONZE.COMPANY_SPECIALITIES FROM @LINKEDIN.BRONZE.LINKEDIN_STAGE/company_specialities.json FILE_FORMAT = (FORMAT_NAME = 'LINKEDIN.BRONZE.JSON_FORMAT');
COPY INTO LINKEDIN.BRONZE.JOB_INDUSTRIES FROM @LINKEDIN.BRONZE.LINKEDIN_STAGE/job_industries.json FILE_FORMAT = (FORMAT_NAME = 'LINKEDIN.BRONZE.JSON_FORMAT');

-- --- VERIFICATION DE CHAQUE TABLE ---
select * from LINKEDIN.BRONZE.BENEFITS;
select * from LINKEDIN.BRONZE.COMPANIES;
select * from LINKEDIN.BRONZE.COMPANY_INDUSTRIES;
select * from LINKEDIN.BRONZE.COMPANY_SPECIALITIES;
select * from LINKEDIN.BRONZE.employee_counts;
select * from LINKEDIN.BRONZE.job_industries;
select * from LINKEDIN.BRONZE.job_skills;

USE DATABASE LINKEDIN;
CREATE SCHEMA IF NOT EXISTS SILVER;

-- 1. COMPANIES (Table Pivot)
CREATE OR REPLACE TABLE LINKEDIN.SILVER.COMPANIES AS
SELECT
    data:company_id::INT AS company_id,
    data:name::STRING AS company_name,
    data:description::STRING AS description,
    data:company_size::INT AS company_size_code,
    data:state::STRING AS state,
    data:country::STRING AS country,
    data:city::STRING AS city,
    data:zip_code::STRING AS zip_code,
    data:address::STRING AS address,
    data:url::STRING AS linkedin_url
FROM LINKEDIN.BRONZE.COMPANIES;

-- 2. COMPANY_INDUSTRIES
CREATE OR REPLACE TABLE LINKEDIN.SILVER.COMPANY_INDUSTRIES AS
SELECT
    data:company_id::INT AS company_id,
    data:industry::STRING AS industry
FROM LINKEDIN.BRONZE.COMPANY_INDUSTRIES;

-- 3. COMPANY_SPECIALITIES
CREATE OR REPLACE TABLE LINKEDIN.SILVER.COMPANY_SPECIALITIES AS
SELECT
    data:company_id::INT AS company_id,
    data:speciality::STRING AS speciality
FROM LINKEDIN.BRONZE.COMPANY_SPECIALITIES;

-- 4. EMPLOYEE_COUNTS
CREATE OR REPLACE TABLE LINKEDIN.SILVER.EMPLOYEE_COUNTS AS
SELECT
    company_id::INT AS company_id,
    employee_count::INT AS employee_count,
    follower_count::INT AS follower_count,
    TO_TIMESTAMP_NTZ(time_recorded::BIGINT) AS recorded_at
FROM LINKEDIN.BRONZE.EMPLOYEE_COUNTS;

-- 5. JOB_POSTINGS (Dépend de COMPANIES pour le nom)
CREATE OR REPLACE TABLE LINKEDIN.SILVER.JOB_POSTINGS AS
SELECT
    jp.job_id::INT AS job_id,
    jp.company_id::INT AS company_id,
    c.company_name, 
    jp.title,
    jp.description,
    TRY_CAST(jp.max_salary AS FLOAT) AS max_salary,
    TRY_CAST(jp.med_salary AS FLOAT) AS med_salary,
    TRY_CAST(jp.min_salary AS FLOAT) AS min_salary,
    jp.pay_period,
    jp.formatted_work_type AS work_type,
    jp.location,
    TRY_CAST(jp.applies AS INT) AS nb_applications,
    TO_TIMESTAMP_NTZ(jp.listed_time::BIGINT / 1000) AS listed_at,
    IFF(TRY_CAST(jp.remote_allowed AS FLOAT) = 1, TRUE, FALSE) AS is_remote,
    jp.formatted_experience_level AS experience_level,
    jp.currency
FROM LINKEDIN.BRONZE.JOB_POSTINGS jp
LEFT JOIN LINKEDIN.SILVER.COMPANIES c 
    ON jp.company_id::INT = c.company_id;

-- 6. JOB_INDUSTRIES
CREATE OR REPLACE TABLE LINKEDIN.SILVER.JOB_INDUSTRIES AS
SELECT
    data:job_id::INT AS job_id,
    data:industry_id::INT AS industry_id
FROM LINKEDIN.BRONZE.JOB_INDUSTRIES;

-- 7. JOB_SKILLS
CREATE OR REPLACE TABLE LINKEDIN.SILVER.JOB_SKILLS AS
SELECT
    job_id::INT AS job_id,
    skill_abr
FROM LINKEDIN.BRONZE.JOB_SKILLS;

-- 8. BENEFITS
CREATE OR REPLACE TABLE LINKEDIN.SILVER.BENEFITS AS
SELECT
    job_id::INT AS job_id,
    inferred::BOOLEAN AS is_inferred,
    type AS benefit_type
FROM LINKEDIN.BRONZE.BENEFITS;

-- --- VERIFICATION DE CHAQUE TABLE ---
select * from LINKEDIN.SILVER.JOB_POSTINGS;
select * from LINKEDIN.SILVER.BENEFITS;
select * from LINKEDIN.SILVER.COMPANIES;
select * from LINKEDIN.SILVER.COMPANY_INDUSTRIES;
select * from LINKEDIN.SILVER.COMPANY_SPECIALITIES;
select * from LINKEDIN.SILVER.employee_counts;
select * from LINKEDIN.SILVER.job_industries;
select * from LINKEDIN.SILVER.job_skills;

-- Création du Schéma Gold
CREATE SCHEMA IF NOT EXISTS LINKEDIN.GOLD;

-- ================================================================
-- COUCHE GOLD : ANALYSES DÉDOUBLONNÉES
-- ================================================================
CREATE SCHEMA IF NOT EXISTS LINKEDIN.GOLD;

-- 0. CTE Intermédiaire pour ne pas multiplier les jobs (1 industrie par entreprise)
-- On l'utilise dans les requêtes ci-dessous
CREATE OR REPLACE VIEW LINKEDIN.GOLD.DIM_COMPANY_MAIN_INDUSTRY AS
SELECT company_id, industry
FROM LINKEDIN.SILVER.COMPANY_INDUSTRIES
QUALIFY ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY industry) = 1;


-- 1. Top 10 titres par industrie (CORRIGÉ)
CREATE OR REPLACE TABLE LINKEDIN.GOLD.REPORT_TOP_TITLES_BY_INDUSTRY AS
SELECT 
    mi.industry,
    jp.title,
    COUNT(*) AS nb_postings
FROM LINKEDIN.SILVER.JOB_POSTINGS jp
JOIN LINKEDIN.GOLD.DIM_COMPANY_MAIN_INDUSTRY mi ON jp.company_id = mi.company_id
GROUP BY 1, 2
QUALIFY ROW_NUMBER() OVER (PARTITION BY mi.industry ORDER BY nb_postings DESC) <= 10;


-- 2. Top 10 salaires par industrie (CORRIGÉ)
CREATE OR REPLACE TABLE LINKEDIN.GOLD.REPORT_TOP_SALARIES_BY_INDUSTRY AS
SELECT 
    mi.industry,
    jp.title,
    MAX(jp.max_salary) AS max_salary_observed
FROM LINKEDIN.SILVER.JOB_POSTINGS jp
JOIN LINKEDIN.GOLD.DIM_COMPANY_MAIN_INDUSTRY mi ON jp.company_id = mi.company_id
WHERE jp.max_salary IS NOT NULL
GROUP BY 1, 2
QUALIFY ROW_NUMBER() OVER (PARTITION BY mi.industry ORDER BY max_salary_observed DESC) <= 10;


-- 3. Répartition par secteur (CORRIGÉ)
CREATE OR REPLACE VIEW LINKEDIN.GOLD.REPORT_JOBS_BY_INDUSTRY AS
SELECT 
    mi.industry,
    COUNT(*) AS nb_jobs
FROM LINKEDIN.SILVER.JOB_POSTINGS jp
JOIN LINKEDIN.GOLD.DIM_COMPANY_MAIN_INDUSTRY mi ON jp.company_id = mi.company_id
GROUP BY 1
ORDER BY 2 DESC;

-- 4. Analyse Taille Entreprise (Correction : Création effective)
CREATE OR REPLACE VIEW LINKEDIN.GOLD.REPORT_JOBS_BY_COMPANY_SIZE AS
SELECT
    CASE c.company_size_code
        WHEN 0 THEN '0 - 1 employé'
        WHEN 1 THEN '2 - 10 employés'
        WHEN 2 THEN '11 - 50 employés'
        WHEN 3 THEN '51 - 200 employés'
        WHEN 4 THEN '201 - 500 employés'
        WHEN 5 THEN '501 - 1000 employés'
        WHEN 6 THEN '1001 - 5000 employés'
        WHEN 7 THEN '5000+ employés'
        ELSE 'Inconnu'
    END AS size_label,
    COUNT(*) AS nb_jobs
FROM LINKEDIN.SILVER.JOB_POSTINGS jp
LEFT JOIN LINKEDIN.SILVER.COMPANIES c ON jp.company_id = c.company_id
GROUP BY 1, c.company_size_code
ORDER BY c.company_size_code;

-- 5. Analyse Type de travail (Correction : Création effective)
CREATE OR REPLACE VIEW LINKEDIN.GOLD.REPORT_JOBS_BY_WORK_TYPE AS
SELECT work_type, COUNT(*) AS nb_jobs
FROM LINKEDIN.SILVER.JOB_POSTINGS
WHERE work_type IS NOT NULL
GROUP BY 1;

SELECT * FROM LINKEDIN.GOLD.REPORT_JOBS_BY_COMPANY_SIZE;
SELECT * FROM LINKEDIN.GOLD.REPORT_JOBS_BY_WORK_TYPE;

-- vérification--
SELECT 
    (SELECT COUNT(*) FROM LINKEDIN.SILVER.JOB_POSTINGS) AS total_jobs_source,
    (SELECT SUM(nb_jobs) FROM LINKEDIN.GOLD.REPORT_JOBS_BY_INDUSTRY) AS total_jobs_analyzed,
    (total_jobs_source - total_jobs_analyzed) AS missing_jobs;
SELECT size_label, nb_jobs
FROM LINKEDIN.GOLD.REPORT_JOBS_BY_COMPANY_SIZE
ORDER BY size_label;
-- Est-ce qu'on a des aberrations (salaires à 0 ou négatifs) ?
SELECT * FROM LINKEDIN.GOLD.REPORT_TOP_SALARIES_BY_INDUSTRY
WHERE max_salary_observed <= 0;

-- Top 5 des industries les mieux payées pour vérifier la logique
SELECT industry, AVG(max_salary_observed) as avg_max
FROM LINKEDIN.GOLD.REPORT_TOP_SALARIES_BY_INDUSTRY
GROUP BY 1
ORDER BY 2 DESC
LIMIT 5;
SELECT work_type, nb_jobs
FROM LINKEDIN.GOLD.REPORT_JOBS_BY_WORK_TYPE; 
</details>
```

## Application Streamlit (Code Python)

L'application utilise **Snowpark** pour interroger Snowflake et **Altair** pour une visualisation haute performance.

```python
# linkedin_dashboard.py
# Application Streamlit - Analyse des offres LinkedIn
# À déployer dans Snowflake via : Snowflake > Streamlit > + Create Streamlit App

import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session

# ──────────────────────────────────────────────
# CONFIG PAGE
# ──────────────────────────────────────────────
st.set_page_config(
    page_title="LinkedIn Job Market Dashboard",
    page_icon="💼",
    layout="wide",
)

# ──────────────────────────────────────────────
# SESSION SNOWFLAKE
# ──────────────────────────────────────────────
session = get_active_session()

# ──────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────
@st.cache_data(ttl=600)
def run_query(sql: str) -> pd.DataFrame:
    return session.sql(sql).to_pandas()

def metric_card(label: str, value: str, delta: str = None):
    if delta:
        st.metric(label=label, value=value, delta=delta)
    else:
        st.metric(label=label, value=value)

# ──────────────────────────────────────────────
# HEADER
# ──────────────────────────────────────────────
st.title("💼 LinkedIn Job Market — Tableau de Bord")
st.caption("Données LinkedIn • Architecture Bronze / Silver / Gold sur Snowflake")
st.divider()

# ──────────────────────────────────────────────
# KPIs GLOBAUX
# ──────────────────────────────────────────────
kpi_sql = """
SELECT
    COUNT(*)                                          AS total_jobs,
    COUNT(DISTINCT company_id)                        AS total_companies,
    ROUND(AVG(med_salary), 0)                         AS avg_med_salary,
    SUM(CASE WHEN is_remote THEN 1 ELSE 0 END)        AS remote_jobs,
    ROUND(100.0 * SUM(CASE WHEN is_remote THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_remote
FROM LINKEDIN.SILVER.JOB_POSTINGS;
"""
kpi = run_query(kpi_sql).iloc[0]

c1, c2, c3, c4, c5 = st.columns(5)
with c1:
    metric_card("Offres totales", f"{int(kpi['TOTAL_JOBS']):,}")
with c2:
    metric_card("Entreprises", f"{int(kpi['TOTAL_COMPANIES']):,}")
with c3:
    avg_sal = kpi['AVG_MED_SALARY']
    metric_card("Salaire médian moyen", f"${avg_sal:,.0f}" if pd.notna(avg_sal) else "N/A")
with c4:
    metric_card("Offres remote", f"{int(kpi['REMOTE_JOBS']):,}")
with c5:
    metric_card("% Remote", f"{kpi['PCT_REMOTE']}%")

st.divider()

# ──────────────────────────────────────────────
# ONGLETS
# ──────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "🏷️ Top Titres / Industrie",
    "💰 Top Salaires / Industrie",
    "🏢 Taille d'Entreprise",
    "🏭 Secteurs d'Activité",
    "⏱️ Type d'Emploi",
    "🔍 Exploration Libre",
])

# ══════════════════════════════════════════════
# TAB 1 — TOP 10 TITRES PAR INDUSTRIE
# ══════════════════════════════════════════════
with tab1:
    st.subheader("Top 10 des titres de postes les plus publiés par industrie")

    industries_sql = """
        SELECT DISTINCT industry
        FROM LINKEDIN.GOLD.REPORT_TOP_TITLES_BY_INDUSTRY
        WHERE industry IS NOT NULL
        ORDER BY 1;
    """
    industries = run_query(industries_sql)["INDUSTRY"].tolist()

    selected_industry = st.selectbox(
        "Sélectionner une industrie",
        options=industries,
        key="tab1_industry",
    )

    df_titles = run_query(f"""
        SELECT title, nb_postings
        FROM LINKEDIN.GOLD.REPORT_TOP_TITLES_BY_INDUSTRY
        WHERE industry = '{selected_industry.replace("'", "''")}'
        ORDER BY nb_postings DESC
        LIMIT 10;
    """)

    if df_titles.empty:
        st.info("Aucune donnée pour cette industrie.")
    else:
        chart = (
            alt.Chart(df_titles)
            .mark_bar(color="#4F8EF7", cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
            .encode(
                x=alt.X("NB_POSTINGS:Q", title="Nombre d'offres"),
                y=alt.Y("TITLE:N", sort="-x", title="Titre du poste"),
                tooltip=["TITLE", "NB_POSTINGS"],
            )
            .properties(height=400)
        )
        st.altair_chart(chart, use_container_width=True)

        with st.expander("Voir les données brutes"):
            st.dataframe(df_titles, use_container_width=True)

# ══════════════════════════════════════════════
# TAB 2 — TOP 10 SALAIRES PAR INDUSTRIE
# ══════════════════════════════════════════════
with tab2:
    st.subheader("Top 10 des postes les mieux rémunérés par industrie")

    col_a, col_b = st.columns([2, 1])
    with col_a:
        industries2_sql = """
            SELECT DISTINCT industry
            FROM LINKEDIN.GOLD.REPORT_TOP_SALARIES_BY_INDUSTRY
            WHERE industry IS NOT NULL
            ORDER BY 1;
        """
        industries2 = run_query(industries2_sql)["INDUSTRY"].tolist()
        selected_industry2 = st.selectbox(
            "Sélectionner une industrie",
            options=industries2,
            key="tab2_industry",
        )
    with col_b:
        top_n = st.slider("Nombre de postes à afficher", 3, 10, 10, key="tab2_topn")

    df_sal = run_query(f"""
        SELECT title, max_salary_observed
        FROM LINKEDIN.GOLD.REPORT_TOP_SALARIES_BY_INDUSTRY
        WHERE industry = '{selected_industry2.replace("'", "''")}'
        ORDER BY max_salary_observed DESC
        LIMIT {top_n};
    """)

    if df_sal.empty:
        st.info("Aucune donnée salariale pour cette industrie.")
    else:
        chart2 = (
            alt.Chart(df_sal)
            .mark_bar(color="#22C55E", cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
            .encode(
                x=alt.X("MAX_SALARY_OBSERVED:Q", title="Salaire max. observé ($)", axis=alt.Axis(format="$,.0f")),
                y=alt.Y("TITLE:N", sort="-x", title="Titre du poste"),
                tooltip=[
                    alt.Tooltip("TITLE", title="Titre"),
                    alt.Tooltip("MAX_SALARY_OBSERVED", title="Salaire max ($)", format="$,.0f"),
                ],
            )
            .properties(height=400)
        )
        st.altair_chart(chart2, use_container_width=True)

        # Tableau récapitulatif
        df_sal_fmt = df_sal.copy()
        df_sal_fmt["MAX_SALARY_OBSERVED"] = df_sal_fmt["MAX_SALARY_OBSERVED"].apply(
            lambda x: f"${x:,.0f}" if pd.notna(x) else "N/A"
        )
        df_sal_fmt.columns = ["Titre du poste", "Salaire max observé"]
        st.dataframe(df_sal_fmt, use_container_width=True, hide_index=True)

    # Bonus : comparaison inter-industries (moyenne des max salaires)
    st.markdown("---")
    st.markdown("##### Comparaison : salaire max moyen par industrie (toutes industries)")
    df_cross = run_query("""
        SELECT industry, ROUND(AVG(max_salary_observed), 0) AS avg_max_salary
        FROM LINKEDIN.GOLD.REPORT_TOP_SALARIES_BY_INDUSTRY
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 20;
    """)
    if not df_cross.empty:
        chart_cross = (
            alt.Chart(df_cross)
            .mark_bar(color="#A855F7", cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
            .encode(
                x=alt.X("AVG_MAX_SALARY:Q", title="Salaire max moyen ($)", axis=alt.Axis(format="$,.0f")),
                y=alt.Y("INDUSTRY:N", sort="-x", title="Industrie"),
                tooltip=[
                    alt.Tooltip("INDUSTRY", title="Industrie"),
                    alt.Tooltip("AVG_MAX_SALARY", title="Salaire max moyen ($)", format="$,.0f"),
                ],
            )
            .properties(height=500)
        )
        st.altair_chart(chart_cross, use_container_width=True)

# ══════════════════════════════════════════════
# TAB 3 — RÉPARTITION PAR TAILLE D'ENTREPRISE
# ══════════════════════════════════════════════
with tab3:
    st.subheader("Répartition des offres par taille d'entreprise")

    df_size = run_query("""
        SELECT size_label, nb_jobs
        FROM LINKEDIN.GOLD.REPORT_JOBS_BY_COMPANY_SIZE
        ORDER BY size_label;
    """)

    if df_size.empty:
        st.info("Aucune donnée disponible.")
    else:
        total = df_size["NB_JOBS"].sum()
        df_size["PCT"] = (df_size["NB_JOBS"] / total * 100).round(1)

        col1, col2 = st.columns([1, 1])

        with col1:
            pie = (
                alt.Chart(df_size)
                .mark_arc(innerRadius=60)
                .encode(
                    theta=alt.Theta("NB_JOBS:Q"),
                    color=alt.Color(
                        "SIZE_LABEL:N",
                        legend=alt.Legend(title="Taille d'entreprise"),
                        scale=alt.Scale(scheme="tableau10"),
                    ),
                    tooltip=[
                        alt.Tooltip("SIZE_LABEL", title="Taille"),
                        alt.Tooltip("NB_JOBS", title="Nb offres"),
                        alt.Tooltip("PCT", title="Part (%)", format=".1f"),
                    ],
                )
                .properties(title="Donut — Part par taille", height=350)
            )
            st.altair_chart(pie, use_container_width=True)

        with col2:
            bar = (
                alt.Chart(df_size)
                .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
                .encode(
                    x=alt.X("SIZE_LABEL:N", title="Taille d'entreprise", sort=None),
                    y=alt.Y("NB_JOBS:Q", title="Nombre d'offres"),
                    color=alt.Color("SIZE_LABEL:N", legend=None, scale=alt.Scale(scheme="tableau10")),
                    tooltip=[
                        alt.Tooltip("SIZE_LABEL", title="Taille"),
                        alt.Tooltip("NB_JOBS", title="Nb offres"),
                        alt.Tooltip("PCT", title="Part (%)", format=".1f"),
                    ],
                )
                .properties(title="Barres — Nb d'offres par taille", height=350)
            )
            st.altair_chart(bar, use_container_width=True)

        # Tableau
        df_size_disp = df_size.copy()
        df_size_disp.columns = ["Taille d'entreprise", "Nb offres", "Part (%)"]
        st.dataframe(df_size_disp, use_container_width=True, hide_index=True)

# ══════════════════════════════════════════════
# TAB 4 — RÉPARTITION PAR SECTEUR D'ACTIVITÉ
# ══════════════════════════════════════════════
with tab4:
    st.subheader("Répartition des offres par secteur d'activité")

    top_n_sector = st.slider("Afficher le top N des secteurs", 5, 30, 15, key="tab4_n")

    df_sector = run_query(f"""
        SELECT industry, nb_jobs
        FROM LINKEDIN.GOLD.REPORT_JOBS_BY_INDUSTRY
        WHERE industry IS NOT NULL
        ORDER BY nb_jobs DESC
        LIMIT {top_n_sector};
    """)

    if df_sector.empty:
        st.info("Aucune donnée disponible.")
    else:
        total_s = df_sector["NB_JOBS"].sum()
        df_sector["PCT"] = (df_sector["NB_JOBS"] / total_s * 100).round(1)

        chart_s = (
            alt.Chart(df_sector)
            .mark_bar(color="#0EA5E9", cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
            .encode(
                x=alt.X("NB_JOBS:Q", title="Nombre d'offres"),
                y=alt.Y("INDUSTRY:N", sort="-x", title="Secteur"),
                tooltip=[
                    alt.Tooltip("INDUSTRY", title="Secteur"),
                    alt.Tooltip("NB_JOBS", title="Nb offres"),
                    alt.Tooltip("PCT", title="Part (%)", format=".1f"),
                ],
            )
            .properties(height=max(300, top_n_sector * 28))
        )
        st.altair_chart(chart_s, use_container_width=True)

        st.markdown("##### Répartition en pourcentages")
        chart_pct = (
            alt.Chart(df_sector)
            .mark_arc()
            .encode(
                theta="NB_JOBS:Q",
                color=alt.Color("INDUSTRY:N", legend=alt.Legend(title="Secteur"), scale=alt.Scale(scheme="category20")),
                tooltip=[
                    alt.Tooltip("INDUSTRY", title="Secteur"),
                    alt.Tooltip("PCT", title="Part (%)", format=".1f"),
                ],
            )
            .properties(height=350)
        )
        st.altair_chart(chart_pct, use_container_width=True)

        with st.expander("Voir le tableau complet"):
            df_sector_disp = df_sector.copy()
            df_sector_disp.columns = ["Secteur", "Nb offres", "Part (%)"]
            st.dataframe(df_sector_disp, use_container_width=True, hide_index=True)

# ══════════════════════════════════════════════
# TAB 5 — RÉPARTITION PAR TYPE D'EMPLOI
# ══════════════════════════════════════════════
with tab5:
    st.subheader("Répartition des offres par type d'emploi")

    df_wt = run_query("""
        SELECT work_type, nb_jobs
        FROM LINKEDIN.GOLD.REPORT_JOBS_BY_WORK_TYPE
        ORDER BY nb_jobs DESC;
    """)

    if df_wt.empty:
        st.info("Aucune donnée disponible.")
    else:
        total_wt = df_wt["NB_JOBS"].sum()
        df_wt["PCT"] = (df_wt["NB_JOBS"] / total_wt * 100).round(1)

        col_w1, col_w2 = st.columns([1, 1])

        with col_w1:
            pie_wt = (
                alt.Chart(df_wt)
                .mark_arc(innerRadius=50)
                .encode(
                    theta="NB_JOBS:Q",
                    color=alt.Color(
                        "WORK_TYPE:N",
                        legend=alt.Legend(title="Type d'emploi"),
                        scale=alt.Scale(scheme="set2"),
                    ),
                    tooltip=[
                        alt.Tooltip("WORK_TYPE", title="Type"),
                        alt.Tooltip("NB_JOBS", title="Nb offres"),
                        alt.Tooltip("PCT", title="Part (%)", format=".1f"),
                    ],
                )
                .properties(title="Donut — Répartition par type", height=350)
            )
            st.altair_chart(pie_wt, use_container_width=True)

        with col_w2:
            bar_wt = (
                alt.Chart(df_wt)
                .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
                .encode(
                    x=alt.X("WORK_TYPE:N", title="Type d'emploi", sort="-y"),
                    y=alt.Y("NB_JOBS:Q", title="Nombre d'offres"),
                    color=alt.Color("WORK_TYPE:N", legend=None, scale=alt.Scale(scheme="set2")),
                    tooltip=[
                        alt.Tooltip("WORK_TYPE", title="Type"),
                        alt.Tooltip("NB_JOBS", title="Nb offres"),
                        alt.Tooltip("PCT", title="Part (%)", format=".1f"),
                    ],
                )
                .properties(title="Barres — Nb d'offres par type", height=350)
            )
            st.altair_chart(bar_wt, use_container_width=True)

        # Tableau récap
        df_wt_disp = df_wt.copy()
        df_wt_disp.columns = ["Type d'emploi", "Nb offres", "Part (%)"]
        st.dataframe(df_wt_disp, use_container_width=True, hide_index=True)

        # Bonus : remote vs on-site par type
        st.markdown("---")
        st.markdown("##### Remote vs On-site par type d'emploi")
        df_remote = run_query("""
            SELECT
                work_type,
                SUM(CASE WHEN is_remote THEN 1 ELSE 0 END) AS remote,
                SUM(CASE WHEN NOT is_remote THEN 1 ELSE 0 END) AS on_site
            FROM LINKEDIN.SILVER.JOB_POSTINGS
            WHERE work_type IS NOT NULL
            GROUP BY 1
            ORDER BY (remote + on_site) DESC;
        """)
        if not df_remote.empty:
            df_remote_melt = df_remote.melt(
                id_vars="WORK_TYPE", value_vars=["REMOTE", "ON_SITE"],
                var_name="MODE", value_name="NB"
            )
            chart_r = (
                alt.Chart(df_remote_melt)
                .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
                .encode(
                    x=alt.X("WORK_TYPE:N", title="Type d'emploi"),
                    y=alt.Y("NB:Q", title="Nombre d'offres"),
                    color=alt.Color("MODE:N", scale=alt.Scale(
                        domain=["REMOTE", "ON_SITE"],
                        range=["#22C55E", "#94A3B8"]
                    ), legend=alt.Legend(title="Mode")),
                    tooltip=["WORK_TYPE", "MODE", "NB"],
                )
                .properties(height=300)
            )
            st.altair_chart(chart_r, use_container_width=True)

# ══════════════════════════════════════════════
# TAB 6 — EXPLORATION LIBRE
# ══════════════════════════════════════════════
with tab6:
    st.subheader("Exploration libre des offres d'emploi")

    # Filtres
    col_f1, col_f2, col_f3 = st.columns(3)

    with col_f1:
        wt_opts = run_query("""
            SELECT DISTINCT work_type FROM LINKEDIN.SILVER.JOB_POSTINGS
            WHERE work_type IS NOT NULL ORDER BY 1;
        """)["WORK_TYPE"].tolist()
        sel_wt = st.multiselect("Type d'emploi", wt_opts, default=wt_opts[:3] if len(wt_opts) >= 3 else wt_opts)

    with col_f2:
        exp_opts = run_query("""
            SELECT DISTINCT experience_level FROM LINKEDIN.SILVER.JOB_POSTINGS
            WHERE experience_level IS NOT NULL ORDER BY 1;
        """)["EXPERIENCE_LEVEL"].tolist()
        sel_exp = st.multiselect("Niveau d'expérience", exp_opts, default=exp_opts)

    with col_f3:
        remote_sel = st.radio("Remote", ["Tous", "Remote uniquement", "On-site uniquement"])

    salary_min, salary_max = st.slider(
        "Fourchette de salaire médian ($)",
        min_value=0, max_value=500000,
        value=(0, 300000), step=5000,
        key="tab6_salary",
    )

    where_clauses = []
    if sel_wt:
        wt_str = ", ".join(f"'{w}'" for w in sel_wt)
        where_clauses.append(f"work_type IN ({wt_str})")
    if sel_exp:
        exp_str = ", ".join(f"'{e}'" for e in sel_exp)
        where_clauses.append(f"experience_level IN ({exp_str})")
    if remote_sel == "Remote uniquement":
        where_clauses.append("is_remote = TRUE")
    elif remote_sel == "On-site uniquement":
        where_clauses.append("is_remote = FALSE")
    where_clauses.append(f"(med_salary IS NULL OR (med_salary >= {salary_min} AND med_salary <= {salary_max}))")

    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    df_explore = run_query(f"""
        SELECT
            job_id, title, company_name, location,
            work_type, experience_level, is_remote,
            min_salary, med_salary, max_salary,
            currency, listed_at
        FROM LINKEDIN.SILVER.JOB_POSTINGS
        {where_sql}
        ORDER BY listed_at DESC NULLS LAST
        LIMIT 500;
    """)

    st.markdown(f"**{len(df_explore):,} offres** correspondent à vos filtres (max 500 affichées)")

    if not df_explore.empty:
        # Renommage colonnes pour affichage
        df_display = df_explore.rename(columns={
            "JOB_ID": "ID", "TITLE": "Titre", "COMPANY_NAME": "Entreprise",
            "LOCATION": "Localisation", "WORK_TYPE": "Type", "EXPERIENCE_LEVEL": "Expérience",
            "IS_REMOTE": "Remote", "MIN_SALARY": "Sal. min", "MED_SALARY": "Sal. médian",
            "MAX_SALARY": "Sal. max", "CURRENCY": "Devise", "LISTED_AT": "Date publication"
        })
        st.dataframe(df_display, use_container_width=True, hide_index=True)

        # Distribution des salaires médians
        df_sal_dist = df_explore.dropna(subset=["MED_SALARY"])
        if not df_sal_dist.empty:
            st.markdown("##### Distribution des salaires médians (offres filtrées)")
            hist = (
                alt.Chart(df_sal_dist)
                .mark_bar(color="#F59E0B", opacity=0.85)
                .encode(
                    x=alt.X("MED_SALARY:Q", bin=alt.Bin(maxbins=30), title="Salaire médian ($)"),
                    y=alt.Y("count():Q", title="Nb d'offres"),
                    tooltip=["count():Q"],
                )
                .properties(height=280)
            )
            st.altair_chart(hist, use_container_width=True)

# ──────────────────────────────────────────────
# FOOTER
# ──────────────────────────────────────────────
st.divider()
st.caption("MBAESG — Architecture Big Data • Snowflake + Streamlit • Données LinkedIn")
```
## Commentaires explicatifs pour la réalisation du code Python et de la génération du Streamlit
### Les résultats obtenus
Vue décisionnelle large sur le marché de l’emploi.
### À travers les visualisations utilisées, cela permet de :

Rendre la lecture des données beaucoup plus claire et compréhensible.

Partir du global à une vue plus détaillée. L’expérience utilisateur est donc améliorée car seuls les éléments pertinents sont affichés.

### Les KPI affichés permettent de connaître directement :
Le total d’offres et d’entreprises uniques.

La fonction % Remote sert à mesurer la flexibilité du marché et donc d’afficher le nombre d’offres proposées.

Enfin, le salaire moyen permet d’offrir aux recruteurs et candidats potentiels un point de repère direct.

## Analyse par onglets
### Concernant les onglets 1 et 2 : Affichage des tendances par industries
La création du graphique ALTAIR permet de montrer les métiers dominant chaque secteur. Dans l’onglet 2, on vient identifier les postes ayant la valeur ajoutée la plus importante par industrie, et donc d’identifier les secteurs les plus intéressants.

### Puis pour les onglets 3, 4 et 5 : La structure des entreprises et les contrats
Le graphique en donut permet de comprendre si le marché professionnel est dominé par les PME ou les Grands Groupes. Et le graphique à barres empilées donne à l’utilisateur une mise en commun intéressante entre le type de contrat et les possibilités de travail (remote par exemple).

### Et enfin pour l’onglet 6 : Exploration libre et dynamique
Le premier filtre permet d’intégrer plusieurs critères en même temps et de les isoler selon les données à afficher souhaitées. L’histogramme complète cela en démontrant si les salaires sont regroupés (donc que nous sommes dans un marché homogène) ou un marché connaissant des disparités.

## Aspects techniques et fonctions utilisées :
Afin de pouvoir générer d’autres charts que ceux à colonnes, la bibliothèque Plotly a été intégrée à l’application. Cela permet à l’utilisateur de pouvoir survoler une barre afin d’en visualiser la valeur exacte. Et également de pouvoir contrôler et choisir la couleur d’affichage afin de garder un aspect visuel professionnel.

Dans le code Python, l’utilisation de la fonction TRIM permet de venir supprimer tous les espaces inutiles afin d’être sûr que la comparaison fonctionne tout le temps. L’utilisation de .replace est utilisée pour l’insertion de variables Python dans les requêtes SQL. Cela permet d’éviter les erreurs de syntaxe si, par exemple, une apostrophe est présente dans un nom d’entreprise.

Altair a été importé comme package. Altair permet d’élargir la création des graphiques et d’améliorer la visualisation finale des données.

L’interface est rendue flexible par l’utilisation de st.tabs et st.columns. Cela évite le scroll de la page à l’infini et donne donc accès à l’information qui l’intéresse directement.

Et enfin, l’utilisation de l’architecture Snowflake native à l’application car le code est conçu pour être déployé dans l’interface elle-même. On obtient donc une sécurité maximale des données car pas de présence sur les réseaux et gérées dans un seul environnement de travail.
