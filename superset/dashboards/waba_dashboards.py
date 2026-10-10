"""Tableaux de bord Superset « as code » : jeux de données, graphiques et tableaux de bord WABA.

Une seule source versionnée -> un bundle d'import Superset (format v1 : YAML zippé), importé à chaque
déploiement par le Job d'initialisation (`superset import-dashboards`, mode écrasement). Les UUID sont
dérivés des noms (uuid5) : un ré-import met à jour les objets au lieu de les dupliquer, et une modification
faite dans l'UI est remplacée par la version du dépôt (source de vérité = Git).

  python waba_dashboards.py bundle.zip [--sqlalchemy-uri trino://superset@trino.serving...:8080/lakehouse]
  python waba_dashboards.py --check-sql | ./scripts/k8s/trino.sh     # exécute chaque jeu de données dans Trino

Conception :
  * jeux de données VIRTUELS (SQL Trino) au grain du graphique, calculés sur les tables Gold (+ Silver
    pour les vues fines : heure de paiement, produits) -> Superset ne fait que des agrégats légers ;
  * chaque jeu de données expose `country_code` : filtre « Pays » natif sur chaque tableau de bord et
    Row Level Security par pays (étape 9.6, rôle country_analyst) sans toucher aux graphiques ;
  * les ratios sont recalculés à partir des sommes (SUM(num) / SUM(den)), jamais moyennés : ils restent
    justes quel que soit le filtre appliqué.
"""
from __future__ import annotations

import argparse
import io
import re
import sys
import uuid
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timezone

NS = uuid.UUID("6f1d4a52-8c55-4b5e-9a57-7a3e0c1b2d01")   # espace de noms WABA (stable)
VERSION = "1.0.0"
DATABASE_NAME = "WABA Lakehouse (Trino)"
DEFAULT_URI = "trino://superset@trino.serving.svc.cluster.local:8080/lakehouse"
GOLD, SILVER = "lakehouse.gold", "lakehouse.silver"

# Seuils réglementaires et contractuels (affichés dans les graphiques, cf. spark/waba_spark/gold.py)
NPL_GREEN, NPL_RED = 3, 5                 # % : vert < 3, orange 3-5, rouge > 5 (seuil BCEAO 5 %)
CIMA_LOSS_RATIO = 70                      # % : seuil CIMA
SLA_WORKING_DAYS = {"IARD": 10, "VIE": 20}  # délai contractuel de règlement des sinistres (hypothèse WABA)
GREEN, ORANGE, RED = "#ACE1C4", "#FDE380", "#EFA1AA"


# Libellés affichés (en-têtes de tableaux, filtres) ; les noms techniques restent ceux des tables Gold
LABELS = {
    "country_code": "Pays", "report_month": "Mois", "business_line": "Ligne métier", "revenue_eur": "Revenus (EUR)",
    "active_customers": "Clients actifs", "product": "Produit", "subscribers": "Souscripteurs",
    "product_rank": "Rang", "npl_outstanding_eur": "Encours en souffrance (EUR)",
    "total_outstanding_eur": "Encours total (EUR)", "npl_loans_count": "Prêts en souffrance",
    "loans_count": "Prêts", "npl_ratio_pct": "NPL (%)", "bceao_status": "Statut BCEAO",
    "is_latest": "Dernier mois", "product_line": "Produit", "insurance_branch": "Branche",
    "premiums_eur": "Primes (EUR)", "claims_paid_eur": "Sinistres payés (EUR)", "event_day": "Jour",
    "aml_events": "Événements AML", "amount_eur": "Montant (EUR)", "segment": "Pays · branche",
    "closed_claims": "Sinistres clos", "weighted_working_days": "Jours ouvrés cumulés",
    "sla_days": "SLA (jours ouvrés)", "hour_label": "Heure", "payments": "Paiements", "corridor": "Corridor",
    "receiver_country": "Pays bénéficiaire", "transfers": "Transferts", "corridor_rank": "Rang",
    "operator": "Opérateur", "txn_count": "Transactions", "failed_count": "Échecs",
}


def uid(kind: str, name: str) -> str:
    return str(uuid.uuid5(NS, f"{kind}:{name}"))


def slug(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", text).strip("_")[:60]


# --------------------------------------------------------------------------- #
# Jeux de données (SQL Trino)
# --------------------------------------------------------------------------- #
@dataclass
class Dataset:
    name: str
    description: str
    sql: str
    columns: dict[str, str]                     # nom -> type Trino ; DATE / TIMESTAMP = temporel
    main_dttm_col: str | None = None

    @property
    def uuid(self) -> str:
        return uid("dataset", self.name)


DATASETS = [
    Dataset(
        "ds_revenus_lignes_metier",
        "Revenus mensuels par pays et ligne métier (EUR) : commissions + intérêts (banque, mobile money), "
        "primes encaissées (assurance).",
        f"""SELECT report_month, country_code, business_line, SUM(revenue_eur) AS revenue_eur
FROM (
  SELECT report_month, country_code,
         CASE WHEN entity_type = 'MOBILE_MONEY' THEN 'Mobile Money' ELSE 'Banque' END AS business_line,
         total_revenue_eur AS revenue_eur
  FROM {GOLD}.customer_arpu_monthly
  UNION ALL
  SELECT report_month, country_code, 'Assurance' AS business_line, premiums_eur AS revenue_eur
  FROM {GOLD}.loss_ratio_by_product
) t
GROUP BY report_month, country_code, business_line""",
        {"report_month": "DATE", "country_code": "VARCHAR", "business_line": "VARCHAR", "revenue_eur": "DOUBLE"},
        "report_month"),
    Dataset(
        "ds_arpc_mensuel",
        "Revenu moyen par client (ARPC) mensuel par pays : revenus / clients actifs (par entité).",
        f"""SELECT report_month, country_code,
       SUM(total_revenue_eur) AS revenue_eur, SUM(active_customers) AS active_customers
FROM {GOLD}.customer_arpu_monthly
GROUP BY report_month, country_code""",
        {"report_month": "DATE", "country_code": "VARCHAR", "revenue_eur": "DOUBLE", "active_customers": "BIGINT"},
        "report_month"),
    Dataset(
        "ds_top_produits",
        "Produits souscrits par pays : assurance (clients ayant payé une prime), banque (comptes ouverts "
        "et prêts par type). Rang par pays.",
        f"""WITH s AS (
  SELECT country_code, 'Assurance' AS business_line, product_line AS product,
         COUNT(DISTINCT customer_id) AS subscribers
  FROM {SILVER}.insurance_operations WHERE is_premium
  GROUP BY country_code, product_line
  UNION ALL
  SELECT country_code, 'Banque' AS business_line, 'COMPTE_' || account_type AS product, COUNT(*) AS subscribers
  FROM {SILVER}.accounts
  WHERE entity_type IN ('BANK', 'MICROFINANCE') AND account_type IN ('CURRENT', 'SAVINGS') AND status <> 'CLOSED'
  GROUP BY country_code, account_type
  UNION ALL
  SELECT country_code, 'Banque' AS business_line, 'PRET_' || loan_type AS product,
         COUNT(DISTINCT loan_account_id) AS subscribers
  FROM {SILVER}.loan_repayments
  GROUP BY country_code, loan_type
)
SELECT country_code, business_line, product, subscribers,
       ROW_NUMBER() OVER (PARTITION BY country_code ORDER BY subscribers DESC, product) AS product_rank
FROM s""",
        {"country_code": "VARCHAR", "business_line": "VARCHAR", "product": "VARCHAR", "subscribers": "BIGINT",
         "product_rank": "BIGINT"}),
    Dataset(
        "ds_npl_mensuel",
        "Créances en souffrance (NPL) par pays et par mois, toutes entités ; is_latest = dernier mois "
        f"disponible du pays. Statut BCEAO : vert < {NPL_GREEN} %, orange {NPL_GREEN}-{NPL_RED} %, rouge > {NPL_RED} %.",
        f"""WITH m AS (
  SELECT country_code, report_month,
         SUM(npl_outstanding_eur) AS npl_outstanding_eur, SUM(total_outstanding_eur) AS total_outstanding_eur,
         SUM(npl_loans_count) AS npl_loans_count, SUM(loans_count) AS loans_count
  FROM {GOLD}.npl_ratio_by_country
  GROUP BY country_code, report_month
)
SELECT country_code, report_month, ROUND(npl_outstanding_eur) AS npl_outstanding_eur,
       ROUND(total_outstanding_eur) AS total_outstanding_eur, npl_loans_count, loans_count,
       ROUND(100 * npl_outstanding_eur / NULLIF(total_outstanding_eur, 0), 2) AS npl_ratio_pct,
       CASE WHEN npl_outstanding_eur < {NPL_GREEN / 100} * total_outstanding_eur THEN 'VERT'
            WHEN npl_outstanding_eur <= {NPL_RED / 100} * total_outstanding_eur THEN 'ORANGE'
            ELSE 'ROUGE' END AS bceao_status,
       report_month = MAX(report_month) OVER (PARTITION BY country_code) AS is_latest
FROM m""",
        {"country_code": "VARCHAR", "report_month": "DATE", "npl_outstanding_eur": "DOUBLE",
         "total_outstanding_eur": "DOUBLE", "npl_loans_count": "BIGINT", "loans_count": "BIGINT",
         "npl_ratio_pct": "DOUBLE", "bceao_status": "VARCHAR", "is_latest": "BOOLEAN"},
        "report_month"),
    Dataset(
        "ds_loss_ratio_12m",
        f"Sinistres payés et primes par produit et pays sur les 12 derniers mois disponibles (seuil CIMA {CIMA_LOSS_RATIO} %).",
        f"""WITH last AS (SELECT MAX(report_month) AS m FROM {GOLD}.loss_ratio_by_product)
SELECT l.country_code, l.product_line, l.insurance_branch,
       SUM(l.premiums_eur) AS premiums_eur, SUM(l.claims_paid_eur) AS claims_paid_eur
FROM {GOLD}.loss_ratio_by_product l CROSS JOIN last
WHERE l.report_month > date_add('month', -12, last.m)
GROUP BY l.country_code, l.product_line, l.insurance_branch""",
        {"country_code": "VARCHAR", "product_line": "VARCHAR", "insurance_branch": "VARCHAR",
         "premiums_eur": "DOUBLE", "claims_paid_eur": "DOUBLE"}),
    Dataset(
        "ds_aml_journalier",
        "Événements AML (déclarations de seuil, speed layer) par jour et par pays, 30 derniers jours d'activité.",
        f"""WITH a AS (
  SELECT CAST(event_time AS DATE) AS event_day, country_code, amount_eur FROM {GOLD}.rt_aml_events
), last AS (SELECT MAX(event_day) AS d FROM a)
SELECT a.event_day, a.country_code, COUNT(*) AS aml_events, SUM(a.amount_eur) AS amount_eur
FROM a CROSS JOIN last
WHERE a.event_day > date_add('day', -30, last.d)
GROUP BY a.event_day, a.country_code""",
        {"event_day": "DATE", "country_code": "VARCHAR", "aml_events": "BIGINT", "amount_eur": "DOUBLE"},
        "event_day"),
    Dataset(
        "ds_sinistres_sla",
        "Délai de règlement des sinistres clos (jours ouvrés) par pays et branche, 12 derniers mois, "
        f"face au SLA contractuel (IARD {SLA_WORKING_DAYS['IARD']} j, VIE {SLA_WORKING_DAYS['VIE']} j).",
        f"""WITH last AS (SELECT MAX(report_month) AS m FROM {GOLD}.claims_processing_time)
SELECT c.country_code, c.insurance_branch, c.country_code || ' · ' || c.insurance_branch AS segment,
       SUM(c.closed_claims_count) AS closed_claims,
       SUM(c.avg_working_days * c.closed_claims_count) AS weighted_working_days,
       CASE c.insurance_branch WHEN 'IARD' THEN {SLA_WORKING_DAYS['IARD']} ELSE {SLA_WORKING_DAYS['VIE']} END AS sla_days
FROM {GOLD}.claims_processing_time c CROSS JOIN last
WHERE c.report_month > date_add('month', -12, last.m)
GROUP BY c.country_code, c.insurance_branch""",
        {"country_code": "VARCHAR", "insurance_branch": "VARCHAR", "segment": "VARCHAR",
         "closed_claims": "BIGINT", "weighted_working_days": "DOUBLE", "sla_days": "INTEGER"}),
    Dataset(
        "ds_mobile_money_horaire",
        "Paiements mobile money par pays et heure de la journée (UTC = heure d'Abidjan).",
        f"""SELECT country_code, lpad(CAST(txn_hour AS VARCHAR), 2, '0') || 'h' AS hour_label,
       COUNT(*) AS payments, SUM(amount_eur) AS amount_eur
FROM {SILVER}.mobile_money_payments
GROUP BY country_code, txn_hour""",
        {"country_code": "VARCHAR", "hour_label": "VARCHAR", "payments": "BIGINT", "amount_eur": "DOUBLE"}),
    Dataset(
        "ds_corridors",
        "Transferts transfrontaliers par corridor (pays émetteur -> pays bénéficiaire), rang par montant.",
        f"""WITH c AS (
  SELECT corridor, sender_country, receiver_country,
         SUM(transfer_count) AS transfers, SUM(total_amount_eur) AS amount_eur
  FROM {GOLD}.cross_border_transfers
  GROUP BY corridor, sender_country, receiver_country
)
SELECT sender_country || ' → ' || receiver_country AS corridor, sender_country AS country_code,
       receiver_country, transfers, amount_eur,
       ROW_NUMBER() OVER (ORDER BY amount_eur DESC) AS corridor_rank
FROM c""",
        {"corridor": "VARCHAR", "country_code": "VARCHAR", "receiver_country": "VARCHAR",
         "transfers": "BIGINT", "amount_eur": "DOUBLE", "corridor_rank": "BIGINT"}),
    Dataset(
        "ds_mobile_money_echecs",
        "Transactions mobile money et échecs par pays et opérateur.",
        f"""SELECT country_code, operator, SUM(txn_count) AS txn_count, SUM(failed_count) AS failed_count,
       SUM(total_amount_eur) AS amount_eur
FROM {GOLD}.mobile_money_daily_flow
GROUP BY country_code, operator""",
        {"country_code": "VARCHAR", "operator": "VARCHAR", "txn_count": "BIGINT", "failed_count": "BIGINT",
         "amount_eur": "DOUBLE"}),
]
DS = {d.name: d for d in DATASETS}


# --------------------------------------------------------------------------- #
# Graphiques
# --------------------------------------------------------------------------- #
def metric(label: str, sql: str) -> dict:
    return {"expressionType": "SQL", "sqlExpression": sql, "label": label, "hasCustomLabel": True,
            "optionName": f"metric_{slug(label).lower()}"}


def sql_filter(sql: str) -> dict:
    return {"expressionType": "SQL", "sqlExpression": sql, "clause": "WHERE"}


def no_time_filter(col: str) -> dict:
    return {"clause": "WHERE", "comparator": "No filter", "expressionType": "SIMPLE",
            "operator": "TEMPORAL_RANGE", "subject": col}


def cond(column: str, operator: str, color: str, value=None, left=None, right=None) -> dict:
    c = {"column": column, "operator": operator, "colorScheme": color, "useGradient": False}
    if value is not None:
        c["targetValue"] = value
    if left is not None:
        c["targetValueLeft"], c["targetValueRight"] = left, right
    return c


REVENUE = metric("Revenus (EUR)", "SUM(revenue_eur)")
ARPC = metric("ARPC (EUR / client)", "SUM(revenue_eur) / NULLIF(SUM(active_customers), 0)")
NPL_PCT = metric("NPL (%)", "100 * SUM(npl_outstanding_eur) / NULLIF(SUM(total_outstanding_eur), 0)")
LOSS_PCT = metric("Loss ratio 12 mois (%)", "100 * SUM(claims_paid_eur) / NULLIF(SUM(premiums_eur), 0)")
AML = metric("Événements AML", "SUM(aml_events)")
DELAY = metric("Délai moyen (jours ouvrés)", "SUM(weighted_working_days) / NULLIF(SUM(closed_claims), 0)")
SLA = metric("SLA contractuel (jours ouvrés)", "MAX(sla_days)")
PAYMENTS = metric("Paiements", "SUM(payments)")
CORRIDOR_AMOUNT = metric("Montant transféré (EUR)", "SUM(amount_eur)")
FAILURE_PCT = metric("Taux d'échec (%)", "100.0 * SUM(failed_count) / NULLIF(SUM(txn_count), 0)")

BAR_DEFAULTS = {
    "color_scheme": "supersetColors", "legendOrientation": "top", "legendType": "scroll", "show_legend": True,
    "rich_tooltip": True, "row_limit": 10000, "order_desc": True, "only_total": True, "truncate_metric": True,
    "x_axis_title_margin": 15, "y_axis_title_margin": 30, "y_axis_title_position": "Left",
    "y_axis_bounds": [None, None], "annotation_layers": [], "extra_form_data": {}, "comparison_type": "values",
    "forecastInterval": 0.8, "forecastPeriods": 10, "sort_series_type": "sum", "show_empty_columns": True,
    "truncateXAxis": True, "tooltipTimeFormat": "smart_date", "x_axis_time_format": "smart_date",
}


@dataclass
class Chart:
    name: str
    dataset: str
    viz_type: str
    params: dict
    description: str = ""
    width: int = 6
    height: int = 50

    @property
    def uuid(self) -> str:
        return uid("chart", self.name)


def bar(name, dataset, x, metrics, groupby=(), stack=False, horizontal=False, filters=(), y_format="SMART_NUMBER",
        sort_by_metric=False, sort_asc=False, row_limit=10000, **kw) -> Chart:
    p = {**BAR_DEFAULTS, "viz_type": "echarts_timeseries_bar", "x_axis": x, "xAxisForceCategorical": True,
         "metrics": list(metrics), "groupby": list(groupby), "adhoc_filters": list(filters),
         "orientation": "horizontal" if horizontal else "vertical", "y_axis_format": y_format,
         "stack": "Stack" if stack else None, "show_value": False, "row_limit": row_limit}
    if sort_by_metric:
        # sort_asc=False sur des barres horizontales : plus gros montant en haut (vérifié au rendu)
        p.update({"x_axis_sort": metrics[0]["label"], "x_axis_sort_asc": sort_asc})
    else:
        p.update({"x_axis_sort_asc": True})
    return Chart(name, dataset, "echarts_timeseries_bar", p, **kw)


def line(name, dataset, x, metrics, groupby=(), time_grain="P1M", y_format="SMART_NUMBER", **kw) -> Chart:
    p = {**BAR_DEFAULTS, "viz_type": "echarts_timeseries_line", "x_axis": x, "time_grain_sqla": time_grain,
         "metrics": list(metrics), "groupby": list(groupby), "adhoc_filters": [no_time_filter(x)],
         "seriesType": "line", "markerEnabled": True, "markerSize": 6, "y_axis_format": y_format,
         "x_axis_sort_asc": True, "opacity": 0.2}
    return Chart(name, dataset, "echarts_timeseries_line", p, **kw)


def big_number(name, dataset, m, y_format="SMART_NUMBER", subheader="", filters=(), **kw) -> Chart:
    p = {"viz_type": "big_number_total", "metric": m, "adhoc_filters": list(filters), "header_font_size": 0.4,
         "subheader": subheader, "subheader_font_size": 0.15, "y_axis_format": y_format, "extra_form_data": {}}
    return Chart(name, dataset, "big_number_total", p, **kw)


def table(name, dataset, columns, order_by, filters=(), conditional=(), page_length=10, **kw) -> Chart:
    p = {"viz_type": "table", "query_mode": "raw", "all_columns": list(columns), "groupby": [], "metrics": [],
         "order_by_cols": [f'["{c}", {str(asc).lower()}]' for c, asc in order_by], "adhoc_filters": list(filters),
         "row_limit": 1000, "server_page_length": page_length, "show_cell_bars": False, "color_pn": False,
         "include_search": False, "allow_render_html": False, "conditional_formatting": list(conditional),
         "table_timestamp_format": "%Y-%m", "percent_metrics": [], "extra_form_data": {}}
    return Chart(name, dataset, "table", p, **kw)


CHARTS = [
    # --- Dashboard 1 : Performance commerciale groupe
    big_number("Revenus consolidés du groupe (EUR)", "ds_revenus_lignes_metier", REVENUE, ",.0f",
               "toutes lignes métier, toute la période", width=4, height=30),
    big_number("ARPC moyen groupe (EUR / client / mois)", "ds_arpc_mensuel", ARPC, ",.2f",
               "revenus / clients actifs", width=4, height=30),
    big_number("Pays couverts", "ds_revenus_lignes_metier", metric("Pays", "COUNT(DISTINCT country_code)"),
               "d", "UEMOA + Guinée + Ghana", width=4, height=30),
    bar("Revenus consolidés par pays et ligne métier", "ds_revenus_lignes_metier", "country_code", [REVENUE],
        groupby=["business_line"], y_format=",.0f", width=6,
        description="Barres groupées : Banque (commissions + intérêts), Assurance (primes), Mobile Money (frais)."),
    Chart("Contribution des pays aux revenus du groupe", "ds_revenus_lignes_metier", "world_map",
          {"viz_type": "world_map", "entity": "country_code", "country_fieldtype": "cca2", "metric": REVENUE,
           "secondary_metric": REVENUE, "show_bubbles": False, "max_bubble_size": "25", "adhoc_filters": [],
           "row_limit": 50000, "color_picker": {"r": 0, "g": 122, "b": 135, "a": 1},
           "linear_color_scheme": "schemeBlues", "color_by": "metric", "y_axis_format": ",.0f",
           "extra_form_data": {}},
          description="Carte choroplèthe : intensité de couleur = revenus cumulés du pays (EUR).", width=6),
    line("Évolution mensuelle de l'ARPC par pays", "ds_arpc_mensuel", "report_month", [ARPC],
         groupby=["country_code"], y_format=",.2f", width=6),
    table("Top 10 des produits souscrits par pays", "ds_top_produits",
          ["country_code", "product_rank", "business_line", "product", "subscribers"],
          [("country_code", True), ("product_rank", True)], filters=[sql_filter("product_rank <= 10")],
          page_length=10, width=6,
          description="Assurance : clients ayant payé une prime ; banque : comptes ouverts et prêts par type."),

    # --- Dashboard 2 : Risque & conformité réglementaire
    table("Taux NPL par pays — dernier mois (BCEAO)", "ds_npl_mensuel",
          ["country_code", "report_month", "npl_ratio_pct", "bceao_status", "npl_outstanding_eur",
           "total_outstanding_eur"],
          [("npl_ratio_pct", False)], filters=[sql_filter("is_latest")],
          conditional=[cond("npl_ratio_pct", "<", GREEN, value=NPL_GREEN),
                       cond("npl_ratio_pct", "≤ x ≤", ORANGE, left=NPL_GREEN, right=NPL_RED),
                       cond("npl_ratio_pct", ">", RED, value=NPL_RED)],
          width=6, description=f"Vert < {NPL_GREEN} %, orange {NPL_GREEN}-{NPL_RED} %, rouge > {NPL_RED} % (seuil BCEAO)."),
    line("Évolution du taux NPL par pays (%)", "ds_npl_mensuel", "report_month", [NPL_PCT],
         groupby=["country_code"], y_format=",.2f", width=6),
    Chart("Loss ratio par produit et pays — seuil CIMA 70 %", "ds_loss_ratio_12m", "pivot_table_v2",
          {"viz_type": "pivot_table_v2", "groupbyRows": ["product_line"], "groupbyColumns": ["country_code"],
           "metrics": [LOSS_PCT], "metricsLayout": "COLUMNS", "aggregateFunction": "Sum", "adhoc_filters": [],
           "row_limit": 10000, "order_desc": True, "valueFormat": ",.1f", "rowOrder": "key_a_to_z",
           "colOrder": "key_a_to_z", "rowTotals": False, "colTotals": False, "transposePivot": False,
           "combineMetric": False, "date_format": "smart_date", "extra_form_data": {},
           "conditional_formatting": [cond(LOSS_PCT["label"], ">", RED, value=CIMA_LOSS_RATIO),
                                      cond(LOSS_PCT["label"], "≤", GREEN, value=CIMA_LOSS_RATIO)]},
          description=f"Sinistres payés / primes encaissées sur 12 mois ; rouge = au-dessus du seuil CIMA ({CIMA_LOSS_RATIO} %).",
          width=6),
    line("Alertes AML par jour et par pays (30 jours)", "ds_aml_journalier", "event_day", [AML],
         groupby=["country_code"], time_grain="P1D", y_format="d", width=6,
         description="Transactions au-delà du seuil déclaratif (1 M XOF / 5 000 GHS), détectées en temps réel."),
    bar("Délai de règlement des sinistres vs SLA contractuel", "ds_sinistres_sla", "segment", [DELAY, SLA],
        y_format=",.1f", width=12,
        description=f"Jours ouvrés, 12 derniers mois. SLA : IARD {SLA_WORKING_DAYS['IARD']} j, VIE {SLA_WORKING_DAYS['VIE']} j."),

    # --- Dashboard 3 : Mobile Money & transferts
    Chart("Flux de paiements mobile money par pays et par heure", "ds_mobile_money_horaire", "heatmap_v2",
          {"viz_type": "heatmap_v2", "x_axis": "hour_label", "groupby": "country_code", "metric": PAYMENTS,
           "adhoc_filters": [], "row_limit": 10000, "linear_color_scheme": "schemeBlues",
           "normalize_across": "heatmap", "sort_x_axis": "alpha_asc", "sort_y_axis": "alpha_asc",
           "show_legend": True, "show_percentage": False, "show_values": False, "value_bounds": [None, None],
           "y_axis_format": "SMART_NUMBER", "xscale_interval": 1, "yscale_interval": 1,
           "left_margin": "auto", "bottom_margin": "auto", "extra_form_data": {}},
          description="Nombre de paiements par heure (UTC = heure d'Abidjan / Dakar / Accra).", width=12, height=60),
    bar("Top 5 des corridors transfrontaliers (montant)", "ds_corridors", "corridor", [CORRIDOR_AMOUNT],
        horizontal=True, y_format=",.0f", sort_by_metric=True, sort_asc=False, row_limit=5,
        width=6, description="Montant cumulé des transferts réussis par corridor émetteur → bénéficiaire."),
    bar("Taux d'échec mobile money par opérateur et par pays", "ds_mobile_money_echecs", "country_code",
        [FAILURE_PCT], groupby=["operator"], y_format=",.2f", width=6),
]
CH = {c.name: c for c in CHARTS}


# --------------------------------------------------------------------------- #
# Tableaux de bord
# --------------------------------------------------------------------------- #
@dataclass
class Dashboard:
    title: str
    slug: str
    description: str
    rows: list[list[str]]                       # lignes de graphiques (somme des largeurs <= 12)
    filter_dataset: str
    extra: dict = field(default_factory=dict)

    @property
    def uuid(self) -> str:
        return uid("dashboard", self.slug)


DASHBOARDS = [
    Dashboard("WABA — Performance commerciale groupe", "waba-performance-commerciale",
              "Revenus consolidés, ARPC et produits phares par pays et ligne métier.",
              [["Revenus consolidés du groupe (EUR)", "ARPC moyen groupe (EUR / client / mois)", "Pays couverts"],
               ["Revenus consolidés par pays et ligne métier", "Contribution des pays aux revenus du groupe"],
               ["Évolution mensuelle de l'ARPC par pays", "Top 10 des produits souscrits par pays"]],
              "ds_revenus_lignes_metier"),
    Dashboard("WABA — Risque & conformité réglementaire", "waba-risque-conformite",
              "NPL (BCEAO), loss ratio (CIMA), alertes AML et délais de règlement des sinistres.",
              [["Taux NPL par pays — dernier mois (BCEAO)", "Évolution du taux NPL par pays (%)"],
               ["Loss ratio par produit et pays — seuil CIMA 70 %", "Alertes AML par jour et par pays (30 jours)"],
               ["Délai de règlement des sinistres vs SLA contractuel"]],
              "ds_npl_mensuel"),
    Dashboard("WABA — Mobile Money & transferts", "waba-mobile-money",
              "Saisonnalité horaire des paiements, corridors transfrontaliers et taux d'échec par opérateur.",
              [["Flux de paiements mobile money par pays et par heure"],
               ["Top 5 des corridors transfrontaliers (montant)", "Taux d'échec mobile money par opérateur et par pays"]],
              "ds_mobile_money_echecs"),
]


def dashboard_charts(d: Dashboard) -> list[Chart]:
    return [CH[n] for row in d.rows for n in row]


def position(d: Dashboard) -> dict:
    pos = {"DASHBOARD_VERSION_KEY": "v2",
           "ROOT_ID": {"children": ["GRID_ID"], "id": "ROOT_ID", "type": "ROOT"},
           "HEADER_ID": {"id": "HEADER_ID", "meta": {"text": d.title}, "type": "HEADER"},
           "GRID_ID": {"children": [], "id": "GRID_ID", "parents": ["ROOT_ID"], "type": "GRID"}}
    for i, row in enumerate(d.rows):
        row_id = f"ROW-{d.slug}-{i}"
        pos["GRID_ID"]["children"].append(row_id)
        pos[row_id] = {"children": [], "id": row_id, "meta": {"background": "BACKGROUND_TRANSPARENT"},
                       "parents": ["ROOT_ID", "GRID_ID"], "type": "ROW"}
        for name in row:
            c = CH[name]
            cid = f"CHART-{slug(c.uuid)[:12]}"
            pos[row_id]["children"].append(cid)
            pos[cid] = {"children": [], "id": cid, "type": "CHART", "parents": ["ROOT_ID", "GRID_ID", row_id],
                        # chartId : valeur provisoire exigée par l'import, remplacée par l'id réel en base
                        "meta": {"height": c.height, "width": c.width, "sliceName": c.name, "uuid": c.uuid,
                                 "chartId": CHARTS.index(c) + 1}}
    return pos


def country_filter(d: Dashboard) -> dict:
    return {"id": f"NATIVE_FILTER-pays-{d.slug}"[:60], "name": "Pays", "filterType": "filter_select",
            "type": "NATIVE_FILTER", "targets": [{"column": {"name": "country_code"},
                                                  "datasetUuid": DS[d.filter_dataset].uuid}],
            "controlValues": {"enableEmptyFilter": False, "defaultToFirstItem": False, "multiSelect": True,
                              "searchAllOptions": False, "inverseSelection": False},
            "defaultDataMask": {"extraFormData": {}, "filterState": {}, "ownState": {}},
            "cascadeParentIds": [], "scope": {"rootPath": ["ROOT_ID"], "excluded": []}, "description":
            "Restreint tous les graphiques aux pays choisis (colonne country_code de chaque jeu de données)."}


# --------------------------------------------------------------------------- #
# Bundle d'import
# --------------------------------------------------------------------------- #
def _dump(obj: dict) -> str:
    import yaml  # importé ici : --check-sql fonctionne avec le Python de la VM, sans PyYAML

    return yaml.safe_dump(obj, allow_unicode=True, sort_keys=False, width=1000)


def database_yaml(uri: str) -> dict:
    return {"database_name": DATABASE_NAME, "sqlalchemy_uri": uri, "cache_timeout": None,
            "expose_in_sqllab": True, "allow_run_async": False, "allow_ctas": False, "allow_cvas": False,
            "allow_dml": False, "allow_file_upload": False,
            "extra": {"allows_virtual_table_explore": True, "metadata_params": {}, "engine_params": {},
                      "metadata_cache_timeout": {}, "schemas_allowed_for_file_upload": []},
            "uuid": uid("database", DATABASE_NAME), "version": VERSION}


def dataset_yaml(d: Dataset, sql_override: str | None = None) -> dict:
    temporal = {"DATE", "TIMESTAMP", "TIMESTAMP(6)"}
    cols = [{"column_name": n, "verbose_name": LABELS.get(n), "is_dttm": t in temporal, "is_active": True, "type": t,
             "groupby": True, "filterable": True, "expression": None, "description": None,
             "python_date_format": None, "advanced_data_type": None, "extra": None}
            for n, t in d.columns.items()]
    return {"table_name": d.name, "main_dttm_col": d.main_dttm_col, "description": d.description,
            "default_endpoint": None, "offset": 0, "cache_timeout": None, "catalog": None, "schema": "gold",
            "sql": sql_override if sql_override is not None else d.sql, "params": None, "template_params": None,
            "filter_select_enabled": True, "fetch_values_predicate": None, "extra": None,
            "normalize_columns": False, "always_filter_main_dttm": False,
            "uuid": d.uuid, "metrics": [], "columns": cols, "version": VERSION,
            "database_uuid": uid("database", DATABASE_NAME)}


def chart_yaml(c: Chart) -> dict:
    return {"slice_name": c.name, "description": c.description or None, "certified_by": None,
            "certification_details": None, "viz_type": c.viz_type,
            "params": {**c.params, "datasource": f"{DS[c.dataset].uuid}__table"}, "query_context": None,
            "cache_timeout": None, "uuid": c.uuid, "version": VERSION, "dataset_uuid": DS[c.dataset].uuid}


def dashboard_yaml(d: Dashboard) -> dict:
    return {"dashboard_title": d.title, "description": d.description, "css": "", "slug": d.slug,
            "certified_by": None, "certification_details": None, "published": True,
            "uuid": d.uuid, "version": VERSION, "position": position(d),
            "metadata": {"color_scheme": "supersetColors", "refresh_frequency": 0, "expanded_slices": {},
                         "label_colors": {}, "shared_label_colors": [], "map_label_colors": {},
                         "color_scheme_domain": [], "timed_refresh_immune_slices": [],
                         "cross_filters_enabled": True, "default_filters": "{}", "chart_configuration": {},
                         "global_chart_configuration": {}, "native_filter_configuration": [country_filter(d)]}}


def build_bundle(uri: str = DEFAULT_URI, sql_override: dict[str, str] | None = None) -> bytes:
    """Zip d'import Superset v1. `sql_override` (tests) : SQL de remplacement par jeu de données."""
    root = "waba_dashboards"
    files = {f"{root}/metadata.yaml": _dump({"version": VERSION, "type": "Dashboard",
                                             "timestamp": datetime.now(timezone.utc).isoformat()}),
             f"{root}/databases/waba_lakehouse.yaml": _dump(database_yaml(uri))}
    for d in DATASETS:
        files[f"{root}/datasets/waba_lakehouse/{d.name}.yaml"] = _dump(
            dataset_yaml(d, (sql_override or {}).get(d.name)))
    for c in CHARTS:
        files[f"{root}/charts/{slug(c.name)}_{c.uuid[:8]}.yaml"] = _dump(chart_yaml(c))
    for d in DASHBOARDS:
        files[f"{root}/dashboards/{d.slug}.yaml"] = _dump(dashboard_yaml(d))
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for path, content in files.items():
            z.writestr(path, content)
    return buf.getvalue()


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("output", nargs="?")
    p.add_argument("--sqlalchemy-uri", default=DEFAULT_URI)
    p.add_argument("--check-sql", action="store_true",
                   help="affiche une requête de contrôle par jeu de données (nombre de lignes), pour Trino")
    a = p.parse_args(argv)
    if a.check_sql:
        for d in DATASETS:
            print(f"SELECT '{d.name}' AS dataset, count(*) AS row_count FROM (\n{d.sql}\n) t;")
        return 0
    if not a.output:
        p.error("chemin du bundle requis")
    with open(a.output, "wb") as f:
        f.write(build_bundle(a.sqlalchemy_uri))
    print(f"bundle {a.output} : {len(DATASETS)} jeux de données, {len(CHARTS)} graphiques, "
          f"{len(DASHBOARDS)} tableaux de bord")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
