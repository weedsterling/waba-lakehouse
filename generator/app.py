"""WABA Group — Application Streamlit de génération de données (Level 1.1).

Lancement : streamlit run app.py   (dans le conteneur `generator`)
"""
from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta
from datetime import time as dtime

import pandas as pd
import streamlit as st

from waba_gen import config as C
from waba_gen import setup_logging
from waba_gen.continuous import ContinuousGenerator
from waba_gen.referentials import generate_referentials
from waba_gen.storage import LakeStorage, ReferentialCache
from waba_gen.transactions import Pools, generate_transactions

setup_logging()
log = logging.getLogger("waba.app")

st.set_page_config(page_title="WABA Group · Data Generator", page_icon="🏦", layout="wide")

DATASET_LABELS = {
    "bank_transactions": "Transactions bancaires",
    "insurance_operations": "Opérations d'assurance",
    "mobile_money_payments": "Paiements mobile money",
    "loan_repayments": "Remboursements de crédit",
}
# Ligne métier -> jeux de données concernés
ENTITY_DATASETS = {
    "BANK": ["bank_transactions", "loan_repayments"],
    "MICROFINANCE": ["bank_transactions", "loan_repayments"],
    "INSURANCE": ["insurance_operations"],
    "MOBILE_MONEY": ["mobile_money_payments"],
}


def last_quarter(today: date) -> tuple[date, date]:
    q_start_month = 3 * ((today.month - 1) // 3) + 1
    current_q_start = date(today.year, q_start_month, 1)
    end = current_q_start - timedelta(days=1)
    return date(end.year, 3 * ((end.month - 1) // 3) + 1, 1), end


# --------------------------------------------------------------------------- #
# Ressources partagées entre reruns
# --------------------------------------------------------------------------- #
@st.cache_resource
def get_storage() -> LakeStorage:
    return LakeStorage()


@st.cache_resource
def get_cache() -> ReferentialCache:
    return ReferentialCache()


@st.cache_resource(show_spinner="Chargement des référentiels…")
def get_referentials(version: float):
    """`version` = mtime du cache : invalide le cache Streamlit après régénération."""
    ref = get_cache().load()
    return ref, Pools(ref)


@st.cache_resource
def get_continuous_holder() -> dict:
    return {"gen": None}


storage, cache = get_storage(), get_cache()

# --------------------------------------------------------------------------- #
# Sidebar : état de la plateforme
# --------------------------------------------------------------------------- #
with st.sidebar:
    st.title("🏦 WABA Group")
    st.caption("Plateforme analytique financière multi-pays")
    minio_ok = storage.healthcheck()
    st.metric("MinIO (raw-landing)", "✅ connecté" if minio_ok else "❌ indisponible")
    st.metric("Référentiels", "✅ prêts" if cache.exists() else "⚠️ à générer")
    st.divider()
    st.caption("Destination : `s3://raw-landing/<type>/<pays>/`")

st.title("Générateur de données — WABA Group")
tab_ref, tab_once, tab_stream = st.tabs(["1️⃣ Référentiels", "2️⃣ Génération one-time", "3️⃣ Flux continu"])

# --------------------------------------------------------------------------- #
# Onglet 1 : référentiels
# --------------------------------------------------------------------------- #
with tab_ref:
    st.subheader("Référentiels (générés une fois, partagés entre pays)")
    st.info("Les référentiels doivent être générés **avant** les transactions : "
            "customers → accounts → transactions garantit l'absence de clés orphelines.")
    c1, c2, c3, c4, c5 = st.columns(5)
    n_cust = c1.number_input("customers", 1_000, 2_000_000, C.DEFAULT_ROWS["customers"], step=10_000)
    n_acc = c2.number_input("accounts", 1_000, 3_000_000, C.DEFAULT_ROWS["accounts"], step=10_000)
    n_br = c3.number_input("branches", 24, 2_000, C.DEFAULT_ROWS["branches"], step=10)
    n_prod = c4.number_input("products", 10, 200, C.DEFAULT_ROWS["products"], step=5)
    seed = c5.number_input("seed", 0, 10_000, 42)

    if st.button("Générer et envoyer vers MinIO", type="primary", disabled=not minio_ok, key="btn_ref"):
        if n_acc < n_cust:
            st.error("accounts doit être ≥ customers (au moins un compte par client).")
        else:
            t0 = time.perf_counter()
            with st.status("Génération des référentiels…", expanded=True) as status:
                ref = generate_referentials({"customers": n_cust, "accounts": n_acc,
                                             "branches": n_br, "products": n_prod}, seed=int(seed))
                st.write("✔️ Données générées, mise en cache locale…")
                cache.save(ref)
                st.write("✔️ Upload vers MinIO…")
                keys = storage.upload_referentials(ref)
                status.update(label=f"Référentiels envoyés en {time.perf_counter() - t0:.1f}s", state="complete")
            st.code("\n".join(keys))
            get_referentials.clear()

    if cache.exists():
        ref, _ = get_referentials(cache.dir.stat().st_mtime)
        st.write("Aperçu :")
        for name, df in ref.as_dict().items():
            with st.expander(f"{name} — {len(df):,} lignes".replace(",", " ")):
                st.dataframe(df.head(20), width="stretch")


# --------------------------------------------------------------------------- #
# Paramètres communs transactions
# --------------------------------------------------------------------------- #
def selection_widgets(key: str):
    c1, c2 = st.columns(2)
    datasets = c1.multiselect("Type de données", list(DATASET_LABELS), default=list(DATASET_LABELS),
                              format_func=DATASET_LABELS.get, key=f"{key}_ds")
    countries = c2.multiselect("Pays", C.COUNTRIES, default=C.COUNTRIES, key=f"{key}_cc")
    entities = st.multiselect("Ligne métier", C.ENTITY_TYPES, default=C.ENTITY_TYPES, key=f"{key}_ent")
    allowed = {d for e in entities for d in ENTITY_DATASETS[e]}
    skipped = [DATASET_LABELS[d] for d in datasets if d not in allowed]
    if skipped:
        st.caption(f"Ignoré(s) car hors lignes métier sélectionnées : {', '.join(skipped)}")
    return [d for d in datasets if d in allowed], countries, entities


def filter_entities(frames: dict[str, pd.DataFrame], entities: list[str]) -> dict[str, pd.DataFrame]:
    out = {}
    for cc, df in frames.items():
        df = df[df["entity_type"].isin(entities)]
        if not df.empty:
            out[cc] = df
    return out


# --------------------------------------------------------------------------- #
# Onglet 2 : one-time
# --------------------------------------------------------------------------- #
with tab_once:
    st.subheader("Génération immédiate")
    if not cache.exists():
        st.warning("Générez d'abord les référentiels (onglet 1).")
    else:
        datasets, countries, entities = selection_widgets("once")
        q_start, q_end = last_quarter(date.today())
        c1, c2 = st.columns(2)
        d_start = c1.date_input("Date de début", q_start)
        d_end = c2.date_input("Date de fin", q_end)
        st.markdown("**Nombre de lignes par type**")
        cols = st.columns(4)
        n_rows = {ds: cols[i].number_input(DATASET_LABELS[ds], 0, 5_000_000, C.DEFAULT_ROWS[ds], step=1_000,
                                           key=f"rows_{ds}")
                  for i, ds in enumerate(DATASET_LABELS)}
        anomaly = st.slider("Taux d'anomalies injectées (test de la validation Spark)", 0.0, 0.05, 0.005,
                            step=0.005, format="%.3f", key="once_anom")

        if st.button("Générer et envoyer vers MinIO", type="primary", key="btn_once",
                     disabled=not (minio_ok and datasets and countries)):
            ref, pools = get_referentials(cache.dir.stat().st_mtime)
            start = datetime.combine(d_start, dtime.min)
            end = datetime.combine(d_end, dtime.max)
            summary = []
            with st.status("Génération en cours…", expanded=True) as status:
                for ds in datasets:
                    frames = generate_transactions(ref, ds, countries, int(n_rows[ds]), start, end,
                                                   anomaly_rate=anomaly, pools=pools)
                    frames = filter_entities(frames, entities)
                    keys = storage.upload_transactions(ds, frames)
                    for (cc, df), key in zip(frames.items(), keys, strict=True):
                        summary.append({"type": ds, "pays": cc, "lignes": len(df), "fichier": key})
                    st.write(f"✔️ {DATASET_LABELS[ds]} : {len(keys)} fichier(s)")
                status.update(label="Génération terminée", state="complete")
            st.dataframe(pd.DataFrame(summary), width="stretch")

# --------------------------------------------------------------------------- #
# Onglet 3 : flux continu
# --------------------------------------------------------------------------- #
with tab_stream:
    st.subheader("Génération continue (simulation temps réel)")
    holder = get_continuous_holder()
    if not cache.exists():
        st.warning("Générez d'abord les référentiels (onglet 1).")
    else:
        datasets, countries, entities = selection_widgets("stream")
        c1, c2 = st.columns(2)
        rows = c1.number_input("Lignes par micro-lot (par type)", 10, 10_000, 200, step=10)
        interval = c2.slider("Intervalle (secondes)", 10, 60, (10, 30))
        anomaly = st.slider("Taux d'anomalies", 0.0, 0.05, 0.0, step=0.005, format="%.3f", key="stream_anom")

        gen: ContinuousGenerator | None = holder["gen"]
        running = gen is not None and gen.running
        b1, b2 = st.columns(2)
        if b1.button("▶️ Démarrer", key="btn_start", disabled=running or not (minio_ok and datasets and countries)):
            ref, _ = get_referentials(cache.dir.stat().st_mtime)
            gen = ContinuousGenerator(storage, ref)
            gen.start(datasets, countries, int(rows), interval[0], interval[1], anomaly)
            holder["gen"] = gen
            st.rerun()
        if b2.button("⏹️ Arrêter", key="btn_stop", disabled=not running):
            gen.stop()
            st.rerun()

        if gen is not None:
            m1, m2, m3 = st.columns(3)
            m1.metric("Statut", "🟢 en cours" if gen.running else "⚪ arrêté")
            m2.metric("Micro-lots", gen.batches)
            m3.metric("Lignes émises", f"{gen.rows:,}".replace(",", " "))
            st.text("\n".join(list(gen.history)[:30]) or "—")
            if gen.running and st.toggle("Rafraîchissement auto (5 s)", value=True):
                time.sleep(5)
                st.rerun()
