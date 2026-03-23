"""Analisis con Pandas del dataset de vehiculos electricos.

Modos de lectura:
- local   : calcula golden desde los CSV en ./datasets (sin conexion a Azure)
- container: lee las tablas Delta ya procesadas en Azure ADLS golden layer
"""

import argparse
import os
from pathlib import Path

import pandas as pd

try:
    import matplotlib.pyplot as plt
except ModuleNotFoundError:
    plt = None

try:
    import adlfs
    import pyarrow.dataset as _pds
    import pyarrow.parquet as _ppq
    _ADLFS_OK = True
except ModuleNotFoundError:
    _ADLFS_OK = False

import json as _json
import re as _re


def normalize_text(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.strip()
        .str.upper()
        .str.replace(r"\s+", " ", regex=True)
    )


def to_numeric(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def print_table(title: str, df: pd.DataFrame, rows: int = 20) -> None:
    print(f"\n{title}")
    if df.empty:
        print("(sin datos)")
    else:
        print(df.head(rows).to_string(index=False))


BASE_DIR = Path(__file__).resolve().parent
DATASETS_DIR = BASE_DIR / "datasets"
OUTPUTS_DIR = BASE_DIR / "outputs"
OUTPUTS_DIR.mkdir(exist_ok=True)

LOCAL_SPECS_PATH = DATASETS_DIR / "electric_vehicles_spec_2025.csv.csv"
LOCAL_POP_PATH = DATASETS_DIR / "Electric_Vehicle_Population_Data.csv"


def load_dotenv_file(file_path: Path) -> None:
    if not file_path.exists():
        return
    for raw_line in file_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


# ---------------------------------------------------------------------------
# Modo CONTAINER: lee tablas Delta de la capa golden desde Azure Blob + SAS
# ---------------------------------------------------------------------------

GOLDEN_TABLES = [
    "golden_ev_model_popularity",
    "golden_make_price_range",
    "golden_make_range_price_ratio",
    "golden_range_vs_registrations",
    "golden_state_distribution",
]


def _delta_active_files(fs, path: str) -> list[str] | None:
    """Parsea el _delta_log para devolver solo los archivos Parquet activos.

    Databricks usa External Location + Delta: al hacer overwrite, los archivos
    viejos quedan físicamente en el blob pero son marcados como 'remove' en el log.
    pyarrow.dataset los lee todos, produciendo duplicados. Este parser evita eso.

    Devuelve None si no se puede leer el log (fallback a leer todo).
    """
    log_path = f"{path}/_delta_log"
    try:
        all_entries = fs.ls(log_path, detail=False)
    except Exception:
        return None

    # Separar checkpoints y JSON de transacciones
    checkpoints = sorted(
        [e for e in all_entries if ".checkpoint." in e or e.endswith(".checkpoint.parquet")],
        reverse=True,
    )
    json_files = sorted([e for e in all_entries if e.endswith(".json")])

    active: set[str] = set()
    start_version = 0

    # Leer el checkpoint más reciente si existe (contiene el snapshot completo)
    if checkpoints:
        latest_ckpt = checkpoints[0]
        m = _re.search(r"/(\d+)\.checkpoint", latest_ckpt)
        if m:
            start_version = int(m.group(1)) + 1
        try:
            with fs.open(latest_ckpt, "rb") as f:
                tbl = _ppq.read_table(f, columns=["add"])
            for row in tbl.to_pydict().get("add", []):
                if row and row.get("path"):
                    active.add(row["path"])
        except Exception:
            active.clear()
            start_version = 0  # si falla, replaya todo desde JSON

    # Aplicar los JSON de transacciones posteriores al checkpoint
    for jf in json_files:
        m = _re.search(r"/(\d+)\.json$", jf)
        if not m:
            continue
        if int(m.group(1)) < start_version:
            continue
        try:
            with fs.open(jf) as f:
                content = f.read().decode("utf-8")
            for line in content.strip().splitlines():
                if not line:
                    continue
                entry = _json.loads(line)
                if entry.get("add") and entry["add"].get("path"):
                    active.add(entry["add"]["path"])
                if entry.get("remove") and entry["remove"].get("path"):
                    active.discard(entry["remove"]["path"])
        except Exception:
            continue

    return list(active) if active else None


def read_delta_table(table_name: str, storage_options: dict) -> pd.DataFrame:
    account = storage_options["account_name"]
    container = storage_options["container"]
    root_path = storage_options["root_path"]
    sas_token = storage_options["sas_token"]
    path = f"{container}/{root_path}/{table_name}"
    print(f"  Leyendo abfs://{path} ...", end=" ", flush=True)
    try:
        fs = adlfs.AzureBlobFileSystem(
            account_name=account,
            sas_token=sas_token,
        )
        # Parsear el delta_log para obtener solo los archivos activos
        active_files = _delta_active_files(fs, path)
        if active_files:
            full_paths = [f"{path}/{f}" for f in active_files]
            dataset = _pds.dataset(full_paths, filesystem=fs, format="parquet")
        else:
            # Fallback: leer todos los parquet del directorio
            dataset = _pds.dataset(path, filesystem=fs, format="parquet", exclude_invalid_files=True)
        df = dataset.to_table().to_pandas()
        print(f"{len(df):,} filas")
        return df
    except Exception as exc:
        raise RuntimeError(
            f"No se pudo leer la tabla '{table_name}'. "
            "Verifica que el SAS sea valido, no haya expirado y tenga permisos rl."
        ) from exc


def read_golden_from_container() -> dict[str, pd.DataFrame]:
    if not _ADLFS_OK:
        raise ModuleNotFoundError(
            "Instala adlfs y pyarrow para modo container: pip install adlfs pyarrow"
        )

    sas_token = os.getenv("AZURE_SAS_TOKEN", "")
    if not sas_token or "<" in sas_token:
        raise ValueError(
            "AZURE_SAS_TOKEN no esta configurado. Edita el .env con tu SAS real."
        )

    storage_options = {
        "account_name": os.getenv("AZURE_STORAGE_ACCOUNT", "adlssmartdata2023"),
        "container": os.getenv("AZURE_CONTAINER", "golden"),
        "root_path": os.getenv("AZURE_GOLDEN_PATH", "catalog_smartdata_final"),
        "sas_token": sas_token,
    }

    tables = {}
    for name in GOLDEN_TABLES:
        tables[name] = read_delta_table(name, storage_options)

    # Ordenar por la metrica principal de cada tabla
    _sort = {
        "golden_ev_model_popularity":    ("vehicles_registered", False),
        "golden_make_price_range":        ("avg_range",           False),
        "golden_state_distribution":      ("total_ev",            False),
        "golden_range_vs_registrations":  ("vehicles_registered", False),
        "golden_make_range_price_ratio":  ("range_per_price_ratio", False),
    }
    for tname, (col, asc) in _sort.items():
        df = tables[tname]
        if col in df.columns:
            tables[tname] = df.sort_values(col, ascending=asc).reset_index(drop=True)

    return tables


# ---------------------------------------------------------------------------
# Modo LOCAL: calcula golden desde los CSV (sin conexion a Azure)
# ---------------------------------------------------------------------------

def build_golden_from_local() -> dict[str, pd.DataFrame]:
    if not LOCAL_SPECS_PATH.exists() or not LOCAL_POP_PATH.exists():
        missing = [str(p) for p in [LOCAL_SPECS_PATH, LOCAL_POP_PATH] if not p.exists()]
        raise FileNotFoundError(f"No se encontraron archivos requeridos: {missing}")

    specs = pd.read_csv(LOCAL_SPECS_PATH)
    population = pd.read_csv(LOCAL_POP_PATH)

    specs = specs.rename(columns={"brand": "make"})
    specs = to_numeric(specs, [
        "top_speed_kmh", "battery_capacity_kWh", "number_of_cells", "torque_nm",
        "efficiency_wh_per_km", "range_km", "acceleration_0_100_s",
        "fast_charging_power_kw_dc", "towing_capacity_kg", "cargo_volume_l",
        "seats", "length_mm", "width_mm", "height_mm",
    ])

    population = population.rename(columns={
        "VIN (1-10)": "vin_1_10", "County": "county", "City": "city",
        "State": "state", "Postal Code": "postal_code", "Model Year": "model_year",
        "Make": "make", "Model": "model", "Electric Vehicle Type": "ev_type",
        "Clean Alternative Fuel Vehicle (CAFV) Eligibility": "cafv_eligibility",
        "Electric Range": "electric_range", "Base MSRP": "base_msrp",
        "Legislative District": "legislative_district", "DOL Vehicle ID": "dol_vehicle_id",
        "Vehicle Location": "vehicle_location", "Electric Utility": "electric_utility",
        "2020 Census Tract": "census_tract_2020",
    })
    population = to_numeric(population, ["electric_range", "base_msrp", "model_year"])

    specs["join_make"] = normalize_text(specs["make"])
    specs["join_model"] = normalize_text(specs["model"])
    population["join_make"] = normalize_text(population["make"])
    population["join_model"] = normalize_text(population["model"])
    population["state"] = normalize_text(population["state"])
    population["city"] = normalize_text(population["city"])
    population["county"] = normalize_text(population["county"])

    enriched = population.merge(specs, how="left", on=["join_make", "join_model"],
                                suffixes=("_pop", "_spec"))
    enriched["has_specs_match"] = enriched["range_km"].notna()
    enriched_with_specs = enriched[enriched["has_specs_match"]].copy()

    popularity = (
        population.groupby(["make", "model"], dropna=False)
        .size().reset_index(name="vehicles_registered")
        .sort_values("vehicles_registered", ascending=False)
    )

    price_range = (
        enriched_with_specs.groupby("make_pop", dropna=False)
        .agg(avg_price=("base_msrp", "mean"), avg_range=("range_km", "mean"),
             vehicles_registered=("make_pop", "size"))
        .reset_index().rename(columns={"make_pop": "make"})
        .sort_values("avg_range", ascending=False)
    )

    state_dist = (
        population.groupby("state", dropna=False)
        .size().reset_index(name="total_ev")
        .sort_values("total_ev", ascending=False)
    )

    range_regs = (
        enriched_with_specs.groupby(["make_pop", "model_pop", "range_km"], dropna=False)
        .size().reset_index(name="vehicles_registered")
        .rename(columns={"make_pop": "make", "model_pop": "model"})
        .sort_values("vehicles_registered", ascending=False)
    )

    ratio = (
        price_range.assign(
            range_per_price_ratio=lambda df: df["avg_range"] / df["avg_price"].replace(0, pd.NA)
        ).dropna(subset=["range_per_price_ratio"])
        .sort_values("range_per_price_ratio", ascending=False)
    )

    print(f"Specs filas: {len(specs):,}")
    print(f"Population filas: {len(population):,}")
    total = len(enriched)
    match = int(enriched["has_specs_match"].sum())
    print(f"\nMetricas de join: total={total:,}  con_match={match:,}  "
          f"porcentaje={round(match/total*100,2):.2f}%")

    return {
        "golden_ev_model_popularity": popularity,
        "golden_make_price_range": price_range,
        "golden_state_distribution": state_dist,
        "golden_range_vs_registrations": range_regs,
        "golden_make_range_price_ratio": ratio,
    }


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

load_dotenv_file(BASE_DIR / ".env")

default_mode = os.getenv("EV_MODE", "local").strip().lower()
if default_mode not in {"local", "container"}:
    default_mode = "local"

parser = argparse.ArgumentParser(description="Analisis ETL de vehiculos con Pandas")
parser.add_argument("--mode", choices=["local", "container"], default=default_mode)
args = parser.parse_args()

print(f"Modo de lectura: {args.mode}")

if args.mode == "container":
    tables = read_golden_from_container()
else:
    tables = build_golden_from_local()

golden_ev_model_popularity    = tables["golden_ev_model_popularity"]
golden_make_price_range       = tables["golden_make_price_range"]
golden_state_distribution     = tables["golden_state_distribution"]
golden_range_vs_registrations = tables["golden_range_vs_registrations"]
golden_make_range_price_ratio = tables["golden_make_range_price_ratio"]

# ---------------------------------------------------------------------------
# Analisis e insights
# ---------------------------------------------------------------------------

print_table("Top modelos por vehiculos registrados:", golden_ev_model_popularity, rows=20)
print_table("Top marcas por autonomia promedio:", golden_make_price_range, rows=20)
print_table("Distribucion por estado:", golden_state_distribution, rows=10)
print_table("Mejor relacion rango/precio por marca:", golden_make_range_price_ratio, rows=10)

top_model = golden_ev_model_popularity.iloc[0] if not golden_ev_model_popularity.empty else None
top_state = golden_state_distribution.iloc[0] if not golden_state_distribution.empty else None
best_ratio = golden_make_range_price_ratio.iloc[0] if not golden_make_range_price_ratio.empty else None

print("\n" + "="*60)
print("INSIGHTS CLAVE DEL ETL")
print("="*60)
if top_model is not None:
    print(
        f"1) Modelo lider en registros : {top_model['make']} {top_model['model']} "
        f"({int(top_model['vehicles_registered']):,})"
    )
if top_state is not None:
    print(f"2) Estado con mayor adopcion  : {top_state['state']} ({int(top_state['total_ev']):,})")
if best_ratio is not None:
    print(
        f"3) Mejor relacion rango/precio: {best_ratio['make']} "
        f"(ratio={best_ratio['range_per_price_ratio']:.6f})"
    )
if not golden_make_price_range.empty:
    top_range = golden_make_price_range.iloc[0]
    print(f"4) Mayor autonomia promedio   : {top_range['make']} ({top_range['avg_range']:.0f} km)")
print("5) Ver golden_range_vs_registrations para correlacion autonomia/demanda.")
print("="*60)


def save_plots() -> None:
    if plt is None:
        print("\nmatplotlib no instalado. Omitiendo graficas.")
        return

    # Top 10 modelos
    top_models = golden_ev_model_popularity.head(10).copy()
    if not top_models.empty:
        top_models["label"] = top_models["make"].astype(str) + " " + top_models["model"].astype(str)
        plt.figure(figsize=(12, 6))
        plt.barh(top_models["label"][::-1], top_models["vehicles_registered"][::-1])
        plt.title("Top 10 modelos por vehiculos registrados")
        plt.xlabel("Vehiculos registrados")
        plt.tight_layout()
        plt.savefig(OUTPUTS_DIR / "top_modelos.png", dpi=120)
        plt.close()

    # Top 10 estados
    top_states = golden_state_distribution.head(10)
    if not top_states.empty:
        plt.figure(figsize=(10, 6))
        plt.bar(top_states["state"], top_states["total_ev"])
        plt.title("Top 10 estados por adopcion EV")
        plt.xlabel("Estado")
        plt.ylabel("Total EV")
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        plt.savefig(OUTPUTS_DIR / "top_estados.png", dpi=120)
        plt.close()

    # Precio vs rango por marca
    scatter_df = golden_make_price_range.dropna(subset=["avg_price", "avg_range"]).copy()
    if not scatter_df.empty:
        plt.figure(figsize=(10, 6))
        plt.scatter(scatter_df["avg_price"], scatter_df["avg_range"], alpha=0.75)
        plt.title("Rango promedio vs precio promedio por marca")
        plt.xlabel("Precio promedio (Base MSRP)")
        plt.ylabel("Rango promedio (km)")
        plt.tight_layout()
        plt.savefig(OUTPUTS_DIR / "precio_vs_rango.png", dpi=120)
        plt.close()

    # Rango vs registros (scatter)
    range_col = next((c for c in ["specs_range_km", "range_km", "avg_range"] if c in golden_range_vs_registrations.columns), None)
    rv = golden_range_vs_registrations.dropna(subset=["vehicles_registered"]).copy()
    if not rv.empty and range_col:
        rv = rv.dropna(subset=[range_col])
        plt.figure(figsize=(10, 6))
        plt.scatter(rv[range_col], rv["vehicles_registered"], alpha=0.5)
        plt.title("Autonomia (km) vs vehiculos registrados por modelo")
        plt.xlabel("Rango (km)")
        plt.ylabel("Vehiculos registrados")
        plt.tight_layout()
        plt.savefig(OUTPUTS_DIR / "rango_vs_registros.png", dpi=120)
        plt.close()

    print(f"\nGraficas guardadas en: {OUTPUTS_DIR}")


save_plots()
