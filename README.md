# CICDSmartDataDatabricks

Pipeline ETL de vehículos eléctricos implementado en **Databricks** usando arquitectura **Medallion** (raw → bronze → silver → golden), con despliegue automatizado desde **GitHub Actions**.

---

## Descripción del proyecto

Este proyecto es una práctica de **CI/CD con Databricks** en el contexto del curso "Ingeniería de Datos e IA con Databricks". Implementa un pipeline de datos completo para analizar la adopción de vehículos eléctricos (EV) en Estados Unidos, cruzando datos de registro vehicular del estado de Washington con especificaciones técnicas globales de modelos 2025.

El repositorio incluye:

- Notebooks de Databricks para cada etapa del pipeline ETL.
- Configuración de CI/CD con GitHub Actions para despliegue automático en Databricks.

---

## Arquitectura

```
GitHub Actions
      │
      ▼
  Databricks
      │
      ▼
Azure Data Lake Storage Gen2 (adlssmartdata2023)
  ├── raw/       ← datasets fuente (CSV)
  ├── bronze/    ← datos ingestados sin transformar (Delta)
  ├── silver/    ← datos limpios y enriquecidos (Delta)
  └── golden/    ← tablas de métricas de negocio (Delta)
```

**Unity Catalog**: `catalog_smartdata_final`

---

## Datasets fuente

| Archivo                                | Fuente                                           | Descripción                                                                  |
| -------------------------------------- | ------------------------------------------------ | ---------------------------------------------------------------------------- |
| `Electric_Vehicle_Population_Data.csv` | WA DOL (Departamento de Licencias de Washington) | 210,165 vehículos eléctricos registrados, 43 marcas, ~99.8% del estado de WA |
| `electric_vehicles_spec_2025.csv`      | ev-database.org                                  | 478 variantes de modelos 2025, 59 marcas, mercado global/europeo             |

---

## Pipeline ETL — Notebooks en `proceso/`

| Notebook                          | Capa            | Descripción                                                                                        |
| --------------------------------- | --------------- | -------------------------------------------------------------------------------------------------- |
| `Preparacion_Ambiente.ipynb`      | Infraestructura | Crea External Locations, Catalog, Schemas y tablas Delta vacías en Unity Catalog                   |
| `ingest_vehicle_population.ipynb` | Raw → Bronze    | Ingesta del dataset de registro vehicular de WA                                                    |
| `ingest_vehicle_specs.ipynb`      | Raw → Bronze    | Ingesta del dataset de especificaciones técnicas 2025                                              |
| `transform_vehicles.ipynb`        | Bronze → Silver | Join enriquecido (exacto + por prefijo), conversión de unidades (millas → km), flag de MSRP válido |
| `load_vehicle.ipynb`              | Silver → Golden | Genera las 5 tablas de métricas de negocio                                                         |
| `diagnostico_calidad_datos.ipynb` | Diagnóstico     | Análisis de calidad, detección de inconsistencias y cobertura del join                             |

### Tablas Golden generadas

| Tabla                           | Filas | Contenido                                              |
| ------------------------------- | ----- | ------------------------------------------------------ |
| `golden_ev_model_popularity`    | 153   | Popularidad por marca y modelo (vehículos registrados) |
| `golden_make_price_range`       | 43    | Precio promedio y autonomía promedio por marca         |
| `golden_make_range_price_ratio` | 11    | Relación autonomía/precio por marca                    |
| `golden_range_vs_registrations` | 153   | Autonomía (km) vs. registros por modelo                |
| `golden_state_distribution`     | 47    | Distribución de EVs por estado                         |

---

## Hallazgos de calidad de datos

Durante el proyecto se identificaron 6 inconsistencias relevantes entre los datasets:

1. **Join exacto make+model con 0.34% de match** — los nombres de modelos difieren en nivel de detalle (ej. `"MODEL 3"` vs `"MODEL 3 LONG RANGE AWD (HIGHLAND)"`). Corregido con join por prefijo en `transform_vehicles.ipynb`.
2. **16 marcas sin cobertura en specs** (CHEVROLET, RIVIAN, NISSAN, TESLA variantes legacy, etc.) — specs es un catálogo 2025 europeo/global, no incluye modelos históricos del mercado US.
3. **`base_msrp = 0` en 98.4% de registros** — valor por defecto del DOL de WA, no es precio real.
4. **Unidades distintas**: `electric_range` en millas EPA (population) vs. `range_km` en km (specs). Transformación aplicada: `electric_range_km = electric_range × 1.60934`.
5. **Sesgo geográfico**: 99.8% de registros pertenecen al estado de Washington.
6. **Specs es catálogo 2025 europeo/global**: no incluye modelos legacy US como LEAF, BOLT EV, VOLT o PRIUS PRIME.

---

## Insights principales (ejecución 2026-03-16)

- **Modelo más popular**: TESLA MODEL Y — 44,038 vehículos registrados.
- **Estado con mayor adopción**: WA — 209,720 EVs.
- **Mayor autonomía promedio**: JAGUAR (234 mi / 376 km) y TESLA (241 mi / 388 km).

---

## Estructura del repositorio

```
CICDSmartDataDatabricks/
├── proceso/
│   ├── Preparacion_Ambiente.ipynb
│   ├── ingest_vehicle_population.ipynb
│   ├── ingest_vehicle_specs.ipynb
│   ├── transform_vehicles.ipynb
│   ├── load_vehicle.ipynb
│   └── diagnostico_calidad_datos.ipynb
├── datasets/
│   ├── Electric_Vehicle_Population_Data.csv
│   └── electric_vehicles_spec_2025.csv.csv
└── README.md
```

---

## Tecnologías utilizadas

- **Databricks** (Apache Spark, Delta Lake, Unity Catalog)
- **Azure Data Lake Storage Gen2**
- **GitHub Actions** (CI/CD)
- **Delta Lake** con arquitectura Medallion
