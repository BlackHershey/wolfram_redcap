# a collection of utility scripts for use with Wolfram Syndrome data

This repository contains small, focused Python scripts and utilities for
importing, formatting, and preparing Wolfram Syndrome research data
for analysis (RedCap exports, SPSS, Wolfram data formats, and related
preprocessing tasks).

Contents (selected):

- `dot_dbs_import.py` — import dotDbs data files
- `extract_form_fields.py` — extract repeated form fields
- `format_aseba_scores.py` — format ASEBA score exports
- `format_track_data.py` — flatten longitudinal TRACK data
- `format_wolfram_data.py` — flatten longitudinal Wolfram data
- `redcap_common.py` — shared helpers for RedCap processing
- `redcap2spss.py` — convert RedCap exports to SPSS-friendly format
- `wfs_db_migration.py` — REDCap DB migration helpers for this project

Requirements
------------

- Python 3.8 is required to run these scripts.
- Create and activate the virtual environment in the project root:

- `python -m venv env`
- `env\Scripts\activate` (PowerShell)
- Install dependencies:

- `pip install -r requirements.txt`

Usage
-----
Run the individual scripts directly with the project virtualenv active:

1. Activate the environment: `env\Scripts\activate` (Windows PowerShell)
2. Run a script: `python format_wolfram_data.py --help` to view options
