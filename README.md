# Subscriber Usage Report Generator
Generate Usage Report for this day. Query data from MongoDB databases and write the report on a MySQL database.
# Required Packages
- Python 3.11
- MongoDB 6.0 Instance
- MySQL 8.0
    - Before installing `mysqlclient`, follow instructions [here](https://github.com/PyMySQL/mysqlclient) at the `Install` section.
# How to Install?
- Create a `.env` file from the `sample.env` file.
```bash
cp sample.env .env
nano .env
```
- Create and activate the Virtual Environment.

**Linux/MacOS**
```bash
python3.11 -m venv .venv
source .venv/bin/activate
```
**Windows**
```bash
python3.11 -m venv .venv
.venv/bin/Activate.ps1
```
- Install the packages.
```bash
(.venv) pip install -r requirements.txt
```
# How to Run?
```
python run.py
```
# How to Test/Develop?
- Assuming you're still in the Virtual Environment, install the Development packages.
```bash
(.venv) pip install -r requirements-dev.txt
```
- Install the `pre-commit` hooks.
```bash
(.venv) pre-commit install
```
- Run all linter and unit tests.
```bash
(.venv) pre-commit
```