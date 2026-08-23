# Demo 

Real-time YouTube comment sentiment analysis using:
* Python
* Apache Kafka
* Apache Spark Structured Streaming
* Spark MLlib / NLP
* MongoDB
* Real-Time Analytics Dashboard

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/giunzz/bigdata_project.git
cd bigdata_project
```

### 2. Install `uv`

If you don't have `uv` installed:

#### Linux / macOS

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Then restart your terminal or run:

```bash
source ~/.bashrc
```

Check:

```bash
uv --version
```

#### Windows

Open PowerShell:

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

Then check:

```powershell
uv --version
```

---

### 3. Setup the Python environment

Inside the project directory, simply run:

```bash
uv sync
```



```bash
git pull
uv sync
```

---

## Environment Variables

Create a local `.env` file:

```env
YOUTUBE_API_KEY=your_api_key
MONGODB_URI=mongodb://localhost:27017
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

**Do not commit `.env` or API keys to GitHub.**

---

## Git Workflow

### Before starting work

```bash
git pull
uv sync
```

### After making changes

```bash
git add .
git commit -m "Describe your changes"
git push
```

### If you add a Python package

```bash
uv add <package>

git add pyproject.toml uv.lock
git commit -m "Add <package> dependency"
git push
```

---

## Important Files

```text
bigdata_project/
│
├── pyproject.toml    # Project dependencies
├── uv.lock           # Locked dependency versions
├── .gitignore
├── README.md
│
├── producer/
├── kafka/
├── spark/
├── database/
└── dashboard/
```

### Do NOT commit

```text
.venv/
.env
__pycache__/
*.log
data/
```

### DO commit

```text
pyproject.toml
uv.lock
README.md
source code
```

---

## Quick Setup

For a new teammate, the entire Python setup is:

```bash
git clone https://github.com/giunzz/bigdata_project.git
cd bigdata_project
uv sync
```

After that, the Python environment is ready.

> **Note:** `uv sync` only sets up the Python environment and Python packages. Apache Kafka, Apache Spark, and MongoDB are separate system services and must be installed/configured separately.
