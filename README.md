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
![alt text](image.png)
