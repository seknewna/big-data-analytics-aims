# Big Data Analytics with Python — AIMS Rwanda

Course website and lab materials for a Masters course on big data analytics, from theory to practice: storage formats, distributed computing, Polars/DuckDB/Dask, Apache Spark, machine learning at scale, streaming, geospatial and satellite data, text and graph analytics, pipelines, and ethics/data protection (including Rwanda's Law No. 058/2021).

The site is built with [MkDocs Material](https://squidfunk.github.io/mkdocs-material/) and deployed to GitHub Pages automatically by GitHub Actions.

## Publish the site (one-time setup, ~10 minutes)

1. Create a new **public** repository on GitHub, e.g. `big-data-analytics-aims`.
2. Edit `mkdocs.yml`: replace `YOUR-GITHUB-USERNAME` in `site_url` and `repo_url`.
3. Push this folder:
   ```bash
   git init
   git add .
   git commit -m "Initial course site"
   git branch -M main
   git remote add origin https://github.com/YOUR-GITHUB-USERNAME/big-data-analytics-aims.git
   git push -u origin main
   ```
4. Wait for the **Actions** tab to show a green tick for "Deploy course site to GitHub Pages". It creates a `gh-pages` branch.
5. In the repository go to **Settings → Pages → Build and deployment → Source: Deploy from a branch**, choose branch **`gh-pages`**, folder **`/ (root)`**, and save.
6. After a minute the site is live at `https://YOUR-GITHUB-USERNAME.github.io/big-data-analytics-aims/`.

Every later push to `main` rebuilds and redeploys the site.

## Preview locally

```bash
pip install -r requirements-docs.txt
mkdocs serve        # open http://127.0.0.1:8000 — reloads as you edit
```

## Repository layout

```text
mkdocs.yml                 site configuration and navigation
docs/
  index.md                 home page
  syllabus.md, setup.md    course guide
  chapters/                13 chapters (theory, code, lab, review questions)
  assignments/             5 assignments + capstone with rubrics
  resources/               data sources, cheat sheets, glossary, reading
labs/
  docker-compose.yml       JupyterLab + PySpark (+ optional Kafka)
  download_data.py         fetch NYC taxi and Wikipedia pageview samples
  minimr.py, wordcount.py  teaching MapReduce engine (Chapter 4)
requirements.txt, environment.yml
.github/workflows/deploy.yml
```

## Editing content

Each page is a Markdown file under `docs/`. Supported extras: admonitions (`!!! tip`), tabs (`=== "Polars"`), Mermaid diagrams, LaTeX maths (`\( ... \)` and `\[ ... \]`), and copy buttons on code blocks. To add a page, create the file and add it to `nav:` in `mkdocs.yml`.

## Licence

Suggested: content under CC BY 4.0, code under MIT. Add a `LICENSE` file of your choice.
