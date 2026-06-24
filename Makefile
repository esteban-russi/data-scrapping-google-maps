# ──────────────────────────────────────────────
# Volunteer ↔ Business Matcher — Makefile
# ──────────────────────────────────────────────

APP        := data-scrapping
PROJECT    := ebay-400411
REGION     := europe-west2
IMAGE      := gcr.io/$(PROJECT)/$(APP)
PYTHON     := .venv/bin/python

.PHONY: help install run clean lint docker-build docker-run deploy logs

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

# ── Local ─────────────────────────────────────

install: ## Create venv & install deps
	python3 -m venv .venv
	.venv/bin/pip install -r requirements.txt

run: ## Run the matching pipeline locally
	$(PYTHON) main.py

clean: ## Remove caches and output
	rm -rf __pycache__ .pytest_cache
	rm -f data/matches.csv

lint: ## Lint with ruff (install if missing)
	.venv/bin/pip install -q ruff
	.venv/bin/ruff check .

# ── Docker ────────────────────────────────────

docker-build: ## Build Docker image locally
	docker build -t $(APP) .

docker-run: docker-build ## Build & run in Docker
	docker run --rm $(APP)

# ── GCP Deploy ────────────────────────────────

deploy: ## Deploy to Cloud Run Jobs on GCP
	gcloud builds submit --tag $(IMAGE) --project $(PROJECT)
	gcloud run jobs deploy $(APP) \
		--image $(IMAGE) \
		--region $(REGION) \
		--project $(PROJECT) \
		--task-timeout 300
	@echo "\n✅ Deployed. Run with: make run-cloud"

run-cloud: ## Execute the Cloud Run Job
	gcloud run jobs execute $(APP) \
		--region $(REGION) \
		--project $(PROJECT) \
		--wait

logs: ## Tail Cloud Run Job logs
	gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=$(APP)" \
		--project $(PROJECT) \
		--limit 50 \
		--format "table(timestamp,textPayload)"
