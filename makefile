SPARK_IMAGE_NAME=dynatron_spark
SPARK_DOCKER_FILE_NAME=DockerfileSpark
CURRENT_DIR = $(shell pwd)
SCALA_VERSION=2.12
SPARK_XML_VERSION=0.17.0
SPARK_XML_URL=https://repo1.maven.org/maven2/com/databricks/spark-xml_$(SCALA_VERSION)/$(SPARK_XML_VERSION)/spark-xml_$(SCALA_VERSION)-$(SPARK_XML_VERSION).jar
SQLITE_VERSION=3.44.1.0
SQLITE_URL=https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/$(SQLITE_VERSION)/sqlite-jdbc-$(SQLITE_VERSION).jar


.PHONY: default help
default help: ## show help message
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m\033[0m\n"} /^[$$()% 0-9a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

.PHONY: init
init: get-jars ## install requirements
	pip install -r requirements.txt

.PHONY: get-jars
get-jars: ## download jar dependencies
	mkdir -p src/jars
	cd src/jars && curl -O $(SPARK_XML_URL)
	cd src/jars && curl -O $(SQLITE_URL)

.PHONY: format
format: ## format python code
	black . --exclude="tables/|data/"

.PHONY: build
build: ## build docker image
	@echo '..building $(SPARK_IMAGE_NAME) image..'
	docker build --compress --tag $(SPARK_IMAGE_NAME) --file $(SPARK_DOCKER_FILE_NAME) . 

.PHONY: run
run: ## run script
	docker run -it -v $(CURRENT_DIR)/.aws:/home/glue_user/.aws \
		-v $(CURRENT_DIR):/home/glue_user/workspace/ \
		-v $(CURRENT_DIR)/data-engineer/data:/root/data \
		-v $(CURRENT_DIR)/sqlite:/db \
		-e DISABLE_SSL=true \
		--rm -p 4040:4040 \
		-p 18080:18080 \
		--name glue_spark_submit $(SPARK_IMAGE_NAME) spark-submit \
			--jars /home/glue_user/workspace/src/jars/spark-xml_$(SCALA_VERSION)-$(SPARK_XML_VERSION).jar,/home/glue_user/workspace/src/jars/sqlite-jdbc-$(SQLITE_VERSION).jar \
			/home/glue_user/workspace/src/script.py \
			2>&1 || true  

.PHONY: bash
bash: ## interactive bash w/ pyspark
	docker run -it -v $(CURRENT_DIR)/.aws:/home/glue_user/.aws \
		-v $(CURRENT_DIR):/home/glue_user/workspace/ \
		-v $(CURRENT_DIR)/data-engineer/data/:/root/data \
		-v $(CURRENT_DIR)/sqlite:/db \
		-e DISABLE_SSL=true\
		--rm -p 4040:4040 \
		-p 18080:18080 \
		--name glue_spark_submit $(SPARK_IMAGE_NAME) \
			pyspark \
			--jars /home/glue_user/workspace/src/jars/spark-xml_$(SCALA_VERSION)-$(SPARK_XML_VERSION).jar,/home/glue_user/workspace/src/jars/sqlite-jdbc-$(SQLITE_VERSION).jar

.PHONY: test
test: ## run unit tests
	docker run -it -v $(CURRENT_DIR)/.aws:/home/glue_user/.aws \
		-v $(CURRENT_DIR):/home/glue_user/workspace/ \
		-v $(CURRENT_DIR)/data-engineer/data/:/root/data \
		-v $(CURRENT_DIR)/sqlite:/db \
		-e DISABLE_SSL=true\
		--rm -p 4040:4040 \
		-p 18080:18080 \
		--name glue_pytest $(SPARK_IMAGE_NAME) \
			-c "python3 -m pytest /home/glue_user/workspace/src/test/ -vvv"
			