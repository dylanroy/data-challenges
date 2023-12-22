## Data Engineer Challenge - Nick Agel
Prerequisites
```
brew install bash
brew install sqlite
brew install docker
```

Application runs on Docker & commands are premade with Makefile to minimize human error

Project Setup
```
make # display all commands & contains context
make init # builds local dependencies
make build # builds docker image for spark job
```

Running Job
```
make run # runs spark job with default rank on order_id
make run RANK_FIELD=["cost"|"date_time"|"order_id"|"repair_details.technician"|"repair_details.repair_parts.part._name"|"repair_details.repair_parts.part._quantity"|"status"] ## change the rank field
```

Running Tests
```
make test
```

Auto Formats & Links
- black
- pylint
- isort
- sqlfmt

```
make format # auto format & lint
make verify-format # verify format is up to code
```

### Observations & Approach
- Docker - AWS standard image for Glue which is serverless spark. Production like environment locally 
- Spark - An Industry standard for data transformation & manipulation. 
- Spark SQL - Expose business logic, simpler for non technical people to read & potentially solve the problem without technical intervention
- XML-spark - Databricks supports xml read & schema support to ensure data quality. There was a corrupt xml record. This was dropped for final output. Not ignored completely, it is logged in output

### Taking this to production
- CI/CD pipeline
- Terraform 

pipeline - every push enforces the following
- code quality
- infrastructure quality
- ability to merge

pipeline - on merge
- package & uploads to s3 with new version
- updates terraform to point to new version
