#!/usr/bin/env zsh
docker-compose exec airflow-apiserver airflow connections add aws_localstack \
    --conn-type aws \
    --conn-host http://localstack:4566 \
    --conn-login "test" \
    --conn-password "test" \
    --conn-extra '{"region_name":"eu-west-1"}'