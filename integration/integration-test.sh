#!/bin/bash

declare -a arr=("test_models_are_executed" "test_models_with_invalid_sql")

## now loop through the above array
for i in "${arr[@]}"
do
  echo "$i"
  dbt compile --project-dir ./integration/projects/$i --profiles-dir ./integration/profiles
  python3 -m dbt_dry_run --manifest-path ./integration/projects/$i/target/manifest.json --profiles-dir ./integration/profiles default
  dbt clean --project-dir ./integration/projects/$i
done
