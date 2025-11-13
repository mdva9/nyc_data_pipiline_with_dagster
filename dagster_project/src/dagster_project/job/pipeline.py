from dagster import job
from dagster_project.scripts.extract import extract
from dagster_project.scripts.transform import transform
from dagster_project.scripts.load import load

@job
def pipeline():
    load(transform(extract()))

