import gzip
import json
import os
import pathlib
import traceback

import fpodms
from google.cloud import storage

from datarobot.utilities import email

FPODMS_USERNAME = os.getenv("FPODMS_USERNAME")
FPODMS_PASSWORD = os.getenv("FPODMS_PASSWORD")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
CURRENT_SCHOOL_YEAR_ID = int(os.getenv("CURRENT_ACADEMIC_YEAR"))
PROJECT_PATH = pathlib.Path(__file__).absolute().parent


def main():
    gcs_client = storage.Client()
    gcs_bucket = gcs_client.bucket(GCS_BUCKET_NAME)

    fp = fpodms.FPODMS(email_address=FPODMS_USERNAME, password=FPODMS_PASSWORD)

    all_years = fp.api.all_years()
    current_school_year_name = next(
        iter([y for y in all_years if y["id"] == CURRENT_SCHOOL_YEAR_ID]), None
    ).get("name")

    data_path = PROJECT_PATH / "data"

    # get all assessment exports
    for fn in fp.export.all_exports:
        print(fn.__name__)
        export = fn(year=CURRENT_SCHOOL_YEAR_ID)
        export_data = export.data

        data_path = PROJECT_PATH / "data" / fn.__name__
        if not data_path.exists():
            data_path.mkdir(parents=True)
            print(f"\tCreated {'/'.join(data_path.parts[-3:])}...")

        data_filename = data_path / export.filename
        with data_filename.open("w") as f:
            f.write(export_data)
        print(f"\tSaved '{export.filename}' to {data_filename}")

        destination_blob_name = "fpodms/" + "/".join(data_filename.parts[-2:])
        blob = gcs_bucket.blob(destination_blob_name)
        blob.upload_from_filename(data_filename)
        print(f"\tUploaded to {destination_blob_name}!\n")

    # get all classroom data
    endpoint_name = "bas_classes"
    schools = fp.api.school_by_district(school_year_id=CURRENT_SCHOOL_YEAR_ID)
    for s in schools:
        print(f"{s.get('name')} classes...")
        school_id = s.get("id")
        classes = fp.api.basclass_by_school(school_id, CURRENT_SCHOOL_YEAR_ID)

        classes_updated = []
        for c in classes:
            c.update(
                dict(schoolYear=current_school_year_name, schoolName=s.get("name"))
            )
            classes_updated.append(c)

        data_path = PROJECT_PATH / "data" / endpoint_name
        if not data_path.exists():
            data_path.mkdir(parents=True)
            print(f"\tCreated {'/'.join(data_path.parts[-3:])}...")

        classes_filename = (
            f"{endpoint_name}_{school_id}_{s.get('schoolYearId')}.json.gz"
        )
        data_filename = data_path / classes_filename
        with gzip.open(data_filename, "wt", encoding="utf-8") as f:
            json.dump(classes_updated, f)
        print(f"\tSaved '{classes_filename}' to {data_filename}")

        destination_blob_name = "fpodms/" + "/".join(data_filename.parts[-2:])
        blob = gcs_bucket.blob(destination_blob_name)
        blob.upload_from_filename(data_filename)
        print(f"\tUploaded to {destination_blob_name}!\n")


if __name__ == "__main__":
    try:
        main()
    except Exception as xc:
        email_subject = "F&P Extract Error"
        email_body = f"{xc}\n\n{traceback.format_exc()}"
        email.send_email(subject=email_subject, body=email_body)
