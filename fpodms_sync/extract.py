import gzip
import json
import os
import pathlib

import fpodms
from google.cloud import storage


def main():
    file_dir = pathlib.Path(__file__).absolute().parent
    data_dir = file_dir / "data"

    current_school_year_id = int(os.getenv("CURRENT_ACADEMIC_YEAR"))

    gcs_client = storage.Client()
    gcs_bucket = gcs_client.bucket(os.getenv("GCS_BUCKET_NAME"))

    fp = fpodms.FPODMS(
        email_address=os.getenv("FPODMS_USERNAME"),
        password=os.getenv("FPODMS_PASSWORD"),
    )

    all_years = fp.api.all_years()
    current_school_year_name = next(
        iter([y for y in all_years if y["id"] == current_school_year_id]), None
    ).get("name")

    # get all assessment exports
    for fn in fp.export.all_exports:
        print(fn.__name__)
        export = fn(year=current_school_year_id)
        export_data = export.data

        export_dir = file_dir / "data" / fn.__name__
        if not export_dir.exists():
            export_dir.mkdir(parents=True)
            print(f"\tCreated {'/'.join(export_dir.parts[-3:])}...")

        export_filepath = export_dir / export.filename
        with export_filepath.open("w") as f:
            f.write(export_data)
        print(f"\tSaved '{export.filename}' to {export_filepath}")

        destination_blob_name = "fpodms/" + "/".join(export_filepath.parts[-2:])
        blob = gcs_bucket.blob(destination_blob_name)
        blob.upload_from_filename(export_filepath)
        print(f"\tUploaded to {destination_blob_name}!\n")

    # get all classroom data
    endpoint_name = "bas_classes"
    schools = fp.api.school_by_district(school_year_id=current_school_year_id)
    for s in schools:
        print(f"{s.get('name')} classes...")
        school_id = s.get("id")
        classes = fp.api.basclass_by_school(school_id, current_school_year_id)

        classes_updated = []
        for c in classes:
            c.update(
                dict(schoolYear=current_school_year_name, schoolName=s.get("name"))
            )
            classes_updated.append(c)

        endpoint_dir = data_dir / endpoint_name
        if not endpoint_dir.exists():
            endpoint_dir.mkdir(parents=True)
            print(f"\tCreated {'/'.join(endpoint_dir.parts[-3:])}...")

        classes_filename = (
            f"{endpoint_name}_{school_id}_{s.get('schoolYearId')}.json.gz"
        )
        classes_filepath = endpoint_dir / classes_filename
        with gzip.open(classes_filepath, "wt", encoding="utf-8") as f:
            json.dump(classes_updated, f)
        print(f"\tSaved '{classes_filename}' to {classes_filepath}")

        destination_blob_name = "fpodms/" + "/".join(classes_filepath.parts[-2:])
        blob = gcs_bucket.blob(destination_blob_name)
        blob.upload_from_filename(classes_filepath)
        print(f"\tUploaded to {destination_blob_name}!\n")


if __name__ == "__main__":
    try:
        main()
    except Exception as xc:
        print(xc)
