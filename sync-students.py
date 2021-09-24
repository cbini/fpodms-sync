import os
import pathlib
import traceback

import fpodms
import pandas as pd
from dotenv import load_dotenv

from datarobot.utilities import email

load_dotenv()

FPODMS_USERNAME = os.getenv("FPODMS_USERNAME")
FPODMS_PASSWORD = os.getenv("FPODMS_PASSWORD")
ROSTER_FILEPATH = os.getenv("ROSTER_FILEPATH")
CURRENT_ACADEMIC_YEAR = int(os.getenv("CURRENT_ACADEMIC_YEAR"))
FIRST_ACADEMIC_YEAR = int(os.getenv("FIRST_ACADEMIC_YEAR"))
PROJECT_PATH = pathlib.Path(__file__).absolute().parent


def main():
    ## load SIS rosters into pandas
    print("Reading SIS roster into pandas...")
    roster_df = pd.read_json(ROSTER_FILEPATH)

    ## create instance of F&P client
    print("Initializing F&P client...")
    fp = fpodms.FPODMS(email_address=FPODMS_USERNAME, password=FPODMS_PASSWORD)

    ## get all years
    print("Pulling all years from F&P...")
    all_years = fp.api.all_years()
    all_years = [y for y in all_years if y["id"] >= FIRST_ACADEMIC_YEAR]

    ## get all schools
    print("Pulling all schools from F&P...")
    all_schools = fp.api.school_by_district(school_year_id=CURRENT_ACADEMIC_YEAR)

    ## get all students currently in F&P
    print("Pulling all students from F&P...")
    all_students_years = []
    for y in all_years:
        print(f"\t{y.get('name')}")
        for s in all_schools:
            school_students = fp.api.students_by_school_and_school_year(
                school_id=s.get("id"), school_year_id=y.get("id")
            )
            all_students_years.extend(school_students)

    ## read F&P students into pandas
    ## drop duplicates and convert id to int
    print("Reading F&P rosters into pandas...")
    fp_df = pd.DataFrame(all_students_years)
    fp_df = fp_df[["studentId", "studentIdentifier"]].drop_duplicates()
    fp_df.studentIdentifier = fp_df.studentIdentifier.astype("float").astype("Int64")

    ## match FP rosters to SIS
    print("Matching SIS roster to F&P...")
    merge_df = pd.merge(left=roster_df, right=fp_df, how="left", on="studentIdentifier")

    ## transform columns
    merge_df.studentId = merge_df.studentId.astype("float").astype("Int64")
    merge_df["schoolYearId"] = CURRENT_ACADEMIC_YEAR
    merge_df["schoolId"] = merge_df.schoolName.apply(
        lambda n: next(
            iter([s.get("id") for s in all_schools if s.get("name") == n]), None
        )
    )
    merge_records = merge_df.to_dict(orient="records")

    print("Processing student records...")
    for r in merge_records:
        if r["studentId"] is pd.NA:
            print(f"\t{r['firstName']} {r['lastName']} {r['studentIdentifier']}")
            try:
                fp.api.add_student(**r)
                print("\t\tCREATED")
            except Exception as xc:
                print(xc)
                email_subject = "F&P Student Sync Error"
                email_body = f"{r}\n\n{xc}\n\n{traceback.format_exc()}"
                email.send_email(subject=email_subject, body=email_body)
        else:
            stu_enr_all = [
                s
                for s in all_students_years
                if s.get("studentIdentifier") == str(r["studentIdentifier"])
            ]
            stu_enr_cur = [
                e for e in stu_enr_all if e.get("schoolYearId") == CURRENT_ACADEMIC_YEAR
            ]
            if not stu_enr_cur:
                print(f"\t{r['firstName']} {r['lastName']} {r['studentIdentifier']}")
                try:
                    fp.api.add_student_to_school_and_grade_and_maybe_class(**r)
                    print("\t\tUPDATED")
                except Exception as xc:
                    print(xc)
                    email_subject = "F&P Student Sync Error"
                    email_body = f"{r}\n\n{xc}\n\n{traceback.format_exc()}"
                    email.send_email(subject=email_subject, body=email_body)


if __name__ == "__main__":
    try:
        main()
    except Exception as xc:
        print(xc)
        email_subject = "F&P Student Sync Error"
        email_body = f"{xc}\n\n{traceback.format_exc()}"
        email.send_email(subject=email_subject, body=email_body)
