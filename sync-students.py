import os
import pathlib

import fpodms
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

FPODMS_USERNAME = os.getenv("FPODMS_USERNAME")
FPODMS_PASSWORD = os.getenv("FPODMS_PASSWORD")
ROSTER_FILEPATH = os.getenv("ROSTER_FILEPATH")
CURRENT_ACADEMIC_YEAR = int(os.getenv("CURRENT_ACADEMIC_YEAR"))
PROJECT_PATH = pathlib.Path(__file__).absolute().parent


def main():
    ## load PS rosters into pandas
    print("Reading PS rosters into pandas...")
    roster_df = pd.read_json(ROSTER_FILEPATH)

    ## create instance of F&P client
    print("Initializing F&P client...")
    fp = fpodms.FPODMS(email_address=FPODMS_USERNAME, password=FPODMS_PASSWORD)

    ## get all years
    print("Pulling all years from F&P...")
    all_years = fp.api.all_years()

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
    fp_df.studentIdentifier = fp_df.studentIdentifier.astype(int)

    ## match FP rosters to PS
    print("Matching PS rosters to F&P...")
    merge_df = pd.merge(left=roster_df, right=fp_df, how="left", on="studentIdentifier")

    ## add academic year to merged df
    ## translate schoolName to schoolId
    merge_df["schoolYearId"] = CURRENT_ACADEMIC_YEAR
    merge_df["schoolId"] = merge_df.schoolName.apply(
        lambda n: next(
            iter([s.get("id") for s in all_schools if s.get("name") == n]), None
        )
    )

    ## subset unmatched students
    print("Filtering students for record creation...")
    create_df = merge_df[merge_df.studentId.isnull()]
    create_records = create_df.to_dict(orient="records")

    ## subset unmatched students
    print("Filtering students for record updates...")
    update_df = merge_df[merge_df.studentId.notnull()]
    update_df.studentId = update_df.studentId.astype(int)
    update_records = update_df.to_dict(orient="records")

    ## create new students
    for c in create_records:
        try:
            print(
                f"\tCREATING {c['firstName']} {c['lastName']} {c['studentIdentifier']}"
            )
            fp.api.add_student(**c)
        except Exception as xc:
            print(xc)

    print(f"\tCreated {len(create_records)} student records...")

    ## check for existing enrollment for current school year
    print("Updating students for new year...")
    n_updated = 0
    for u in update_records:
        student_enrollments = [
            s
            for s in all_students_years
            if s.get("studentIdentifier") == str(u["studentIdentifier"])
        ]
        current_year_enrollments = [
            e
            for e in student_enrollments
            if e.get("schoolYearId") == CURRENT_ACADEMIC_YEAR
        ]
        if not current_year_enrollments:
            try:
                print(
                    f"\tUPDATING {u['firstName']} {u['lastName']} {u['studentIdentifier']}"
                )
                fp.api.add_student_to_school_and_grade_and_maybe_class(**u)
                n_updated += 1
            except Exception as xc:
                print(xc)

    print(f"\tUpdated {n_updated} student records...")


if __name__ == "__main__":
    try:
        main()
    except Exception as xc:
        print(xc)
