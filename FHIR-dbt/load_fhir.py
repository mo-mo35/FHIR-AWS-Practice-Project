import duckdb
import json
import os
import glob
import pandas as pd

con = duckdb.connect("fhir.duckdb")

patients = []
conditions = []
encounters = []
medications = []
immunizations = []
observations = []

fhir_files = glob.glob("output/fhir/*.json")

for filepath in fhir_files:
    with open(filepath, "r", encoding = "utf-8") as f:
        bundle = json.load(f)

    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        rtype = resource.get("resourceType")

        if rtype == "Patient":
            patients.append({
                "id": resource.get("id"),
                "gender": resource.get("gender"),
                "birthDate": resource.get("birthDate"),
                "city": resource.get("address", [{}])[0].get("city"),
                "state": resource.get("address", [{}])[0].get("state")
            })

        elif rtype == "Condition":
            patients_ref = resource.get("subject", {}).get("reference", "")
            conditions.append({
                "id": resource.get("id"),
                "patient_id": patients_ref.replace("urn:uuid:", ""),
                "code_text": resource.get("code", {}).get("text"),
                "onset_date": resource.get("onsetDateTime")
            })

        elif rtype == "Encounter":
            patients_ref = resource.get("subject", {}).get("reference", "")
            encounters.append({
                "id": resource.get("id"),
                "patient_id": patients_ref.replace("urn:uuid:", ""),
                "type": resource.get("type", [{}])[0].get("text"),
                "start": resource.get("period", {}).get("start"),
                "end": resource.get("period", {}).get("end")
            })

        elif rtype == "MedicationRequest":
            patients_ref = resource.get("subject", {}).get("reference", "")
            medications.append({
                "id": resource.get("id"),
                "patient_id": patients_ref.replace("urn:uuid:", ""),
                "medication": resource.get("medicationCodeableConcept", {}).get("text"),
                "authored_on": resource.get("authoredOn")
            })

        elif rtype == "Immunization":
            immunizations.append({
                "id": resource.get("id"),
                "patient_id": resource.get("patient", {}).get("reference", "").replace("urn:uuid:", ""),
                "vaccine_text": resource.get("vaccineCode", {}).get("text"),
                "date": resource.get("occurrenceDateTime")
            })

        elif rtype == "Observation":
            patient_ref = resource.get("subject", {}).get("reference", "")
            observations.append({
                "id": resource.get("id"),
                "patient_id": patient_ref.replace("urn:uuid:", ""),
                "code_text": resource.get("code", {}).get("text"),
                "value": resource.get("valueQuantity", {}).get("value"),
                "unit": resource.get("valueQuantity", {}).get("unit"),
                "effective_date": resource.get("effectiveDateTime")
            })

df_patients = pd.DataFrame(patients)
df_conditions = pd.DataFrame(conditions)
df_encounters = pd.DataFrame(encounters)
df_medications = pd.DataFrame(medications)
df_immunizations = pd.DataFrame(immunizations)
df_observations = pd.DataFrame(observations)

con.execute("CREATE OR REPLACE TABLE patients AS SELECT * FROM df_patients")
con.execute("CREATE OR REPLACE TABLE conditions AS SELECT * FROM df_conditions")
con.execute("CREATE OR REPLACE TABLE encounters AS SELECT * FROM df_encounters")
con.execute("CREATE OR REPLACE TABLE medications AS SELECT * FROM df_medications")
con.execute("CREATE OR REPLACE TABLE immunizations AS SELECT * FROM df_immunizations")
con.execute("CREATE OR REPLACE TABLE observations AS SELECT * FROM df_observations")
print(f"Loaded: {len(patients)} patients, {len(conditions)} conditions, {len(encounters)} encounters, {len(medications)} medications, {len(immunizations)} immunizations, {len(observations)} observations")
con.close()