import psycopg2
import pandas as pd
from faker import Faker
# Database connection parameters
db_params = {
   'host': 'localhost',
   'database': 'Project 1 - Healthcare Database',
   'user': 'user',
   'password': 'password'
}


# Connect to PostgreSQL database
conn = psycopg2.connect(**db_params)
cursor = conn.cursor()


# Initialize Faker for synthetic data generation
fake = Faker()


# Generate synthetic insurance providers
insurance_providers = []
for _ in range(10):
   provider = (
       fake.company(),
       fake.address()
   )
   insurance_providers.append(provider)


# Convert to DataFrame
insurance_df = pd.DataFrame(insurance_providers, columns=['provider_name', 'contact_info'])


# Insert data into InsuranceProviders table
for index, row in insurance_df.iterrows():
   cursor.execute(
       """
       INSERT INTO InsuranceProviders (provider_name, contact_info)
       VALUES (%s, %s)
       """,
       (row['provider_name'], row['contact_info'])
   )
conn.commit()
print("InsuranceProviders table populated successfully.")


departments = [
   ('Emergency', 'First Floor', 'Emergency care services'),
   ('Cardiology', 'Second Floor', 'Heart-related treatments'),
   ('Neurology', 'Third Floor', 'Brain and nervous system services'),
   ('Orthopedics', 'Second Floor', 'Bone and muscle treatments'),
   ('Oncology', 'Fourth Floor', 'Cancer treatments'),
   ('Pediatrics', 'First Floor', 'Child healthcare services')
]


# Define new departments
new_departments = [
   ('InternalMedicine', 'Third Floor', 'Internal medicine treatments'),
   ('Family/GeneralPractice', 'First Floor', 'General practice services'),
   ('Surgery', 'Second Floor', 'Surgical procedures and operations'),
   ('Emergency/Trauma', 'Ground Floor', 'Emergency and trauma care'),
   ('General Medicine', 'Second Floor', 'General medical services') 
]


departments_df = pd.DataFrame(departments, columns=['department_name', 'location', 'services_offered'])


# Fetch department IDs from the database
cursor.execute("SELECT department_id, department_name FROM HospitalDepartments")
departments = cursor.fetchall()
department_mapping = {dept_name: dept_id for dept_id, dept_name in departments}


# Generate synthetic healthcare providers
providers = []
specialties = ['Emergency Medicine', 'Cardiology', 'Neurology', 'Orthopedics', 'Oncology', 'Pediatrics']
for _ in range(20):
   specialty = fake.random_choices(elements=specialties, length=1)[0]
   department_id = department_mapping.get(specialty.split()[0], None)
   provider = (
       fake.first_name(),
       fake.last_name(),
       specialty,
       department_id,
       fake.phone_number()
   )
   providers.append(provider)


providers_df = pd.DataFrame(providers, columns=['first_name', 'last_name', 'specialty', 'department_id', 'contact_info'])


# Insert data into HealthcareProviders table
for index, row in providers_df.iterrows():
   cursor.execute(
       """
       INSERT INTO HealthcareProviders (first_name, last_name, specialty, department_id, contact_info)
       VALUES (%s, %s, %s, %s, %s)
       """,
       (row['first_name'], row['last_name'], row['specialty'], row['department_id'], row['contact_info'])
   )


conn.commit()
print("HealthcareProviders table populated successfully.")


medications = [
   ('Atorvastatin', 'Lipitor', 'Tablet', 'Pfizer'),
   ('Levothyroxine', 'Synthroid', 'Tablet', 'AbbVie'),
   ('Lisinopril', 'Prinivil', 'Tablet', 'Merck'),
   ('Metformin', 'Glucophage', 'Tablet', 'Bristol-Myers Squibb'),
   ('Amlodipine', 'Norvasc', 'Tablet', 'Pfizer'),
   ('Metoprolol', 'Lopressor', 'Tablet', 'Novartis')
]


medications_df = pd.DataFrame(medications, columns=['generic_name', 'brand_name', 'dosage_form', 'manufacturer'])




# Insert data into Medications table
for index, row in medications_df.iterrows():
   cursor.execute(
       """
       INSERT INTO Medications (generic_name, brand_name, dosage_form, manufacturer)
       VALUES (%s, %s, %s, %s)
       """,
       (row['generic_name'], row['brand_name'], row['dosage_form'], row['manufacturer'])
   )


conn.commit()
print("Medications table populated successfully.")


# Medical Code Generation and Integration


medical_codes = [
   ('Diagnosis', 'E11.9', 'Type 2 diabetes mellitus without complications'),
   ('Diagnosis', 'I10', 'Essential (primary) hypertension'),
   ('Procedure', '0W3P0ZZ', 'Insertion of Infusion Device into Subcutaneous Tissue'),
   ('Procedure', '3E0A3MZ', 'Introduction of Other Antineoplastic into Peripheral Vein'),
]


medical_codes_df = pd.DataFrame(medical_codes, columns=['code_type', 'code_value', 'description'])


# Insert data into MedicalCodes table
for index, row in medical_codes_df.iterrows():
   cursor.execute(
       """
       INSERT INTO MedicalCodes (code_type, code_value, description)
       VALUES (%s, %s, %s)
       """,
       (row['code_type'], row['code_value'], row['description'])
   )


conn.commit()
print("MedicalCodes table populated successfully.")


## Static Patient Table
# Load Kaggle dataset
kaggle_df = pd.read_csv("/Users/ateraalam/Desktop/DATA ANALYTICS ENGINEERING/IE 6750/PROJECT_1_Datasets/hospital_readmissions.csv")


# Generate unique patient IDs
file_path = "/Users/ateraalam/Desktop/DATA ANALYTICS ENGINEERING/IE 6750/PROJECT_1_Datasets/hospital_readmissions.csv"


kaggle_df['patient_id'] = kaggle_df.index + 1


# Estimate date_of_birth based on age bracket
import pandas as pd
from datetime import datetime


def estimate_dob(age_range):
   # Clean up unwanted characters from the age range
   age_range = age_range.replace('[', '').replace(']', '').replace(')', '').replace('(', '').strip()
   age_range = age_range.split('-')


   if len(age_range) == 2:
       try:
           # Calculate the average age
           age = int((int(age_range[0].strip()) + int(age_range[1].strip())) / 2)
           # Estimate the year of birth
           birth_year = pd.Timestamp.now().year - age
           # Create a date object, assuming January 1st as the birth date
           date_of_birth = datetime(birth_year, 1, 1).date()
           return date_of_birth
       except ValueError:
           print(f"Invalid age range: {age_range}")
           return None
   else:
       print(f"Unexpected format in age range: {age_range}")
       return None
kaggle_df['date_of_birth'] = kaggle_df['age'].apply(estimate_dob)


# Generate synthetic gender and contact_info
kaggle_df['gender'] = kaggle_df['patient_id'].apply(lambda x: fake.random_element(elements=('Male', 'Female')))
kaggle_df['contact_info'] = kaggle_df['patient_id'].apply(lambda x: fake.address())


# Assign insurance_provider_id randomly
cursor.execute("SELECT insurance_provider_id FROM InsuranceProviders")
insurance_provider_ids = [row[0] for row in cursor.fetchall()]
kaggle_df['insurance_provider_id'] = kaggle_df['patient_id'].apply(lambda x: fake.random_element(elements=insurance_provider_ids))


# Prepare patients DataFrame
patients_df = kaggle_df[['patient_id', 'date_of_birth', 'gender', 'contact_info', 'insurance_provider_id']].drop_duplicates(subset='patient_id')


for index, row in kaggle_df.iterrows():
   cursor.execute(
       """
       INSERT INTO Patients (patient_id, date_of_birth, gender, contact_info, insurance_provider_id)
       VALUES (%s, %s, %s, %s, %s)
       ON CONFLICT (patient_id) DO UPDATE
       SET date_of_birth = EXCLUDED.date_of_birth,
           gender = EXCLUDED.gender,
           contact_info = EXCLUDED.contact_info,
           insurance_provider_id = EXCLUDED.insurance_provider_id
       """,
       (
           row['patient_id'],
           row['date_of_birth'],
           row['gender'],
           row['contact_info'],
           row['insurance_provider_id'],
       )
   )
conn.commit()
print("Patients table populated successfully.")


### Transactional Tables


# Assign random admission_date
start_date = datetime(2020, 1, 1)
end_date = datetime(2020, 12, 31)
kaggle_df['admission_date'] = kaggle_df['patient_id'].apply(
   lambda x: fake.date_between_dates(date_start=start_date, date_end=end_date)
)


# Map 'diag_1' to 'reason_for_admission' using MedicalCodes
cursor.execute("SELECT code_value, description FROM MedicalCodes WHERE code_type = 'Diagnosis'")
code_mapping = {row[0]: row[1] for row in cursor.fetchall()}
kaggle_df['reason_for_admission'] = kaggle_df['diag_1'].map(code_mapping)


# Fetch department IDs
cursor.execute("SELECT department_id, department_name FROM HospitalDepartments")
departments = cursor.fetchall()
department_mapping = {dept_name: dept_id for dept_id, dept_name in departments}


# Map 'medical_specialty' to 'department_id'
kaggle_df['department_id'] = kaggle_df['medical_specialty'].map(department_mapping)


# Fetch provider IDs
cursor.execute("SELECT provider_id, department_id FROM HealthcareProviders")
providers = cursor.fetchall()
providers_df = pd.DataFrame(providers, columns=['provider_id', 'department_id'])


# Function to assign provider_id based on department_id
def assign_provider(dept_id):
   available_providers = providers_df[providers_df['department_id'] == dept_id]['provider_id'].tolist()
   if available_providers:
       return fake.random_element(elements=available_providers)
   else:
       return None


kaggle_df['provider_id'] = kaggle_df['department_id'].apply(assign_provider)


# Prepare admissions DataFrame
admissions_df = kaggle_df[['patient_id', 'admission_date', 'reason_for_admission', 'department_id', 'provider_id']]
for index, row in admissions_df.iterrows():
   # Handle NaN or None values in the relevant columns
   reason_for_admission = row['reason_for_admission'] if pd.notnull(row['reason_for_admission']) else 'Unknown'
   department_id = row['department_id'] if pd.notnull(row['department_id']) else None
   provider_id = row['provider_id'] if pd.notnull(row['provider_id']) else None
   # Insert into Admissions, omitting admission_id
   cursor.execute(
       """
       INSERT INTO Admissions (patient_id, admission_date, reason_for_admission, department_id, provider_id)
       VALUES (%s, %s, %s, %s, %s)
       """,
       (
           row['patient_id'],
           row['admission_date'],
           reason_for_admission,
           department_id,
           provider_id,
       )
   )


conn.commit()


print("Admissions table populated successfully.")
# Fetch admission_id and patient_id from the Admissions table
cursor.execute("SELECT admission_id, patient_id FROM Admissions")
admissions_from_db = cursor.fetchall()


# Create a mapping between patient_id and admission_id
admission_id_mapping = {row[1]: row[0] for row in admissions_from_db}


# Map admission_id back to kaggle_df based on patient_id
kaggle_df['admission_id'] = kaggle_df['patient_id'].map(admission_id_mapping)


# Ensure default values for department_id and provider_id
kaggle_df['reason_for_admission'].fillna('Unknown', inplace=True)
kaggle_df['department_id'].fillna(1, inplace=True) # Assuming 1 as the default department
kaggle_df['provider_id'].fillna(1, inplace=True)   # Assuming 1 as the default provider


# Update Admissions table with the correct values
for index, row in kaggle_df.iterrows():
   cursor.execute(
       """
       UPDATE Admissions
       SET reason_for_admission = %s,
           department_id = %s,
           provider_id = %s
       WHERE admission_id = %s
       """,
       (
           row['reason_for_admission'],
           row['department_id'],
           row['provider_id'],
           row['admission_id']
       )
   )


conn.commit()
print("Admissions table updated successfully.")


## Discharge Table


from datetime import timedelta


# Calculate discharge_date by adding 'time_in_hospital' days to 'admission_date'
kaggle_df['discharge_date'] = kaggle_df.apply(
   lambda row: row['admission_date'] + timedelta(days=row['time_in_hospital']), axis=1
)


# Generate 'discharge_disposition' and 'follow_up_instructions' synthetically
discharge_dispositions = ['Discharged to home', 'Transferred to another hospital', 'Left against medical advice']
kaggle_df['discharge_disposition'] = kaggle_df['patient_id'].apply(
   lambda x: fake.random_element(elements=discharge_dispositions)
)
kaggle_df['follow_up_instructions'] = 'Follow up with primary care physician in 2 weeks.'


# Prepare discharges DataFrame
discharges_df = kaggle_df[['discharge_date', 'discharge_disposition', 'follow_up_instructions']]


# Fetch admission_ids from Admissions table
cursor.execute("SELECT admission_id, patient_id, admission_date FROM Admissions")
admissions_records = cursor.fetchall()
admissions_df_db = pd.DataFrame(admissions_records, columns=['admission_id', 'patient_id', 'admission_date'])


# Merge admission_ids back into discharges_df
discharges_df = admissions_df_db.merge(
   discharges_df,
   left_index=True,
   right_index=True
)


# Insert data into Discharges table without specifying discharge_id
for index, row in discharges_df.iterrows():
   cursor.execute(
       """
       INSERT INTO Discharges (admission_id, discharge_date, discharge_disposition, follow_up_instructions)
       VALUES (%s, %s, %s, %s)
       ON CONFLICT (admission_id) DO NOTHING
       """,
       (
           int(row['admission_id']),
           row['discharge_date'],
           row['discharge_disposition'],
           row['follow_up_instructions']
       )
   )


conn.commit()
print("Discharges table populated successfully.")


import pandas as pd
from datetime import timedelta


# Prepare procedure records
procedure_records = []


# For each admission, I created procedure records based on 'diag_1', 'diag_2', 'diag_3'
for index, row in kaggle_df.iterrows():
   admission_id = row['admission_id']
   provider_id = row['provider_id']
   procedure_date = row['admission_date'] + timedelta(days=1)  # Assume procedure is done the next day after admission
   procedure_outcome = 'Successful'  # Assuming all procedures are successful


   # Iterate over diagnosis columns to assign procedure codes
   for diag_col in ['diag_1', 'diag_2', 'diag_3']:
       code_value = row[diag_col]
       if pd.notnull(code_value):
           cursor.execute(
               """
               SELECT code_id FROM MedicalCodes WHERE code_value = %s
               """,
               (code_value,)
           )
           result = cursor.fetchone()
          
           if result is not None:
               code_id = result[0]
               procedure_records.append({
                   'admission_id': admission_id,
                   'provider_id': provider_id,
                   'procedure_date': procedure_date,
                   'code_id': code_id,
                   'procedure_outcome': procedure_outcome
               })
           else:
               print(f"Warning: No code_id found for diagnosis {code_value}")


# Convert the records into a DataFrame
procedures_df = pd.DataFrame(procedure_records)


# Insert data into MedicalProcedures table
for index, row in procedures_df.iterrows():
   # Skip the row if provider_id is NaN
   if pd.isna(row['provider_id']):
       print(f"Skipping row {index} due to missing provider_id.")
       continue


   cursor.execute(
       """
       INSERT INTO MedicalProcedures (admission_id, provider_id, procedure_date, code_id, procedure_outcome)
       VALUES (%s, %s, %s, %s, %s)
       """,
       (
           int(row['admission_id']),
           int(row['provider_id']),  # Ensure it's a valid integer
           row['procedure_date'],
           int(row['code_id']),
           row['procedure_outcome']
       )
   )


conn.commit()


print("MedicalProcedures table populated successfully.")




# Generate and insert Medications Administered data


medications_administered_records = []


for index, row in kaggle_df.iterrows():
   admission_id = row['admission_id']
   medication_id = fake.random_int(min=1, max=6)  # Assuming there are 6 medications
   dosage = f"{fake.random_int(min=1, max=2)} tablet(s)"  # Generate dosage as 1 or 2 tablets
   route_of_administration = 'Oral'  # Assuming oral route for simplicity
   administration_date = row['admission_date'] + timedelta(days=1)  # Day after admission
  
   medications_administered_records.append({
       'admission_id': admission_id,
       'medication_id': medication_id,
       'dosage': dosage,
       'route_of_administration': route_of_administration,
       'administration_date': administration_date
   })


# Insert into MedicationsAdministered table
for record in medications_administered_records:
   cursor.execute(
       """
       INSERT INTO MedicationsAdministered (admission_id, medication_id, dosage, route_of_administration, administration_date)
       VALUES (%s, %s, %s, %s, %s)
       """,
       (
           record['admission_id'],
           record['medication_id'],
           record['dosage'],
           record['route_of_administration'],
           record['administration_date']
       )
   )


conn.commit()
print("MedicationsAdministered table populated successfully.")




# Generate and insert Laboratory Tests data


lab_tests_records = []


for index, row in kaggle_df.iterrows():
   admission_id = row['admission_id']
   test_type = fake.random_element(elements=('Glucose', 'A1C', 'Blood Panel', 'Lipid Panel'))
   test_code = fake.random_int(min=1000, max=9999)  # Assuming test codes are random integers
   test_date = row['admission_date'] + timedelta(days=2)  # Lab test done 2 days after admission
   result = fake.random_element(elements=('Normal', 'Abnormal'))
  
   lab_tests_records.append({
       'admission_id': admission_id,
       'test_type': test_type,
       'test_code': test_code,
       'test_date': test_date,
       'result': result
   })


# Insert into LaboratoryTests table
for record in lab_tests_records:
   cursor.execute(
       """
       INSERT INTO LaboratoryTests (admission_id, test_type, test_code, test_date, result)
       VALUES (%s, %s, %s, %s, %s)
       """,
       (
           record['admission_id'],
           record['test_type'],
           record['test_code'],
           record['test_date'],
           record['result']
       )
   )


conn.commit()
print("LaboratoryTests table populated successfully.")


# Generate and insert Readmission Records data


readmission_records = []


for index, row in kaggle_df.iterrows():
   original_admission_id = row['admission_id']
   readmission_date = row['admission_date'] + timedelta(days=fake.random_int(min=30, max=100))  # Readmission happens 30-100 days later
   readmission_reason = fake.sentence(nb_words=6)  # Random sentence for readmission reason
   associated_costs = fake.random_int(min=500, max=5000)
  
   readmission_records.append({
       'original_admission_id': original_admission_id,
       'readmission_date': readmission_date,
       'readmission_reason': readmission_reason,
       'associated_costs': associated_costs
   })


# Insert into ReadmissionRecords table
for record in readmission_records:
   cursor.execute(
       """
       INSERT INTO ReadmissionRecords (original_admission_id, readmission_date, readmission_reason, associated_costs)
       VALUES (%s, %s, %s, %s)
       """,
       (
           record['original_admission_id'],
           record['readmission_date'],
           record['readmission_reason'],
           record['associated_costs']
       )
   )


conn.commit()
print("ReadmissionRecords table populated successfully.")


# Generate and insert Billing Records data


billing_records = []


for index, row in kaggle_df.iterrows():
   admission_id = row['admission_id']
   total_charges = fake.random_int(min=5000, max=20000)  # Random total charge between 5000 and 20000
   claim_status = fake.random_element(elements=('Pending', 'Approved', 'Denied'))
   patient_out_of_pocket = fake.random_int(min=500, max=3000)
  
   billing_records.append({
       'admission_id': admission_id,
       'total_charges': total_charges,
       'claim_status': claim_status,
       'patient_out_of_pocket': patient_out_of_pocket
   })


# Insert into BillingRecords table
for record in billing_records:
   cursor.execute(
       """
       INSERT INTO BillingRecords (admission_id, total_charges, claim_status, patient_out_of_pocket)
       VALUES (%s, %s, %s, %s)
       """,
       (
           record['admission_id'],
           record['total_charges'],
           record['claim_status'],
           record['patient_out_of_pocket']
       )
   )


conn.commit()
print("BillingRecords table populated successfully.")




# Generate and insert Patient Feedback data


feedback_records = []


for index, row in kaggle_df.iterrows():
   admission_id = row['admission_id']
   satisfaction_score = fake.random_int(min=1, max=5)  # Satisfaction score from 1 to 5
   comments = fake.sentence(nb_words=10)  # Random comment
  
   feedback_records.append({
       'admission_id': admission_id,
       'satisfaction_score': satisfaction_score,
       'comments': comments
   })


# Insert into PatientFeedback table
for record in feedback_records:
   cursor.execute(
       """
       INSERT INTO PatientFeedback (admission_id, satisfaction_score, comments)
       VALUES (%s, %s, %s)
       """,
       (
           record['admission_id'],
           record['satisfaction_score'],
           record['comments']
       )
   )


conn.commit()
print("PatientFeedback table populated successfully.")




# Generate and insert Staff Scheduling data


staff_scheduling_records = []


for index, row in providers_df.iterrows():
   provider_id = row['provider_id']
   shift_date = fake.date_between(start_date='-30d', end_date='today')  # Last 30 days
   shift_start_time = fake.time()
   shift_end_time = fake.time()
   department_id = row['department_id']
  
   staff_scheduling_records.append({
       'provider_id': provider_id,
       'shift_date': shift_date,
       'shift_start_time': shift_start_time,
       'shift_end_time': shift_end_time,
       'department_id': department_id
   })
# Insert into StaffScheduling table


from datetime import datetime


staff_scheduling_records = []


for index, row in providers_df.iterrows():
   provider_id = int(row['provider_id'])
   department_id = row['department_id']


   # Skip if department_id is NaN
   if pd.isna(department_id):
       continue


   department_id = int(department_id)


   shift_date = fake.date_between(start_date='-30d', end_date='today')  # Last 30 days


   # Generate times and ensure proper format
   shift_start_time_str = fake.time()
   shift_end_time_str = fake.time()


   shift_start_time = datetime.strptime(shift_start_time_str, '%H:%M:%S').time()
   shift_end_time = datetime.strptime(shift_end_time_str, '%H:%M:%S').time()


   staff_scheduling_records.append({
       'provider_id': provider_id,
       'shift_date': shift_date,
       'shift_start_time': shift_start_time,
       'shift_end_time': shift_end_time,
       'department_id': department_id
   })


for record in staff_scheduling_records:
   try:
       cursor.execute(
           """
           INSERT INTO StaffScheduling (provider_id, shift_date, shift_start_time, shift_end_time, department_id)
           VALUES (%s, %s, %s, %s, %s)
           """,
           (
               record['provider_id'],
               record['shift_date'],
               record['shift_start_time'],
               record['shift_end_time'],
               record['department_id']
           )
       )
       conn.commit()
   except Exception as e:
       print(f"Error inserting StaffScheduling record: {record}")
       print(f"Exception: {e}")
       conn.rollback()
print("Staff Scheduling table populated successfully.")


#equipment table
equipment_items = []


for equipment_id in range(1, 11):  # Equipment IDs from 1 to 10
   equipment_name = f"Equipment {equipment_id}"
   equipment_type = fake.random_element(elements=('MRI Machine', 'CT Scanner', 'X-Ray Machine', 'Ultrasound', 'ECG Machine'))
   maintenance_schedule = fake.date_between(start_date='today', end_date='+30d')
   department_id = fake.random_int(min=1, max=10)  # Assuming department IDs from 1 to 10


   equipment_items.append({
       'equipment_id': equipment_id,
       'equipment_name': equipment_name,
       'equipment_type': equipment_type,
       'maintenance_schedule': maintenance_schedule,
       'department_id': department_id
   })




# Insert into Equipment table
for item in equipment_items:
   cursor.execute(
       """
       SELECT COUNT(*) FROM Equipment WHERE equipment_id = %s
       """,
       (item['equipment_id'],)
   )
   count = cursor.fetchone()[0]
  
   if count == 0:  # Only insert if the equipment_id doesn't already exist
       cursor.execute(
           """
           INSERT INTO Equipment (equipment_id, equipment_name, equipment_type, maintenance_schedule, department_id)
           VALUES (%s, %s, %s, %s, %s)
           """,
           (
               item['equipment_id'],
               item['equipment_name'],
               item['equipment_type'],
               item['maintenance_schedule'],
               item['department_id']
           )
       )
   else:
       print(f"Equipment ID {item['equipment_id']} already exists. Skipping insertion.")
conn.commit()
print("Equipment table populated successfully.")