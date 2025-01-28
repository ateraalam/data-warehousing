import psycopg2 
import pandas as pd
from faker import Faker
# Database connection parameters
db_params = {
    'host': 'localhost',
    'database': 'Project 1 - Healthcare Database',
    'user': 'coolusername',
    'password': 'coolerpassword'
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

#define a small set of codes 

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


# Load Kaggle dataset
kaggle_df = pd.read_csv('/Users/ateraalam/Desktop/DATA ANALYTICS ENGINEERING/IE 6750 /PROJECT_1/Datasets/hospital_readmissions.csv')

# Generate unique patient IDs)
file_path = '/Users/ateraalam/Desktop/DATA ANALYTICS ENGINEERING/IE 6750 /PROJECT_1/Datasets/hospital_readmissions.csv'
# Generate unique patient IDs
kaggle_df['patient_id'] = kaggle_df.index + 1

# Estimate date_of_birth based on age bracket
def estimate_dob(age_bracket):
    age_range = age_bracket.strip('[]').split('-')
    if len(age_range) == 2:
        age = int((int(age_range[0]) + int(age_range[1])) / 2)
    else:
        age = int(age_range[0].strip('+'))
    birth_year = 2020 - age  # Assuming current year is 2020
    return pd.to_datetime(f'{birth_year}-01-01')

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
