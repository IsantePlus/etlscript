#!/usr/bin/env python3
"""
Test Data Generator for iSantePlus Reports ETL Script

Generates synthetic test data for the isanteplusreportsdmlscript.sql ETL script,
covering patient demographics, dispensing, prescriptions, TB, nutrition, OB/GYN,
laboratory, vaccinations, pregnancy, and more.

The ETL reads from openmrs.* source tables and writes to isanteplus.* destination
tables. This generator creates OpenMRS source data so the ETL has data to process.

Usage:
    # Direct database insert:
    python generate_test_data_reports_dml.py --host localhost --user root --password secret --patients 100000

    # Generate SQL files:
    python generate_test_data_reports_dml.py --sql-output --patients 100000

    # Generate DDL (CREATE TABLE statements) only:
    python generate_test_data_reports_dml.py --ddl-output

    # Generate both DDL and test data:
    python generate_test_data_reports_dml.py --ddl-output --sql-output --patients 100000
"""

import argparse
import os
import random
import sys
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import IntEnum
from typing import Any, Dict, List, Optional, TextIO


def ensure_directory(path: str) -> None:
    """Create directory if it doesn't exist."""
    if path and path != '.':
        os.makedirs(path, exist_ok=True)

# Optional MySQL connector - only needed for direct database mode
try:
    import mysql.connector
    from mysql.connector import Error as MySQLError
    HAS_MYSQL = True
except ImportError:
    HAS_MYSQL = False
    MySQLError = Exception


# =============================================================================
# CONSTANTS - Concept IDs and UUIDs used by the ETL script
# =============================================================================

# All 16 encounter type UUIDs referenced in the ETL
ENCOUNTER_TYPES = {
    'first_visit':             '17536ba6-dd7c-4f58-8014-08c7cb798ac7',
    'followup':                '204ad066-c5c2-4229-9a62-644bc5617ca2',
    'pediatric_first':         '349ae0b4-65c1-4122-aa06-480f186c8350',
    'pediatric_followup':      '33491314-c352-42d0-bd5d-a9d0bffc9bf1',
    'lab':                     'f037e97b-471e-4898-a07c-b8e169e0ddc4',
    'discontinuation':         '9d0113c6-f23a-4461-8428-7e9a7344f2ba',
    'dispensing1':             '10d73929-54b6-4d18-a647-8b7316bc1ae3',
    'dispensing2':             'a9392241-109f-4d67-885b-57cc4b8c638f',
    'adult_initial':           '12f4d7c3-e047-4455-a607-47a40fe32460',
    'adult_followup':          'a5600919-4dde-4eb8-a45b-05c204af8284',
    'pediatric_initial':       '709610ff-5e39-4a47-9c27-a60e740b0944',
    'pediatric_consult':       'fdb5b14f-555f-4282-b4c1-9286addf0aae',
    'obgyn_initial':           '5c312603-25c1-4dbe-be18-1a167eb85f97',
    'obgyn_followup':          '49592bec-dd22-4b6c-a97f-4dd2af6f2171',
    'labor_delivery':          'd95b3540-a39f-4d1e-a301-8ee0e03d5eab',
    'imaging':                 'a4cab59f-f0ce-46c3-bd76-416db36ec719',
}

# Encounter types NOT processed by the ETL (noise data for filter testing)
NOISE_ENCOUNTER_TYPES = {
    'registration':       'aaaaaaaa-0000-0000-0000-000000000001',
    'vitals':             'aaaaaaaa-0000-0000-0000-000000000002',
    'general_consult':    'aaaaaaaa-0000-0000-0000-000000000003',
}

# Concept IDs that don't appear in the ETL's _tmp_obs WHERE clause
NOISE_CONCEPT_IDS = [999901, 999902, 999903, 999904, 999905]

# Concept UUIDs for group-level lookups (obs_group_id parent concepts)
CONCEPT_UUIDS = {
    # TB diagnosis groups
    'tb_diag_group':           '30d2b9eb-0a2f-4b0a-9ae9-31476ec13ed6',
    'mdr_tb_diag_group':       'b148cd09-496d-4a97-8cd5-75500f2d684f',

    # DDP (Dispensation à Domicile par les Patients)
    'ddp':                     'c2aacdc8-156e-4527-8934-a8fb94162419',

    # Virological test obs group concepts (PCR / viral load)
    'viro_group_1':            'eaa7f684-1473-4f59-acb4-686bada87846',
    'viro_group_2':            '9a05c0d5-2c03-4c3a-a810-6bc513ae7ee7',
    'viro_group_3':            '535b63e9-0773-4f4e-94af-69ff8f412411',

    # Serological test obs group concepts
    'sero_group_1':            '28e8ffc8-1b65-484c-baa1-929f0b8901a6',
    'sero_group_2':            '6e3aa01c-8a70-42b6-94fe-6ac465b620d9',
    'sero_group_3':            '2a66236f-d84b-4cc8-a552-15b12238e7ea',
    'sero_group_4':            '121d7ed6-c039-465d-9663-4ab631232ba9',
    'sero_group_5':            'ec6e3a54-3e4b-4647-b9bd-baf0d06a98d2',
    'sero_group_6':            '99f7b98e-8900-4898-9772-a88f4783babd',

    # Pregnancy obs group concepts (trigger concepts for patient_pregnancy)
    'preg_group_1':            '3fea18d4-88f1-40c1-aadc-41dca3449f9d',
    'preg_group_2':            '73da2a29-a035-41b5-8891-717ba99a3081',
    'preg_group_3':            '361bd482-59a9-4ee8-80f0-e7e39b1d1827',
    'preg_group_4':            'fd7987b1-d551-4451-b8e2-59a998adf1d5',
    'preg_group_5':            'ee6c7fd3-6a2f-4af0-8978-e1c5e06a9a62',
    'preg_group_6':            '6e639f6c-1b62-41c4-8cfd-fb76b3205313',
    'preg_group_7':            'f9d52515-6c56-41b3-881a-1b40f355144c',
    'preg_group_8':            '1dfb560d-6627-441e-a8e2-d1517b51c8b4',
    'preg_group_9':            '756f00e4-b1b6-40cd-b5ab-d5cce8a571fb',
    'preg_group_10':           '22be1344-65f9-4310-9be3-1d300e57820b',
    'preg_group_11':           'cb4d6c75-c218-4a26-9046-41e0939e55c4',

    # Key population / breast feeding / TB genexpert
    'key_population':          'b2726cc7-df4b-463c-919d-1c7a600fef87',
    'breast_feeding_start':    '7e0f24aa-4f8e-42d0-8649-282bc3c867e3',
    'tb_genexpert':            '4cbdc90a-e007-4a48-af54-5dd204edadd9',

    # ARV regimen line concepts (value_coded matched by UUID)
    'regimen_first_line':      'dd69cffe-d7b8-4cf1-bc11-3ac302763d48',
    'regimen_second_line':     '77488a7b-957f-4ebc-892a-e53e7c910363',
    'regimen_third_line':      '99d88c3e-00ad-4122-a300-a88ff5c125c9',

    # Posology alt concept
    'posology_alt':            'ca8bc9c3-7f97-450a-8f33-e98f776b90e1',

    # date_transferred_in
    'date_transferred_in':     'd9885523-a923-474b-88df-f3294d422c3c',
}

# Patient identifier type UUIDs (5 types used by demographics section)
IDENTIFIER_TYPE_UUIDS = {
    'st':         'd059f6d0-9e42-4760-8de1-8316b48bc5f1',
    'pc':         'b7a154fd-0097-4071-ac09-af11ee7e0310',
    'national':   '9fb4533d-4fd5-4276-875b-2ab41597f5dd',
    'isanteplus': '05a29f94-c0ed-11e2-94be-8c13b969e334',
    'isante':     '0e0c7cc2-3491-4675-b705-746e372ff346',
}

# Person attribute type UUIDs (birthplace, telephone, mother's name)
PERSON_ATTR_TYPE_UUIDS = {
    'birthplace':    '8d8718c2-c2cc-11de-8d13-0010c6dffd0f',
    'telephone':     '14d4f066-15f5-102d-96e4-000c29c2a5d7',
    'mothers_name':  '8d871d18-c2cc-11de-8d13-0010c6dffd0f',
}

# Location attribute type UUID (site code)
LOCATION_ATTR_TYPE_UUID = '0e52924e-4ebb-40ba-9b83-b198b532653b'


class ConceptID(IntEnum):
    """Concept IDs used in obs records throughout the ETL script."""

    # Demographics
    CIVIL_STATUS         = 1054
    OCCUPATION           = 1542
    CONTACT_NAME_GROUP   = 163258  # obs group for contact person info
    NEXT_VISIT           = 5096
    NEXT_VISIT_ALT       = 162549  # also used as next dispensation date
    TRANSFERRED_IN       = 159936  # value_coded = 1065 (yes)
    ARV_DATE_OTHER_SITE  = 159599  # value_datetime

    # HIV test
    HIV_TEST             = 1040
    HIV_CONFIRMED        = 1042
    POSITIVE             = 703
    NEGATIVE             = 664
    YES                  = 1065
    NO                   = 1066

    # Dispensing obs group children (parent group concept is 163711)
    DISPENSING_GROUP     = 163711
    DRUG_PRESCRIBED      = 1282
    DATE_DISPENSED       = 1276
    POSOLOGY             = 1444
    DOSE_DAY             = 159368
    PILLS_REMAINING      = 1443
    COMMUNITY_DISPENSING = 1755
    RX_OR_PROPHY         = 160742
    RX_TREATMENT         = 138405
    RX_PROPHYLAXIS       = 163768
    PRESCRIPTION_GROUP   = 1442   # alt parent group for prescription section

    # Vitals / nutrition
    WEIGHT               = 5089
    HEIGHT               = 5090
    MUAC                 = 1343
    EDEMA                = 460
    WEIGHT_FOR_HEIGHT    = 163515
    NUTRITIONAL_STATUS   = 5314

    # Adherence / family planning
    ADHERENCE            = 163710
    FAMILY_PLANNING      = 374

    # TB screening
    TB_SYMPTOM_SCREEN    = 160592  # question about TB symptoms
    COUGH                = 113489  # cough concept (value_coded answer)
    TB_TEST_DONE         = 160749
    TB_NEW_FOLLOWUP      = 1659    # new=1660, follow-up=1661
    SPUTUM_RESULT        = 307     # AFB/culture result (child of sputum group)
    TB_CLASSIFICATION    = 160040
    TB_PULMONARY         = 6042    # pulmonary TB diagnosis concept
    TB_EXTRAPULMONARY    = 6097    # extra-pulmonary TB diagnosis concept
    TB_MEDICATIONS       = 1111
    COTRIMOXAZOLE        = 105281
    COTRIMOXAZOLE_DRUG   = 1109

    # Laboratory
    TEST_ORDERED         = 1271
    CD4_COUNT            = 1941
    CD4_PERCENT          = 163544
    VIRAL_LOAD_NUMERIC   = 856
    VIRAL_LOAD_CODED     = 1305
    PCR_TEST             = 844
    TEST_RESULT_CODED    = 162087  # answer concept for viro/sero group children
    SUPPRESSED           = 1306
    AGE_UNITS_DAYS       = 1072
    AGE_UNITS_MONTHS     = 1074
    AGE_AT_TEST          = 163540  # value_numeric
    AGE_UNIT_CODED       = 163541  # value_coded (1072=days, 1074=months)

    # OB/GYN
    CURRENTLY_PREGNANT   = 160288
    PREGNANT_YES         = 1622
    EDD                  = 5596    # expected delivery date (value_datetime)
    HIGH_RISK_PREGNANCY  = 160079
    GESTATIONAL_AGE      = 1438
    FUNDAL_HEIGHT        = 1439
    FETAL_HEART_RATE     = 1440
    TETANUS_VACCINE      = 984
    TETANUS_TT2          = 84879
    DELIVERY_LOCATION    = 1572
    DELIVERY_DATE_OBS    = 5599
    VISIT_TYPE_OBS       = 160288  # same concept, value_coded differs for visit_type
    DDR                  = 1427    # date dernière règle (value_datetime)

    # Pregnancy trigger concepts (various coded answers)
    PREGNANCY_CONDITION  = 1284    # pregnancy conditions obs (value_coded from list)
    PREGNANCY_STATUS     = 1592    # pregnancy status
    B_HCG_TEST           = 1945
    PREGNANCY_TEST       = 45
    BIRTH_PLAN_CONCEPTS  = 163764  # group placeholder; actual are 163764-163766 etc

    # Pediatric HIV
    PTME                 = 163776
    PROPHYLAXIS_72H      = 5665
    ACTUAL_HIV_STATUS    = 1401

    # Menstruation
    MENSTRUATION_GROUP   = 163732
    MENSTRUATION_DATE    = 160597

    # Imaging
    IMAGING_RESULT       = 159975  # placeholder for imaging obs

    # Discontinuation
    DISCONTINUATION_REASON = 161555
    DISC_SUB_REASON      = 1667
    DECEASED             = 159
    TRANSFERRED          = 159492

    # VIH risk factors
    RISK_FACTOR          = 1061
    RISK_FACTOR_ALT      = 160581

    # Regimen line
    REGIMEN_LINE         = 164432

    # Breast feeding
    BREAST_FEEDING_OBS   = 5632

    # patient_on_art extras
    TB_DATE_INH          = 163284
    DATE_STARTED_ARV     = 160082
    TB_DATE_ENROLLED     = 1113

    # Malaria
    FEVER_SYMPTOM        = 159614   # also used for cough/fetal movement
    DIAGNOSIS            = 6042     # same as TB_PULMONARY above
    MALARIA_TREATMENT    = 1282     # same as DRUG_PRESCRIBED
    MALARIA_TEST_RESULT  = 1366
    MALARIA_SPECIES      = 1643

    # Serological test children
    SERO_RESULT          = 163722   # test_result for sero group
    SERO_AGE             = 163540   # same as AGE_AT_TEST
    SERO_AGE_UNIT        = 163541   # same as AGE_UNIT_CODED


# Vaccine concept IDs used in immunization obs groups.
# The ETL joins obs.value_coded to concept.concept_id to look up vaccine UUIDs,
# so these must use the real concept IDs from OpenMRS.
VACCINE_CONCEPTS = [
    (886,   'BCG'),
    (783,   'Polio'),
    (781,   'DTP'),
    (782,   'HepB'),
    (5261,  'HIB'),
    (1423,  'Pentavalent'),
    (83531, 'Pentavalent-alt'),
    (159701,'MMR'),
    (162586,'Measles/Rubella'),
]

# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class GeneratorConfig:
    """Configuration for the data generator."""
    num_patients: int = 100000
    start_date: datetime = None
    end_date: datetime = None
    seed: Optional[int] = None
    batch_size: int = 10000

    # Distribution percentages
    pct_hiv_positive: float  = 0.60
    pct_on_arv: float        = 0.55
    pct_discontinued: float  = 0.08
    pct_pregnant: float      = 0.08
    pct_pediatric: float     = 0.15
    pct_with_tb: float       = 0.06
    pct_with_lab: float      = 0.40
    pct_with_viral_load: float = 0.50
    pct_with_nutrition: float  = 0.30
    pct_with_obgyn: float    = 0.15
    pct_noise: float         = 0.05   # fraction of patients that are pure noise
    pct_voided_visit: float  = 0.10   # fraction of normal patients that get a voided visit

    def __post_init__(self):
        if self.start_date is None:
            self.start_date = datetime.now() - timedelta(days=5 * 365)
        if self.end_date is None:
            self.end_date = datetime.now()


def _generate_uuid() -> str:
    return str(uuid.uuid4())


@dataclass
class Patient:
    """Represents a patient with all associated data."""
    patient_id: int
    birthdate: datetime
    gender: str
    is_hiv_positive: bool
    is_on_arv: bool
    is_discontinued: bool
    discontinuation_reason: Optional[int]
    is_pregnant: bool
    is_pediatric: bool
    date_started_arv: Optional[datetime]
    location_id: int
    has_tb: bool = False
    has_lab: bool = False
    has_nutrition: bool = False
    has_obgyn: bool = False
    noise: bool = False
    visits: List['Visit'] = field(default_factory=list)
    first_visit_date: Optional[datetime] = None
    person_uuid: str = field(default_factory=_generate_uuid)
    given_name: str = ''
    family_name: str = ''

    @property
    def person_id(self) -> int:
        return self.patient_id


@dataclass
class Visit:
    """Represents a visit with encounters."""
    visit_id: int
    patient_id: int
    date_started: datetime
    location_id: int
    date_stopped: Optional[datetime] = None
    voided: int = 0
    encounters: List['Encounter'] = field(default_factory=list)
    uuid: str = field(default_factory=_generate_uuid)


@dataclass
class Encounter:
    """Represents an encounter with observations."""
    encounter_id: int
    visit_id: int
    patient_id: int
    encounter_type_id: int
    encounter_datetime: datetime
    location_id: int
    form_id: Optional[int] = None
    voided: int = 0
    observations: List['Observation'] = field(default_factory=list)
    uuid: str = field(default_factory=_generate_uuid)


@dataclass
class Observation:
    """Represents a clinical observation."""
    obs_id: int
    person_id: int
    encounter_id: int
    concept_id: int
    value_coded: Optional[int] = None
    value_numeric: Optional[float] = None
    value_datetime: Optional[datetime] = None
    value_text: Optional[str] = None
    obs_datetime: datetime = None
    obs_group_id: Optional[int] = None
    location_id: int = 1
    voided: int = 0
    uuid: str = field(default_factory=_generate_uuid)


# =============================================================================
# ID GENERATOR
# =============================================================================

class IDGenerator:
    """Generates sequential IDs for various entity types."""

    def __init__(self, start_id: int = 1):
        self._counters: Dict[str, int] = {}
        self._start_id = start_id

    def next(self, entity_type: str) -> int:
        if entity_type not in self._counters:
            self._counters[entity_type] = self._start_id
        current = self._counters[entity_type]
        self._counters[entity_type] += 1
        return current

    def current(self, entity_type: str) -> int:
        return self._counters.get(entity_type, self._start_id) - 1


# =============================================================================
# DATA GENERATOR
# =============================================================================

class TestDataGenerator:
    """Generates test data for the isanteplusreportsdmlscript ETL script."""

    def __init__(self, config: GeneratorConfig):
        self.config = config
        self.id_gen = IDGenerator()
        self.encounter_type_ids: Dict[str, int] = {}
        self.noise_encounter_type_ids: Dict[str, int] = {}
        self.encounter_type_names: Dict[int, str] = {}
        self.concept_ids: Dict[str, int] = {}   # name -> concept_id for UUID concepts
        self.concept_uuids: Dict[int, str] = {}  # concept_id -> uuid

        if config.seed is None:
            self.seed = random.randint(0, 2**32 - 1)
        else:
            self.seed = config.seed
        random.seed(self.seed)

        # OpenMRS source tables
        self.patients: List[Patient] = []
        self.visits: List[Visit] = []
        self.encounters: List[Encounter] = []
        self.encounter_providers: List[Dict] = []
        self.observations: List[Observation] = []
        self.person_names: List[Dict] = []
        self.person_addresses: List[Dict] = []
        self.patient_identifiers: List[Dict] = []
        self.person_attributes: List[Dict] = []

        # Reference / lookup tables generated once
        self.encounter_types: List[Dict] = []
        self.concepts: List[Dict] = []
        self.concept_names: List[Dict] = []
        self.identifier_types: List[Dict] = []
        self.person_attr_types: List[Dict] = []
        self.locations: List[Dict] = []
        self.location_attributes: List[Dict] = []
        self.location_attr_types: List[Dict] = []

        # iSantePlus source tables (pre-populated by earlier ETL runs; used as
        # input by the alert and patient_on_art sections)
        self.isanteplus_patients: List[Dict] = []
        self.patient_on_arv: List[int] = []       # patient_ids on ARV
        self.patient_dispensing: List[Dict] = []
        self.patient_prescription: List[Dict] = []
        self.patient_laboratory: List[Dict] = []
        self.discontinuation_reasons: List[Dict] = []
        self.patient_pregnancy_records: List[Dict] = []

    # -------------------------------------------------------------------------
    # Utility methods
    # -------------------------------------------------------------------------

    def _random_date(self, start: datetime, end: datetime) -> datetime:
        delta = end - start
        return start + timedelta(
            days=random.randint(0, max(0, delta.days)),
            seconds=random.randint(0, 86400)
        )

    def _random_date_after(self, after: datetime, max_days: int = 90) -> datetime:
        return after + timedelta(days=random.randint(1, max_days))

    def _random_name(self) -> tuple[str, str]:
        first = ['Jean', 'Marie', 'Pierre', 'Claire', 'Joseph', 'Rose',
                 'Michel', 'Josette', 'Frantz', 'Nadège', 'Robert', 'Claudette']
        last = ['Duval', 'Larose', 'Toussaint', 'Pierre', 'Jean', 'Baptiste',
                'Moreau', 'Simon', 'François', 'Léger', 'Henry', 'Celestin']
        return random.choice(first), random.choice(last)

    # -------------------------------------------------------------------------
    # Reference data setup
    # -------------------------------------------------------------------------

    def _setup_encounter_types(self) -> List[Dict]:
        enc_types = []
        for name, uid in ENCOUNTER_TYPES.items():
            type_id = self.id_gen.next('encounter_type')
            self.encounter_type_ids[name] = type_id
            self.encounter_type_names[type_id] = name
            enc_types.append({
                'encounter_type_id': type_id,
                'name': name.replace('_', ' ').title(),
                'uuid': uid,
                'creator': 1,
                'date_created': self.config.start_date,
            })
        for name, uid in NOISE_ENCOUNTER_TYPES.items():
            type_id = self.id_gen.next('encounter_type')
            self.noise_encounter_type_ids[name] = type_id
            self.encounter_type_names[type_id] = name
            enc_types.append({
                'encounter_type_id': type_id,
                'name': name.replace('_', ' ').title(),
                'uuid': uid,
                'creator': 1,
                'date_created': self.config.start_date,
            })
        return enc_types

    def _setup_concepts(self) -> List[Dict]:
        """Create concept rows for UUID-based lookups.

        UUID-lookup concepts get auto-generated IDs mapped by name. Vaccine
        concepts use their canonical IDs because the ETL joins obs.value_coded
        to concept.concept_id when fetching vaccine UUIDs.
        """
        concepts = []
        for name, uid in CONCEPT_UUIDS.items():
            concept_id = self.id_gen.next('concept')
            self.concept_ids[name] = concept_id
            self.concept_uuids[concept_id] = uid
            concepts.append({'concept_id': concept_id, 'uuid': uid})

        for concept_id, _name in VACCINE_CONCEPTS:
            concepts.append({'concept_id': concept_id, 'uuid': _generate_uuid()})

        return concepts

    def _setup_concept_names(self) -> List[Dict]:
        """Create concept_name rows with locale='fr'.

        The patient_laboratory section joins concept_name on concept_id with
        locale='fr' to get French-language test names.
        """
        names = []
        lab_concepts = [
            (ConceptID.CD4_COUNT,         'CD4'),
            (ConceptID.CD4_PERCENT,       'CD4 %'),
            (ConceptID.VIRAL_LOAD_NUMERIC, 'Charge virale'),
            (ConceptID.VIRAL_LOAD_CODED,   'Charge virale codée'),
            (ConceptID.PCR_TEST,           'PCR'),
            (ConceptID.TEST_RESULT_CODED,  'Résultat du test'),
        ]
        for concept_id, name_fr in lab_concepts:
            names.append({
                'concept_name_id': self.id_gen.next('concept_name'),
                'concept_id': concept_id,
                'locale': 'fr',
                'name': name_fr,
                'concept_name_type': 'FULLY_SPECIFIED',
                'uuid': _generate_uuid(),
            })
        return names

    def _setup_identifier_types(self) -> List[Dict]:
        types = []
        for name, uid in IDENTIFIER_TYPE_UUIDS.items():
            type_id = self.id_gen.next('identifier_type')
            self.concept_ids[f'id_type_{name}'] = type_id
            types.append({
                'patient_identifier_type_id': type_id,
                'name': name.upper(),
                'uuid': uid,
                'creator': 1,
                'date_created': self.config.start_date,
                'required': 0,
            })
        return types

    def _setup_person_attr_types(self) -> List[Dict]:
        types = []
        for name, uid in PERSON_ATTR_TYPE_UUIDS.items():
            type_id = self.id_gen.next('person_attr_type')
            self.concept_ids[f'attr_type_{name}'] = type_id
            types.append({
                'person_attribute_type_id': type_id,
                'name': name,
                'uuid': uid,
                'creator': 1,
                'date_created': self.config.start_date,
            })
        return types

    def _setup_locations(self, num_locations: int = 5) -> List[Dict]:
        locations = []
        for i in range(1, num_locations + 1):
            locations.append({
                'location_id': i,
                'name': f'Clinic {i}',
                'uuid': _generate_uuid(),
                'creator': 1,
                'date_created': self.config.start_date,
                'retired': 0,
            })
        return locations

    def _setup_location_attr_types(self) -> List[Dict]:
        type_id = self.id_gen.next('location_attr_type')
        self.concept_ids['location_attr_type_site_code'] = type_id
        return [{
            'location_attribute_type_id': type_id,
            'name': 'site_code',
            'uuid': LOCATION_ATTR_TYPE_UUID,
            'creator': 1,
            'date_created': self.config.start_date,
        }]

    def _setup_location_attributes(self, locations: List[Dict]) -> List[Dict]:
        attr_type_id = self.concept_ids.get('location_attr_type_site_code', 1)
        attrs = []
        for loc in locations:
            attrs.append({
                'location_attribute_id': self.id_gen.next('location_attribute'),
                'location_id': loc['location_id'],
                'attribute_type_id': attr_type_id,
                'value_reference': f'SITE{loc["location_id"]:03d}',
                'uuid': _generate_uuid(),
                'creator': 1,
                'date_created': self.config.start_date,
            })
        return attrs

    # -------------------------------------------------------------------------
    # Patient generation
    # -------------------------------------------------------------------------

    def _generate_patient(self) -> Patient:
        is_pediatric = random.random() < self.config.pct_pediatric
        if is_pediatric:
            birthdate = datetime.now() - timedelta(days=random.randint(0, 18 * 365))
        else:
            birthdate = datetime.now() - timedelta(days=random.randint(18 * 365, 80 * 365))

        gender = random.choice(['M', 'F'])
        is_hiv_positive = random.random() < self.config.pct_hiv_positive
        is_on_arv = is_hiv_positive and random.random() < self.config.pct_on_arv
        is_discontinued = is_hiv_positive and random.random() < self.config.pct_discontinued
        disc_reason = None
        if is_discontinued:
            disc_reason = random.choice([ConceptID.DECEASED, ConceptID.TRANSFERRED,
                                         ConceptID.DISC_SUB_REASON])
        is_pregnant = (gender == 'F' and not is_pediatric
                       and random.random() < self.config.pct_pregnant)
        date_started_arv = None
        if is_on_arv:
            months_back = random.randint(1, 60)
            date_started_arv = datetime.now() - timedelta(days=months_back * 30)

        given, family = self._random_name()
        return Patient(
            patient_id=self.id_gen.next('patient'),
            birthdate=birthdate,
            gender=gender,
            is_hiv_positive=is_hiv_positive,
            is_on_arv=is_on_arv,
            is_discontinued=is_discontinued,
            discontinuation_reason=disc_reason,
            is_pregnant=is_pregnant,
            is_pediatric=is_pediatric,
            date_started_arv=date_started_arv,
            location_id=random.randint(1, 5),
            has_tb=is_hiv_positive and random.random() < self.config.pct_with_tb,
            has_lab=random.random() < self.config.pct_with_lab,
            has_nutrition=random.random() < self.config.pct_with_nutrition,
            has_obgyn=(gender == 'F' and not is_pediatric
                       and random.random() < self.config.pct_with_obgyn),
            given_name=given,
            family_name=family,
        )

    def _generate_noise_patient(self) -> Patient:
        """Create a patient whose encounters/obs use only noise types and concepts."""
        patient = self._generate_patient()
        patient.noise = True
        return patient

    def _noise_encounter_types_for_visit(self) -> List[str]:
        """Return 1-2 random noise encounter type names."""
        names = list(NOISE_ENCOUNTER_TYPES.keys())
        return random.sample(names, min(random.randint(1, 2), len(names)))

    def _generate_noise_observations(
        self, encounter: Encounter, patient: Patient
    ) -> None:
        """Generate observations using only noise concept IDs."""
        num_obs = random.randint(2, 5)
        for _ in range(num_obs):
            concept_id = random.choice(NOISE_CONCEPT_IDS)
            # Randomly choose between value_numeric and value_coded
            if random.random() < 0.5:
                self._make_obs(patient, encounter, concept_id,
                               value_numeric=round(random.uniform(1, 500), 1))
            else:
                self._make_obs(patient, encounter, concept_id,
                               value_coded=random.randint(1, 9999))

    def _generate_visits_for_patient(self, patient: Patient) -> List[Visit]:
        if patient.date_started_arv:
            months = (datetime.now() - patient.date_started_arv).days // 30
            num_visits = min(months + 1, random.randint(2, 24))
        else:
            num_visits = random.randint(1, 6)

        visit_date = (
            patient.first_visit_date or
            patient.date_started_arv or
            self._random_date(
                self.config.start_date,
                self.config.end_date - timedelta(days=30))
        )

        visits = []
        for _ in range(num_visits):
            # ~80% of visits are completed (have a date_stopped)
            if random.random() < 0.8:
                date_stopped = visit_date + timedelta(
                    hours=random.randint(1, 8))
            else:
                date_stopped = None
            visit = Visit(
                visit_id=self.id_gen.next('visit'),
                patient_id=patient.patient_id,
                date_started=visit_date,
                location_id=patient.location_id,
                date_stopped=date_stopped,
            )
            visits.append(visit)
            visit_date = self._random_date_after(visit_date, max_days=90)
            if visit_date > self.config.end_date:
                break
        return visits

    def _encounter_types_for_visit(
        self, patient: Patient, is_first: bool
    ) -> List[str]:
        """Determine which encounter types to create for a visit."""
        if patient.noise:
            return self._noise_encounter_types_for_visit()

        types = []

        # Primary HIV encounter
        if patient.is_hiv_positive or patient.is_pediatric:
            if patient.is_pediatric:
                types.append('pediatric_first' if is_first else 'pediatric_followup')
            else:
                types.append('first_visit' if is_first else 'followup')

        # Dispensing
        if patient.is_on_arv:
            types.append(random.choice(['dispensing1', 'dispensing2']))

        # Lab
        if patient.has_lab and random.random() < 0.4:
            types.append('lab')

        # TB / nutrition / OB/GYN → primary care encounter types
        if patient.has_tb or patient.has_nutrition:
            if patient.is_pediatric:
                types.append('pediatric_initial' if is_first else 'pediatric_consult')
            else:
                types.append('adult_initial' if is_first else 'adult_followup')

        # OB/GYN
        if patient.has_obgyn or patient.is_pregnant:
            types.append('obgyn_initial' if is_first else 'obgyn_followup')

        # Labor/delivery for some pregnant patients
        if patient.is_pregnant and not is_first and random.random() < 0.15:
            types.append('labor_delivery')

        # Discontinuation
        if patient.is_discontinued and random.random() < 0.5:
            types.append('discontinuation')

        # Imaging occasionally
        if random.random() < 0.05:
            types.append('imaging')

        return list(dict.fromkeys(types))  # deduplicate preserving order

    def _make_obs(
        self,
        patient: Patient,
        encounter: Encounter,
        concept_id: int,
        value_coded: int = None,
        value_numeric: float = None,
        value_datetime: datetime = None,
        value_text: str = None,
        obs_group_id: int = None,
    ) -> Observation:
        obs = Observation(
            obs_id=self.id_gen.next('obs'),
            person_id=patient.patient_id,
            encounter_id=encounter.encounter_id,
            concept_id=concept_id,
            value_coded=value_coded,
            value_numeric=value_numeric,
            value_datetime=value_datetime,
            value_text=value_text,
            obs_datetime=encounter.encounter_datetime,
            obs_group_id=obs_group_id,
            location_id=encounter.location_id,
        )
        self.observations.append(obs)
        return obs

    def _generate_observations_for_encounter(
        self, encounter: Encounter, patient: Patient, enc_type_name: str
    ) -> None:
        """Generate observations for a single encounter, appending to self.observations."""
        add = lambda cid, **kw: self._make_obs(patient, encounter, cid, **kw)

        dt = encounter.encounter_datetime

        # -----------------------------------------------------------------
        # Demographics obs (first_visit / pediatric_first)
        # -----------------------------------------------------------------
        if enc_type_name in ('first_visit', 'pediatric_first'):
            # Civil status, occupation
            add(ConceptID.CIVIL_STATUS,
                value_coded=random.choice([1057, 5555, 1058, 1060]))
            add(ConceptID.OCCUPATION,
                value_coded=random.choice([1540, 1539, 1538]))

            # HIV test
            hiv_result = ConceptID.POSITIVE if patient.is_hiv_positive else ConceptID.NEGATIVE
            add(ConceptID.HIV_TEST, value_coded=hiv_result)

            # Transferred-in patient
            if random.random() < 0.1:
                add(ConceptID.TRANSFERRED_IN, value_coded=ConceptID.YES)
                add(ConceptID.ARV_DATE_OTHER_SITE,
                    value_datetime=dt - timedelta(days=random.randint(30, 365)))

        # -----------------------------------------------------------------
        # Next visit date (all clinical encounter types)
        # -----------------------------------------------------------------
        if enc_type_name in ('first_visit', 'followup', 'pediatric_first',
                             'pediatric_followup', 'obgyn_initial', 'obgyn_followup'):
            next_visit = dt + timedelta(days=random.choice([30, 60, 90]))
            add(ConceptID.NEXT_VISIT, value_datetime=next_visit)

        # -----------------------------------------------------------------
        # Dispensing obs group (dispensing1, dispensing2)
        # Used by patient_dispensing and patient_prescription ETL sections.
        # -----------------------------------------------------------------
        if enc_type_name in ('dispensing1', 'dispensing2'):
            num_drugs = random.randint(1, 3)
            drug_ids = random.sample([84795, 78643, 75523, 80586, 70056, 84309,
                                      75628, 794, 165085], num_drugs)
            for drug_id in drug_ids:
                # Parent obs group
                parent = add(ConceptID.DISPENSING_GROUP)
                add(ConceptID.DRUG_PRESCRIBED, value_coded=drug_id,
                    obs_group_id=parent.obs_id)
                add(ConceptID.DATE_DISPENSED, value_datetime=dt,
                    obs_group_id=parent.obs_id)
                add(ConceptID.POSOLOGY, value_numeric=random.choice([1, 2]),
                    obs_group_id=parent.obs_id)
                add(ConceptID.DOSE_DAY, value_numeric=random.choice([30, 60, 90]),
                    obs_group_id=parent.obs_id)
                add(ConceptID.PILLS_REMAINING, value_numeric=random.randint(0, 10),
                    obs_group_id=parent.obs_id)

            # Next dispensation date
            next_disp = dt + timedelta(days=random.choice([30, 60, 90]))
            add(ConceptID.NEXT_VISIT_ALT, value_datetime=next_disp)

            # RX or prophylaxis type
            add(ConceptID.RX_OR_PROPHY,
                value_coded=random.choice([ConceptID.RX_TREATMENT,
                                           ConceptID.RX_PROPHYLAXIS]))

            # DDP (community dispensing) occasionally
            if random.random() < 0.05:
                ddp_id = self.concept_ids.get('ddp', 99999)
                add(ddp_id, value_coded=ConceptID.YES)

        # -----------------------------------------------------------------
        # Vitals / nutrition (adult_initial, adult_followup,
        #                      pediatric_initial, pediatric_consult)
        # -----------------------------------------------------------------
        if enc_type_name in ('adult_initial', 'adult_followup',
                             'pediatric_initial', 'pediatric_consult'):
            add(ConceptID.WEIGHT, value_numeric=round(random.uniform(10, 100), 1))
            add(ConceptID.HEIGHT, value_numeric=round(random.uniform(50, 200), 1))

            if patient.has_nutrition or random.random() < 0.3:
                add(ConceptID.MUAC, value_numeric=round(random.uniform(10, 35), 1))
                add(ConceptID.NUTRITIONAL_STATUS,
                    value_coded=random.choice([1115, 1116, 163303]))

            if patient.is_pediatric:
                add(ConceptID.WEIGHT_FOR_HEIGHT,
                    value_numeric=round(random.uniform(-3, 3), 2))
                add(ConceptID.EDEMA, value_coded=random.choice([
                    ConceptID.YES, ConceptID.NO]))

            add(ConceptID.ADHERENCE,
                value_coded=random.choice([159405, 159406, 159407]))
            add(ConceptID.FAMILY_PLANNING,
                value_coded=random.choice([5275, 1107, 160570]))

            # TB screening
            if patient.has_tb or random.random() < 0.2:
                add(ConceptID.TB_SYMPTOM_SCREEN,
                    value_coded=random.choice([ConceptID.YES, ConceptID.NO]))
                add(ConceptID.TB_NEW_FOLLOWUP,
                    value_coded=random.choice([1660, 1661]))

        # -----------------------------------------------------------------
        # TB diagnosis (adult_initial, adult_followup)
        # -----------------------------------------------------------------
        if patient.has_tb and enc_type_name in ('adult_initial', 'adult_followup',
                                                 'first_visit', 'followup'):
            tb_group_id = self.concept_ids.get('tb_diag_group', 88888)
            parent = add(tb_group_id)
            add(ConceptID.TB_PULMONARY, value_coded=random.choice([6042, 6097]),
                obs_group_id=parent.obs_id)
            add(ConceptID.TB_CLASSIFICATION, value_coded=random.choice([42, 5622]),
                obs_group_id=parent.obs_id)

            # Sputum result
            add(ConceptID.SPUTUM_RESULT,
                value_coded=random.choice([703, 664, 1138]))

            # Cotrimoxazole
            if random.random() < 0.5:
                add(ConceptID.COTRIMOXAZOLE_DRUG,
                    value_coded=ConceptID.COTRIMOXAZOLE)

        # -----------------------------------------------------------------
        # Lab encounter observations
        # -----------------------------------------------------------------
        if enc_type_name == 'lab':
            # Test orders
            add(ConceptID.TEST_ORDERED, value_coded=random.choice([856, 1941, 844]))

            # CD4
            if random.random() < 0.6:
                add(ConceptID.CD4_COUNT,
                    value_numeric=random.randint(50, 1500))
                add(ConceptID.CD4_PERCENT,
                    value_numeric=round(random.uniform(1, 45), 1))

            # Viral load
            if patient.is_on_arv and random.random() < self.config.pct_with_viral_load:
                vl = random.randint(20, 100000)
                add(ConceptID.VIRAL_LOAD_NUMERIC, value_numeric=float(vl))
                add(ConceptID.VIRAL_LOAD_CODED,
                    value_coded=ConceptID.SUPPRESSED if vl < 1000 else 1301)

            # Virological test obs group (for virological_tests ETL section)
            if random.random() < 0.3:
                viro_group_name = random.choice(['viro_group_1', 'viro_group_2',
                                                 'viro_group_3'])
                viro_group_concept = self.concept_ids.get(viro_group_name, 77777)
                parent = add(viro_group_concept)
                add(ConceptID.TEST_RESULT_CODED, value_coded=1030,
                    obs_group_id=parent.obs_id)  # 1030=PCR test
                # Test result
                add(1030, value_coded=random.choice([703, 664, 1138]),
                    obs_group_id=parent.obs_id)
                # Age at test
                add(ConceptID.AGE_AT_TEST,
                    value_numeric=random.randint(1, 36),
                    obs_group_id=parent.obs_id)
                add(ConceptID.AGE_UNIT_CODED,
                    value_coded=random.choice([ConceptID.AGE_UNITS_DAYS,
                                              ConceptID.AGE_UNITS_MONTHS]),
                    obs_group_id=parent.obs_id)

            # Serological test obs group (for serological_tests ETL section)
            if random.random() < 0.3:
                sero_group_name = random.choice([f'sero_group_{i}' for i in range(1, 7)])
                sero_concept = self.concept_ids.get(sero_group_name, 66666)
                parent = add(sero_concept)
                add(ConceptID.TEST_RESULT_CODED, value_coded=ConceptID.SERO_RESULT,
                    obs_group_id=parent.obs_id)
                add(ConceptID.SERO_RESULT,
                    value_coded=random.choice([163722, 1042]),
                    obs_group_id=parent.obs_id)
                add(ConceptID.SERO_AGE,
                    value_numeric=random.randint(1, 60),
                    obs_group_id=parent.obs_id)
                add(ConceptID.SERO_AGE_UNIT,
                    value_coded=random.choice([ConceptID.AGE_UNITS_DAYS,
                                              ConceptID.AGE_UNITS_MONTHS]),
                    obs_group_id=parent.obs_id)

        # -----------------------------------------------------------------
        # OB/GYN observations
        # -----------------------------------------------------------------
        if enc_type_name in ('obgyn_initial', 'obgyn_followup'):
            add(ConceptID.MUAC, value_numeric=round(random.uniform(20, 35), 1))

            if patient.is_pregnant or random.random() < 0.7:
                add(ConceptID.CURRENTLY_PREGNANT,
                    value_coded=ConceptID.PREGNANT_YES)
                # EDD
                edd = dt + timedelta(days=random.randint(30, 270))
                add(ConceptID.EDD, value_datetime=edd)

            add(ConceptID.GESTATIONAL_AGE,
                value_numeric=random.randint(4, 40))
            add(ConceptID.FUNDAL_HEIGHT,
                value_numeric=round(random.uniform(10, 40), 1))
            add(ConceptID.FETAL_HEART_RATE,
                value_numeric=random.randint(120, 170))

            # Tetanus vaccine
            if random.random() < 0.6:
                add(ConceptID.TETANUS_VACCINE, value_coded=ConceptID.YES)

            # Visit type (for visit_type ETL section)
            add(ConceptID.VISIT_TYPE_OBS,
                value_coded=random.choice([160456, 1622, 1623, 5483]))

            # DDR
            ddr = dt - timedelta(days=random.randint(0, 60))
            add(ConceptID.DDR, value_datetime=ddr)

            # Menstruation obs
            add(ConceptID.MENSTRUATION_GROUP, value_coded=163732)

        # -----------------------------------------------------------------
        # Labor/delivery observations
        # -----------------------------------------------------------------
        if enc_type_name == 'labor_delivery':
            add(ConceptID.DELIVERY_LOCATION,
                value_coded=random.choice([163266, 1501, 1502, 5622]))
            add(ConceptID.DELIVERY_DATE_OBS, value_datetime=dt)

        # -----------------------------------------------------------------
        # Discontinuation observations
        # -----------------------------------------------------------------
        if enc_type_name == 'discontinuation' and patient.is_discontinued:
            add(ConceptID.DISCONTINUATION_REASON,
                value_coded=patient.discontinuation_reason)
            if patient.discontinuation_reason == ConceptID.DISC_SUB_REASON:
                add(ConceptID.DISC_SUB_REASON,
                    value_coded=random.choice([115198, 159737]))

        # -----------------------------------------------------------------
        # Pediatric HIV visit observations
        # -----------------------------------------------------------------
        if enc_type_name in ('pediatric_first', 'pediatric_followup'):
            add(ConceptID.ACTUAL_HIV_STATUS, value_coded=random.choice([703, 664]))
            if random.random() < 0.4:
                add(ConceptID.PTME, value_coded=ConceptID.YES)
            if random.random() < 0.3:
                add(ConceptID.PROPHYLAXIS_72H, value_coded=ConceptID.YES)

            # Immunization obs groups (for vaccination ETL section)
            if patient.is_pediatric and random.random() < 0.6:
                vaccines = random.sample(VACCINE_CONCEPTS,
                                         min(3, len(VACCINE_CONCEPTS)))
                for vaccine_id, _ in vaccines:
                    for dose in range(1, random.randint(2, 4)):
                        parent = add(1421)  # immunization history construct
                        add(984, value_coded=vaccine_id,
                            obs_group_id=parent.obs_id)
                        add(1418, value_numeric=dose,
                            obs_group_id=parent.obs_id)
                        add(1410, value_datetime=dt - timedelta(
                                days=random.randint(0, 30)),
                            obs_group_id=parent.obs_id)

        # -----------------------------------------------------------------
        # VIH risk factors (first_visit, pediatric_first)
        # -----------------------------------------------------------------
        if enc_type_name in ('first_visit', 'pediatric_first'):
            if random.random() < 0.2:
                add(ConceptID.RISK_FACTOR,
                    value_coded=random.choice(
                        [163290, 163291, 105, 1063, 163273, 163289]))

        # -----------------------------------------------------------------
        # Imaging observations
        # -----------------------------------------------------------------
        if enc_type_name == 'imaging':
            add(ConceptID.IMAGING_RESULT, value_coded=random.choice([703, 664]))

        # -----------------------------------------------------------------
        # Patient_on_art related obs (breast feeding, key population,
        # regimen line, TB genexpert, INH date)
        # -----------------------------------------------------------------
        if enc_type_name in ('first_visit', 'followup'):
            # Key population
            kp_concept = self.concept_ids.get('key_population', 55555)
            if random.random() < 0.05:
                add(kp_concept,
                    value_coded=random.choice([160578, 160579, 162277, 124275, 105]))

            # Breast feeding
            bf_concept = self.concept_ids.get('breast_feeding_start', 55556)
            if patient.gender == 'F' and random.random() < 0.1:
                add(bf_concept, value_coded=ConceptID.YES)
            add(ConceptID.BREAST_FEEDING_OBS,
                value_coded=random.choice([ConceptID.YES, ConceptID.NO]))

            # Regimen line
            if patient.is_on_arv and random.random() < 0.5:
                line_concept_id = random.choice([
                    self.concept_ids.get('regimen_first_line', 44441),
                    self.concept_ids.get('regimen_second_line', 44442),
                    self.concept_ids.get('regimen_third_line', 44443),
                ])
                add(ConceptID.REGIMEN_LINE, value_coded=line_concept_id)

            # TB genexpert
            if patient.has_tb and random.random() < 0.3:
                gx_concept = self.concept_ids.get('tb_genexpert', 55557)
                add(gx_concept, value_coded=random.choice([1301, 664]))

        # -----------------------------------------------------------------
        # Pregnancy trigger obs (triggers patient_pregnancy ETL inserts)
        # -----------------------------------------------------------------
        if patient.is_pregnant and enc_type_name in ('obgyn_initial', 'obgyn_followup',
                                                      'first_visit', 'followup'):
            # obs_group parent concept for pregnancy condition
            preg_group_name = random.choice(
                [f'preg_group_{i}' for i in range(1, 12)])
            preg_group_concept = self.concept_ids.get(preg_group_name, 33333)
            parent = add(preg_group_concept)
            add(ConceptID.PREGNANCY_CONDITION,
                value_coded=random.choice(
                    [46, 129251, 132678, 47, 163751, 1449, 118245, 141631]),
                obs_group_id=parent.obs_id)

            # Also "Femme enceinte" coded obs (second trigger pattern)
            if random.random() < 0.5:
                add(162225, value_coded=1434)

        # -----------------------------------------------------------------
        # Malaria obs (adult encounters)
        # -----------------------------------------------------------------
        if enc_type_name in ('adult_initial', 'adult_followup') and \
                random.random() < 0.1:
            add(ConceptID.FEVER_SYMPTOM, value_coded=163740)
            add(ConceptID.TB_PULMONARY, value_coded=random.choice([116128, 160148]))
            add(ConceptID.TEST_ORDERED, value_coded=1366)
            add(ConceptID.MALARIA_TEST_RESULT,
                value_coded=random.choice([664, 1365, 1364]))

    # -------------------------------------------------------------------------
    # iSantePlus pre-populated source table records
    # -------------------------------------------------------------------------

    def _generate_isanteplus_data(self, patient: Patient) -> None:
        """Generate iSantePlus source records used by alert / patient_on_art sections."""
        # isanteplus.patient row (populated by demographics ETL section)
        self.isanteplus_patients.append({
            'patient_id': patient.patient_id,
            'location_id': patient.location_id,
            'vih_status': 1 if patient.is_hiv_positive else 0,
            'date_started_arv': patient.date_started_arv,
            'voided': 0,
        })

        if not patient.visits:
            return

        # patient_on_arv
        if patient.is_on_arv:
            self.patient_on_arv.append(patient.patient_id)

        # discontinuation_reason
        if patient.is_discontinued and patient.discontinuation_reason:
            self.discontinuation_reasons.append({
                'patient_id': patient.patient_id,
                'reason': patient.discontinuation_reason,
                'visit_date': patient.visits[-1].date_started,
                'visit_id': patient.visits[-1].visit_id,
            })

        # patient_dispensing (needed by alert section)
        if patient.is_on_arv:
            for visit in patient.visits:
                next_date = visit.date_started + timedelta(
                    days=random.choice([30, 60, 90]))
                enc_id = (visit.encounters[0].encounter_id
                          if visit.encounters else
                          self.id_gen.next('disp_enc'))
                self.patient_dispensing.append({
                    'patient_id': patient.patient_id,
                    'encounter_id': enc_id,
                    'visit_id': visit.visit_id,
                    'visit_date': visit.date_started,
                    'next_dispensation_date': next_date,
                    'arv_drug': ConceptID.YES,  # 1065
                    'rx_or_prophy': random.choice([ConceptID.RX_TREATMENT, None]),
                    'drug_id': random.choice([84795, 78643, 75523, 80586]),
                    'voided': 0,
                    'location_id': visit.location_id,
                })

        # patient_prescription (needed by regimen ETL section)
        if patient.is_on_arv and patient.visits:
            drugs = random.sample([84795, 78643, 75523, 80586, 70056], 3)
            for visit in patient.visits:
                enc_id = (visit.encounters[0].encounter_id
                          if visit.encounters else
                          self.id_gen.next('rx_enc'))
                rx = random.choice([ConceptID.RX_TREATMENT, None])
                for drug_id in drugs:
                    self.patient_prescription.append({
                        'patient_id': patient.patient_id,
                        'encounter_id': enc_id,
                        'location_id': visit.location_id,
                        'visit_date': visit.date_started,
                        'drug_id': drug_id,
                        'arv_drug': ConceptID.YES,
                        'rx_or_prophy': rx,
                        'voided': 0,
                    })

        # patient_laboratory (needed by alert section)
        if patient.is_on_arv and random.random() < self.config.pct_with_viral_load:
            test_date = (patient.date_started_arv or patient.visits[0].date_started) \
                        + timedelta(days=180)
            for _ in range(random.randint(1, 4)):
                if test_date > datetime.now():
                    break
                vl = random.randint(20, 100000)
                enc_id = self.id_gen.next('lab_enc')
                self.patient_laboratory.append({
                    'patient_id': patient.patient_id,
                    'encounter_id': enc_id,
                    'location_id': patient.location_id,
                    'test_id': ConceptID.VIRAL_LOAD_NUMERIC,
                    'test_done': 1,
                    'test_result': vl,
                    'visit_date': test_date,
                    'date_test_done': test_date,
                    'voided': 0,
                })
                self.patient_laboratory.append({
                    'patient_id': patient.patient_id,
                    'encounter_id': enc_id,
                    'location_id': patient.location_id,
                    'test_id': ConceptID.VIRAL_LOAD_CODED,
                    'test_done': 1,
                    'test_result': (ConceptID.SUPPRESSED if vl < 1000
                                    else 1301),
                    'visit_date': test_date,
                    'date_test_done': test_date,
                    'voided': 0,
                })
                test_date += timedelta(days=random.randint(180, 365))

        # patient_pregnancy_records (needed by alert 2 - pregnant ARV)
        if patient.is_pregnant:
            self.patient_pregnancy_records.append({
                'patient_id': patient.patient_id,
                'encounter_id': self.id_gen.next('preg_enc'),
                'start_date': (datetime.now() - timedelta(
                    days=random.randint(0, 270))).date(),
                'voided': 0,
            })

    # -------------------------------------------------------------------------
    # Person-level support records
    # -------------------------------------------------------------------------

    def _generate_person_name(self, patient: Patient, now: datetime) -> Dict:
        return {
            'person_name_id': self.id_gen.next('person_name'),
            'person_id': patient.patient_id,
            'given_name': patient.given_name,
            'family_name': patient.family_name,
            'preferred': 1,
            'creator': 1,
            'date_created': now,
            'voided': 0,
            'uuid': _generate_uuid(),
        }

    def _generate_person_address(self, patient: Patient, now: datetime) -> Dict:
        streets = [
            'Rue Capois', 'Rue Pavée', 'Ave Jean Paul II',
            'Blvd Harry Truman', 'Route de Delmas', 'Rue Monseigneur Guilloux',
            'Ave Lamartinière', 'Rue des Miracles',
        ]
        areas = [
            'Pétion-Ville', 'Delmas', 'Tabarre', 'Carrefour',
            'Croix-des-Bouquets', 'Kenscoff', 'Cité Soleil',
        ]
        return {
            'person_address_id': self.id_gen.next('person_address'),
            'person_id': patient.patient_id,
            'address1': f'{random.randint(1, 500)} {random.choice(streets)}',
            'address2': random.choice(areas),
            'preferred': 1,
            'creator': 1,
            'date_created': now,
            'voided': 0,
            'uuid': _generate_uuid(),
        }

    def _generate_identifiers(self, patient: Patient, now: datetime) -> List[Dict]:
        recs = []
        for name, type_id_key in [
            ('st', 'id_type_st'), ('pc', 'id_type_pc'),
            ('national', 'id_type_national'),
            ('isanteplus', 'id_type_isanteplus'),
        ]:
            type_id = self.concept_ids.get(type_id_key, 99)
            recs.append({
                'patient_identifier_id': self.id_gen.next('identifier'),
                'patient_id': patient.patient_id,
                'identifier': f'{name.upper()}-{patient.patient_id:06d}',
                'identifier_type': type_id,
                'location_id': patient.location_id,
                'preferred': 1,
                'creator': 1,
                'date_created': now,
                'voided': 0,
                'uuid': _generate_uuid(),
            })
        return recs

    def _generate_person_attributes(
        self, patient: Patient, now: datetime
    ) -> List[Dict]:
        attrs = []
        for attr_name, type_key, value in [
            ('birthplace',   'attr_type_birthplace',   'Port-au-Prince'),
            ('telephone',    'attr_type_telephone',    '509-3700-0000'),
            ('mothers_name', 'attr_type_mothers_name', 'Marie Baptiste'),
        ]:
            type_id = self.concept_ids.get(type_key, 99)
            attrs.append({
                'person_attribute_id': self.id_gen.next('person_attribute'),
                'person_id': patient.patient_id,
                'value': value,
                'person_attribute_type_id': type_id,
                'creator': 1,
                'date_created': now,
                'voided': 0,
                'uuid': _generate_uuid(),
            })
        return attrs

    # -------------------------------------------------------------------------
    # Seed patients
    # -------------------------------------------------------------------------

    def _generate_seed_patients(self) -> List[Patient]:
        """Create specific patients to ensure all ETL code paths are exercised."""
        seeds = []
        now = datetime.now()

        scenarios = [
            # (hiv+, on_arv, disc, disc_reason, months_arv, pregnant, pediatric,
            #   has_tb, has_lab, has_obgyn)
            # Alert 1: ARV ≥6 months, no viral load
            (True,  True,  False, None,                    8,  False, False, False, False, False),
            # Alert 2: pregnant, ARV ≥4 months, no viral load
            (True,  True,  False, None,                    5,  True,  False, False, False, True),
            # Alert 3: last viral load >12 months ago (generate lab record)
            (True,  True,  False, None,                    18, False, False, False, True,  False),
            # HIV+ with TB
            (True,  True,  False, None,                    12, False, False, True,  True,  False),
            # Pediatric HIV
            (True,  False, False, None,                    None, False, True, False, True, False),
            # Discontinued (deceased)
            (True,  True,  True,  ConceptID.DECEASED,      12, False, False, False, False, False),
            # Discontinued (transferred)
            (True,  True,  True,  ConceptID.TRANSFERRED,   12, False, False, False, False, False),
            # Pregnant HIV+ (OB/GYN)
            (True,  True,  False, None,                    6,  True,  False, False, False, True),
            # Non-HIV patient with nutrition issues
            (False, False, False, None,                    None, False, False, False, True,  False),
            # Adult male with TB
            (True,  True,  False, None,                    24, False, False, True,  True,  False),
        ]

        for (hiv, arv, disc, reason, months_arv, pregnant, pediatric,
             has_tb, has_lab, has_obgyn) in scenarios:
            patient_id = self.id_gen.next('patient')
            date_started_arv = None
            if arv and months_arv:
                date_started_arv = now - timedelta(days=months_arv * 30)

            if pediatric:
                birthdate = now - timedelta(days=random.randint(180, 12 * 365))
            else:
                birthdate = now - timedelta(days=random.randint(20 * 365, 50 * 365))

            gender = 'F' if pregnant else random.choice(['M', 'F'])
            given, family = self._random_name()
            patient = Patient(
                patient_id=patient_id,
                birthdate=birthdate,
                gender=gender,
                is_hiv_positive=hiv,
                is_on_arv=arv,
                is_discontinued=disc,
                discontinuation_reason=reason,
                is_pregnant=pregnant,
                is_pediatric=pediatric,
                date_started_arv=date_started_arv,
                location_id=1,
                has_tb=has_tb,
                has_lab=has_lab,
                has_nutrition=True,
                has_obgyn=has_obgyn,
                given_name=given,
                family_name=family,
            )
            seeds.append(patient)

        return seeds

    # -------------------------------------------------------------------------
    # Main generate() method
    # -------------------------------------------------------------------------

    def generate(self) -> None:
        """Generate all test data."""
        print(f"Generating test data for {self.config.num_patients} patients...")
        print(f"  Using seed: {self.seed} (use --seed {self.seed} to reproduce)")

        now = datetime.now()

        # Setup reference / lookup tables
        self.encounter_types = self._setup_encounter_types()
        self.concepts = self._setup_concepts()
        self.concept_names = self._setup_concept_names()
        self.identifier_types = self._setup_identifier_types()
        self.person_attr_types = self._setup_person_attr_types()
        self.locations = self._setup_locations()
        self.location_attr_types = self._setup_location_attr_types()
        self.location_attributes = self._setup_location_attributes(self.locations)

        # Generate patients: seeds + noise + random
        seed_patients = self._generate_seed_patients()
        num_noise = int(self.config.num_patients * self.config.pct_noise)
        num_random = max(0, self.config.num_patients - len(seed_patients) - num_noise)
        noise_patients = [self._generate_noise_patient() for _ in range(num_noise)]
        all_patients = seed_patients + noise_patients + [
            self._generate_patient() for _ in range(num_random)
        ]

        for i, patient in enumerate(all_patients):
            if (i + 1) % 10000 == 0:
                print(f"  Generated {i + 1} patients...")

            self.patients.append(patient)

            # Person name, address, and identifiers
            self.person_names.append(self._generate_person_name(patient, now))
            self.person_addresses.append(self._generate_person_address(patient, now))
            self.patient_identifiers.extend(
                self._generate_identifiers(patient, now))
            self.person_attributes.extend(
                self._generate_person_attributes(patient, now))

            # Visits and encounters
            patient.visits = self._generate_visits_for_patient(patient)
            self.visits.extend(patient.visits)

            for idx, visit in enumerate(patient.visits):
                enc_type_names = self._encounter_types_for_visit(
                    patient, is_first=(idx == 0))
                for enc_type_name in enc_type_names:
                    # Noise patients use noise_encounter_type_ids; others use encounter_type_ids
                    if patient.noise:
                        type_id = self.noise_encounter_type_ids.get(enc_type_name)
                    else:
                        type_id = self.encounter_type_ids.get(enc_type_name)
                    if type_id is None:
                        continue
                    enc = Encounter(
                        encounter_id=self.id_gen.next('encounter'),
                        visit_id=visit.visit_id,
                        patient_id=patient.patient_id,
                        encounter_type_id=type_id,
                        encounter_datetime=visit.date_started,
                        location_id=visit.location_id,
                        form_id=random.randint(1, 150),
                    )
                    visit.encounters.append(enc)
                    self.encounters.append(enc)
                    self.encounter_providers.append({
                        'encounter_provider_id': self.id_gen.next('enc_provider'),
                        'encounter_id': enc.encounter_id,
                        'provider_id': random.randint(1, 20),
                        'encounter_role_id': 1,
                        'creator': 1,
                        'date_created': visit.date_started,
                        'voided': 1 if random.random() < 0.05 else 0,
                        'uuid': _generate_uuid(),
                    })
                    if patient.noise:
                        self._generate_noise_observations(enc, patient)
                    else:
                        self._generate_observations_for_encounter(
                            enc, patient, enc_type_name)

            # Add a voided visit with valid encounter types for some normal patients
            if (not patient.noise
                    and patient.visits
                    and random.random() < self.config.pct_voided_visit):
                voided_visit = Visit(
                    visit_id=self.id_gen.next('visit'),
                    patient_id=patient.patient_id,
                    date_started=patient.visits[-1].date_started,
                    location_id=patient.location_id,
                    voided=1,
                )
                patient.visits.append(voided_visit)
                self.visits.append(voided_visit)
                # Use a valid ETL encounter type so this tests voided filtering
                voided_enc_type = random.choice(list(ENCOUNTER_TYPES.keys()))
                voided_type_id = self.encounter_type_ids[voided_enc_type]
                voided_enc = Encounter(
                    encounter_id=self.id_gen.next('encounter'),
                    visit_id=voided_visit.visit_id,
                    patient_id=patient.patient_id,
                    encounter_type_id=voided_type_id,
                    encounter_datetime=voided_visit.date_started,
                    location_id=voided_visit.location_id,
                    form_id=random.randint(1, 150),
                    voided=1,
                )
                voided_visit.encounters.append(voided_enc)
                self.encounters.append(voided_enc)
                for _ in range(random.randint(2, 3)):
                    self._make_obs(
                        patient, voided_enc,
                        random.choice([ConceptID.WEIGHT, ConceptID.HEIGHT,
                                       ConceptID.CD4_COUNT]),
                        value_numeric=round(random.uniform(10, 200), 1),
                    )
                # Mark those obs as voided
                for obs in voided_enc.observations:
                    obs.voided = 1

            # iSantePlus source data
            if not patient.noise:
                self._generate_isanteplus_data(patient)

        print(f"Generated:")
        print(f"  - {len(self.patients)} patients")
        print(f"  - {len(self.visits)} visits")
        print(f"  - {len(self.encounters)} encounters")
        print(f"  - {len(self.observations)} observations")
        print(f"  - {len(self.patient_dispensing)} dispensing records")
        print(f"  - {len(self.patient_laboratory)} laboratory records")


# =============================================================================
# SQL OUTPUT
# =============================================================================

class SQLWriter:
    """Writes generated data to SQL INSERT files."""

    def __init__(self, generator: TestDataGenerator, output_dir: str = '.'):
        self.gen = generator
        self.output_dir = output_dir
        ensure_directory(output_dir)

    def _escape_value(self, val: Any) -> str:
        if val is None:
            return 'NULL'
        if isinstance(val, bool):
            return '1' if val else '0'
        if isinstance(val, (int, float)):
            return str(val)
        if isinstance(val, datetime):
            return f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'"
        escaped = str(val).replace("'", "''")
        return f"'{escaped}'"

    def _write_batch_insert(
        self,
        f: TextIO,
        table: str,
        columns: List[str],
        rows: List[tuple],
        batch_size: int = 1000,
    ) -> None:
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            f.write(f"INSERT INTO {table} ({', '.join(columns)}) VALUES\n")
            f.write(',\n'.join(
                '(' + ', '.join(self._escape_value(v) for v in row) + ')'
                for row in batch
            ))
            f.write(';\n\n')

    def write_openmrs_data(self, filename: str = 'test_data_reports_openmrs.sql') -> None:
        """Write OpenMRS source schema data to SQL file."""
        filepath = os.path.join(self.output_dir, filename)
        print(f"Writing OpenMRS data to {filepath}...")
        now = datetime.now()

        with open(filepath, 'w') as f:
            f.write("-- Generated test data for OpenMRS schema (reports ETL)\n")
            f.write("-- Run this against a test database only!\n\n")
            f.write("USE openmrs;\n\n")
            f.write("SET FOREIGN_KEY_CHECKS = 0;\n")
            f.write("SET SQL_MODE = 'NO_AUTO_VALUE_ON_ZERO';\n\n")

            # Location attribute types
            f.write("-- Location attribute types\n")
            f.write("TRUNCATE TABLE location_attribute_type;\n")
            rows = [
                (la['location_attribute_type_id'], la['name'], la['uuid'],
                 la['creator'], la['date_created'])
                for la in self.gen.location_attr_types
            ]
            self._write_batch_insert(
                f, 'location_attribute_type',
                ['location_attribute_type_id', 'name', 'uuid', 'creator', 'date_created'],
                rows)

            # Locations
            f.write("-- Locations\n")
            f.write("TRUNCATE TABLE location;\n")
            rows = [
                (loc['location_id'], loc['name'], loc['uuid'],
                 loc['creator'], loc['date_created'], loc['retired'])
                for loc in self.gen.locations
            ]
            self._write_batch_insert(
                f, 'location',
                ['location_id', 'name', 'uuid', 'creator', 'date_created', 'retired'],
                rows)

            # Location attributes (site codes)
            f.write("-- Location attributes (site codes)\n")
            f.write("TRUNCATE TABLE location_attribute;\n")
            rows = [
                (la['location_attribute_id'], la['location_id'],
                 la['attribute_type_id'], la['value_reference'],
                 la['uuid'], la['creator'], la['date_created'])
                for la in self.gen.location_attributes
            ]
            self._write_batch_insert(
                f, 'location_attribute',
                ['location_attribute_id', 'location_id', 'attribute_type_id',
                 'value_reference', 'uuid', 'creator', 'date_created'],
                rows)

            # Encounter types
            f.write("-- Encounter types\n")
            f.write("TRUNCATE TABLE encounter_type;\n")
            rows = [
                (et['encounter_type_id'], et['name'], et['uuid'],
                 et['creator'], et['date_created'], 0)
                for et in self.gen.encounter_types
            ]
            self._write_batch_insert(
                f, 'encounter_type',
                ['encounter_type_id', 'name', 'uuid', 'creator', 'date_created', 'retired'],
                rows)

            # Concepts
            f.write("-- Concepts (for UUID lookups)\n")
            f.write("TRUNCATE TABLE concept;\n")
            rows = [(c['concept_id'], c['uuid']) for c in self.gen.concepts]
            self._write_batch_insert(f, 'concept', ['concept_id', 'uuid'], rows)

            # Concept names (for locale='fr' joins in lab section)
            f.write("-- Concept names (French locale)\n")
            f.write("TRUNCATE TABLE concept_name;\n")
            rows = [
                (cn['concept_name_id'], cn['concept_id'], cn['locale'],
                 cn['name'], cn['concept_name_type'], cn['uuid'])
                for cn in self.gen.concept_names
            ]
            self._write_batch_insert(
                f, 'concept_name',
                ['concept_name_id', 'concept_id', 'locale', 'name',
                 'concept_name_type', 'uuid'],
                rows)

            # Patient identifier types
            f.write("-- Patient identifier types\n")
            f.write("TRUNCATE TABLE patient_identifier_type;\n")
            rows = [
                (it['patient_identifier_type_id'], it['name'], it['uuid'],
                 it['creator'], it['date_created'], it['required'])
                for it in self.gen.identifier_types
            ]
            self._write_batch_insert(
                f, 'patient_identifier_type',
                ['patient_identifier_type_id', 'name', 'uuid', 'creator',
                 'date_created', 'required'],
                rows)

            # Person attribute types
            f.write("-- Person attribute types\n")
            f.write("TRUNCATE TABLE person_attribute_type;\n")
            rows = [
                (pt['person_attribute_type_id'], pt['name'], pt['uuid'],
                 pt['creator'], pt['date_created'])
                for pt in self.gen.person_attr_types
            ]
            self._write_batch_insert(
                f, 'person_attribute_type',
                ['person_attribute_type_id', 'name', 'uuid', 'creator', 'date_created'],
                rows)

            # Person
            f.write("-- Person records\n")
            f.write("TRUNCATE TABLE person;\n")
            rows = [
                (p.person_id, p.gender, p.birthdate, 1, now, 0, p.person_uuid)
                for p in self.gen.patients
            ]
            self._write_batch_insert(
                f, 'person',
                ['person_id', 'gender', 'birthdate', 'creator',
                 'date_created', 'voided', 'uuid'],
                rows)

            # Person names
            f.write("-- Person names\n")
            f.write("TRUNCATE TABLE person_name;\n")
            rows = [
                (pn['person_name_id'], pn['person_id'], pn['given_name'],
                 pn['family_name'], pn['preferred'], pn['creator'],
                 pn['date_created'], pn['voided'], pn['uuid'])
                for pn in self.gen.person_names
            ]
            self._write_batch_insert(
                f, 'person_name',
                ['person_name_id', 'person_id', 'given_name', 'family_name',
                 'preferred', 'creator', 'date_created', 'voided', 'uuid'],
                rows)

            # Person addresses
            f.write("-- Person addresses\n")
            f.write("TRUNCATE TABLE person_address;\n")
            rows = [
                (pa['person_address_id'], pa['person_id'], pa['address1'],
                 pa['address2'], pa['preferred'], pa['creator'],
                 pa['date_created'], pa['voided'], pa['uuid'])
                for pa in self.gen.person_addresses
            ]
            self._write_batch_insert(
                f, 'person_address',
                ['person_address_id', 'person_id', 'address1', 'address2',
                 'preferred', 'creator', 'date_created', 'voided', 'uuid'],
                rows)

            # Person attributes
            f.write("-- Person attributes\n")
            f.write("TRUNCATE TABLE person_attribute;\n")
            rows = [
                (pa['person_attribute_id'], pa['person_id'],
                 pa['value'], pa['person_attribute_type_id'],
                 pa['creator'], pa['date_created'], pa['voided'], pa['uuid'])
                for pa in self.gen.person_attributes
            ]
            self._write_batch_insert(
                f, 'person_attribute',
                ['person_attribute_id', 'person_id', 'value',
                 'person_attribute_type_id', 'creator', 'date_created',
                 'voided', 'uuid'],
                rows)

            # Patient (OpenMRS)
            f.write("-- Patient records\n")
            f.write("TRUNCATE TABLE patient;\n")
            rows = [(p.patient_id, 1, now, 0) for p in self.gen.patients]
            self._write_batch_insert(
                f, 'patient',
                ['patient_id', 'creator', 'date_created', 'voided'],
                rows)

            # Patient identifiers
            f.write("-- Patient identifiers\n")
            f.write("TRUNCATE TABLE patient_identifier;\n")
            rows = [
                (pi['patient_identifier_id'], pi['patient_id'],
                 pi['identifier'], pi['identifier_type'], pi['location_id'],
                 pi['preferred'],
                 pi['creator'], pi['date_created'], pi['voided'], pi['uuid'])
                for pi in self.gen.patient_identifiers
            ]
            self._write_batch_insert(
                f, 'patient_identifier',
                ['patient_identifier_id', 'patient_id', 'identifier',
                 'identifier_type', 'location_id', 'preferred', 'creator',
                 'date_created', 'voided', 'uuid'],
                rows)

            # Visit
            f.write("-- Visit records\n")
            f.write("TRUNCATE TABLE visit;\n")
            rows = [
                (v.visit_id, v.patient_id, v.date_started, v.date_stopped,
                 v.location_id, 1, now, v.voided, v.uuid)
                for v in self.gen.visits
            ]
            self._write_batch_insert(
                f, 'visit',
                ['visit_id', 'patient_id', 'date_started', 'date_stopped',
                 'location_id', 'creator', 'date_created', 'voided', 'uuid'],
                rows)

            # Encounter
            f.write("-- Encounter records\n")
            f.write("TRUNCATE TABLE encounter;\n")
            rows = [
                (e.encounter_id, e.encounter_type_id, e.patient_id,
                 e.location_id, e.form_id, e.encounter_datetime, e.visit_id,
                 1, now, e.voided, e.uuid)
                for e in self.gen.encounters
            ]
            self._write_batch_insert(
                f, 'encounter',
                ['encounter_id', 'encounter_type', 'patient_id', 'location_id',
                 'form_id', 'encounter_datetime', 'visit_id', 'creator',
                 'date_created', 'voided', 'uuid'],
                rows)

            # Encounter provider
            f.write("-- Encounter provider records\n")
            f.write("TRUNCATE TABLE encounter_provider;\n")
            rows = [
                (ep['encounter_provider_id'], ep['encounter_id'],
                 ep['provider_id'], ep['encounter_role_id'], ep['creator'],
                 ep['date_created'], ep['voided'], ep['uuid'])
                for ep in self.gen.encounter_providers
            ]
            self._write_batch_insert(
                f, 'encounter_provider',
                ['encounter_provider_id', 'encounter_id', 'provider_id',
                 'encounter_role_id', 'creator', 'date_created', 'voided',
                 'uuid'],
                rows)

            # Obs
            f.write(f"-- Observation records ({len(self.gen.observations)} rows)\n")
            f.write("TRUNCATE TABLE obs;\n")
            rows = [
                (o.obs_id, o.person_id, o.encounter_id, o.concept_id,
                 o.obs_datetime, o.location_id, o.value_coded,
                 o.value_numeric, o.value_datetime, o.value_text,
                 o.obs_group_id, 1, now, o.voided, o.uuid)
                for o in self.gen.observations
            ]
            self._write_batch_insert(
                f, 'obs',
                ['obs_id', 'person_id', 'encounter_id', 'concept_id',
                 'obs_datetime', 'location_id', 'value_coded',
                 'value_numeric', 'value_datetime', 'value_text',
                 'obs_group_id', 'creator', 'date_created', 'voided', 'uuid'],
                rows)

            f.write("SET FOREIGN_KEY_CHECKS = 1;\n")

        print(f"  Written {len(self.gen.observations)} observation records")

    def write_isanteplus_data(
        self, filename: str = 'test_data_reports_isanteplus.sql'
    ) -> None:
        """Write iSantePlus source schema data and clear destination tables."""
        filepath = os.path.join(self.output_dir, filename)
        print(f"Writing iSantePlus data to {filepath}...")

        with open(filepath, 'w') as f:
            f.write("-- Generated iSantePlus source data for reports ETL\n")
            f.write("-- Run this against a test database only!\n\n")
            f.write("USE isanteplus;\n\n")
            f.write("SET FOREIGN_KEY_CHECKS = 0;\n\n")

            # isanteplus.patient (demographics ETL populates this; we seed it)
            f.write("-- iSantePlus patient records (seeded for alert section)\n")
            f.write("TRUNCATE TABLE patient;\n")
            rows = [
                (p['patient_id'], p['location_id'], p['vih_status'],
                 p['date_started_arv'], p['voided'])
                for p in self.gen.isanteplus_patients
            ]
            self._write_batch_insert(
                f, 'patient',
                ['patient_id', 'location_id', 'vih_status',
                 'date_started_arv', 'voided'],
                rows)

            # patient_on_arv
            f.write("-- Patients on ARV\n")
            f.write("TRUNCATE TABLE patient_on_arv;\n")
            rows = [(pid,) for pid in self.gen.patient_on_arv]
            if rows:
                self._write_batch_insert(f, 'patient_on_arv', ['patient_id'], rows)

            # discontinuation_reason
            f.write("-- Discontinuation reasons\n")
            f.write("TRUNCATE TABLE discontinuation_reason;\n")
            rows = [
                (dr['patient_id'], dr['reason'], dr['visit_date'],
                 dr.get('visit_id'))
                for dr in self.gen.discontinuation_reasons
            ]
            if rows:
                self._write_batch_insert(
                    f, 'discontinuation_reason',
                    ['patient_id', 'reason', 'visit_date', 'visit_id'],
                    rows)

            # patient_dispensing
            f.write("-- Patient dispensing records\n")
            f.write("TRUNCATE TABLE patient_dispensing;\n")
            rows = [
                (pd['patient_id'], pd['encounter_id'], pd['visit_id'],
                 pd['visit_date'], pd['next_dispensation_date'],
                 pd['arv_drug'], pd['rx_or_prophy'], pd['drug_id'],
                 pd['voided'], pd['location_id'])
                for pd in self.gen.patient_dispensing
            ]
            if rows:
                self._write_batch_insert(
                    f, 'patient_dispensing',
                    ['patient_id', 'encounter_id', 'visit_id', 'visit_date',
                     'next_dispensation_date', 'arv_drug', 'rx_or_prophy',
                     'drug_id', 'voided', 'location_id'],
                    rows)

            # patient_prescription
            f.write("-- Patient prescription records\n")
            f.write("TRUNCATE TABLE patient_prescription;\n")
            rows = [
                (pp['patient_id'], pp['encounter_id'], pp['location_id'],
                 pp['visit_date'], pp['drug_id'], pp['arv_drug'],
                 pp['rx_or_prophy'], pp['voided'])
                for pp in self.gen.patient_prescription
            ]
            if rows:
                self._write_batch_insert(
                    f, 'patient_prescription',
                    ['patient_id', 'encounter_id', 'location_id',
                     'visit_date', 'drug_id', 'arv_drug', 'rx_or_prophy',
                     'voided'],
                    rows)

            # patient_laboratory
            f.write("-- Patient laboratory records\n")
            f.write("TRUNCATE TABLE patient_laboratory;\n")
            rows = [
                (pl['patient_id'], pl['encounter_id'], pl['location_id'],
                 pl['test_id'], pl['test_done'], pl['test_result'],
                 pl['visit_date'], pl['date_test_done'], pl['voided'])
                for pl in self.gen.patient_laboratory
            ]
            if rows:
                self._write_batch_insert(
                    f, 'patient_laboratory',
                    ['patient_id', 'encounter_id', 'location_id', 'test_id',
                     'test_done', 'test_result', 'visit_date',
                     'date_test_done', 'voided'],
                    rows)

            # patient_pregnancy (source records for alert 2)
            f.write("-- Patient pregnancy records\n")
            f.write("TRUNCATE TABLE patient_pregnancy;\n")
            rows = [
                (pp['patient_id'], pp['encounter_id'], pp['start_date'],
                 pp['voided'])
                for pp in self.gen.patient_pregnancy_records
            ]
            if rows:
                self._write_batch_insert(
                    f, 'patient_pregnancy',
                    ['patient_id', 'encounter_id', 'start_date', 'voided'],
                    rows)

            # Clear all ETL destination tables
            f.write("-- Clear ETL output/destination tables\n")
            for table in [
                'alert', 'visit_type', 'patient_visit', 'patient_delivery',
                'virological_tests', 'pediatric_hiv_visit', 'patient_menstruation',
                'vih_risk_factor', 'vaccination', 'serological_tests', 'patient_pcr',
                'patient_malaria', 'patient_on_art', 'key_populations',
                'family_planning', 'patient_tb_diagnosis', 'patient_nutrition',
                'patient_ob_gyn', 'patient_imagerie', 'stopping_reason',
                'health_qual_patient_visit', 'regimen', 'pepfarTable',
                'exposed_infants', 'patient_immunization', 'immunization_dose',
            ]:
                f.write(f"TRUNCATE TABLE {table};\n")

            f.write("\nSET FOREIGN_KEY_CHECKS = 1;\n")

        print(f"  Written {len(self.gen.patient_dispensing)} dispensing records")
        print(f"  Written {len(self.gen.patient_laboratory)} laboratory records")


# =============================================================================
# DDL OUTPUT
# =============================================================================

class DDLWriter:
    """Writes DDL (CREATE TABLE) statements for all required tables."""

    def __init__(self, output_dir: str = '.'):
        self.output_dir = output_dir
        ensure_directory(output_dir)

    def write_openmrs_ddl(self, filename: str = 'ddl_reports_openmrs.sql') -> None:
        filepath = os.path.join(self.output_dir, filename)
        print(f"Writing OpenMRS DDL to {filepath}...")
        ddl = """\
-- =============================================================================
-- OpenMRS Schema DDL for Reports ETL Testing
-- =============================================================================
CREATE DATABASE IF NOT EXISTS openmrs;
USE openmrs;
SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS location_attribute_type;
CREATE TABLE location_attribute_type (
    location_attribute_type_id INT NOT NULL AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL,
    uuid CHAR(38) NOT NULL,
    creator INT NOT NULL DEFAULT 1,
    date_created DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    retired TINYINT(1) NOT NULL DEFAULT 0,
    PRIMARY KEY (location_attribute_type_id),
    UNIQUE KEY uuid (uuid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS location;
CREATE TABLE location (
    location_id INT NOT NULL AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL DEFAULT '',
    uuid CHAR(38) NOT NULL,
    creator INT NOT NULL DEFAULT 1,
    date_created DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    retired TINYINT(1) NOT NULL DEFAULT 0,
    PRIMARY KEY (location_id),
    UNIQUE KEY uuid (uuid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS location_attribute;
CREATE TABLE location_attribute (
    location_attribute_id INT NOT NULL AUTO_INCREMENT,
    location_id INT NOT NULL,
    attribute_type_id INT NOT NULL,
    value_reference VARCHAR(255) NOT NULL,
    uuid CHAR(38) NOT NULL,
    creator INT NOT NULL DEFAULT 1,
    date_created DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    voided TINYINT(1) NOT NULL DEFAULT 0,
    PRIMARY KEY (location_attribute_id),
    UNIQUE KEY uuid (uuid),
    KEY idx_location_attribute_location (location_id),
    CONSTRAINT loc_attr_location_fk FOREIGN KEY (location_id)
        REFERENCES location (location_id),
    CONSTRAINT loc_attr_type_fk FOREIGN KEY (attribute_type_id)
        REFERENCES location_attribute_type (location_attribute_type_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS encounter_type;
CREATE TABLE encounter_type (
    encounter_type_id INT NOT NULL AUTO_INCREMENT,
    name VARCHAR(50) NOT NULL DEFAULT '',
    description VARCHAR(255) DEFAULT NULL,
    creator INT NOT NULL DEFAULT 0,
    date_created DATETIME NOT NULL,
    retired TINYINT(1) NOT NULL DEFAULT 0,
    uuid CHAR(38) NOT NULL,
    PRIMARY KEY (encounter_type_id),
    UNIQUE KEY uuid (uuid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS concept;
CREATE TABLE concept (
    concept_id INT NOT NULL AUTO_INCREMENT,
    retired TINYINT(1) NOT NULL DEFAULT 0,
    datatype_id INT NOT NULL DEFAULT 0,
    class_id INT NOT NULL DEFAULT 0,
    is_set TINYINT(1) NOT NULL DEFAULT 0,
    creator INT NOT NULL DEFAULT 0,
    date_created DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    uuid CHAR(38) NOT NULL,
    PRIMARY KEY (concept_id),
    UNIQUE KEY uuid (uuid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS concept_name;
CREATE TABLE concept_name (
    concept_name_id INT NOT NULL AUTO_INCREMENT,
    concept_id INT NOT NULL DEFAULT 0,
    locale VARCHAR(50) NOT NULL DEFAULT '',
    name VARCHAR(255) NOT NULL DEFAULT '',
    concept_name_type VARCHAR(50) DEFAULT NULL,
    uuid CHAR(38) NOT NULL,
    voided TINYINT(1) NOT NULL DEFAULT 0,
    PRIMARY KEY (concept_name_id),
    UNIQUE KEY uuid (uuid),
    KEY idx_concept_name_concept (concept_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS patient_identifier_type;
CREATE TABLE patient_identifier_type (
    patient_identifier_type_id INT NOT NULL AUTO_INCREMENT,
    name VARCHAR(50) NOT NULL DEFAULT '',
    uuid CHAR(38) NOT NULL,
    creator INT NOT NULL DEFAULT 0,
    date_created DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    required TINYINT(1) NOT NULL DEFAULT 0,
    retired TINYINT(1) NOT NULL DEFAULT 0,
    PRIMARY KEY (patient_identifier_type_id),
    UNIQUE KEY uuid (uuid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS person_attribute_type;
CREATE TABLE person_attribute_type (
    person_attribute_type_id INT NOT NULL AUTO_INCREMENT,
    name VARCHAR(50) NOT NULL DEFAULT '',
    uuid CHAR(38) NOT NULL,
    creator INT NOT NULL DEFAULT 0,
    date_created DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    retired TINYINT(1) NOT NULL DEFAULT 0,
    PRIMARY KEY (person_attribute_type_id),
    UNIQUE KEY uuid (uuid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS person;
CREATE TABLE person (
    person_id INT NOT NULL AUTO_INCREMENT,
    gender VARCHAR(50) DEFAULT '',
    birthdate DATE DEFAULT NULL,
    creator INT DEFAULT NULL,
    date_created DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    voided TINYINT(1) NOT NULL DEFAULT 0,
    uuid CHAR(38) NOT NULL,
    PRIMARY KEY (person_id),
    UNIQUE KEY uuid (uuid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS person_name;
CREATE TABLE person_name (
    person_name_id INT NOT NULL AUTO_INCREMENT,
    person_id INT NOT NULL,
    given_name VARCHAR(50) DEFAULT NULL,
    family_name VARCHAR(50) DEFAULT NULL,
    preferred TINYINT(1) NOT NULL DEFAULT 0,
    creator INT NOT NULL DEFAULT 0,
    date_created DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    voided TINYINT(1) NOT NULL DEFAULT 0,
    uuid CHAR(38) NOT NULL,
    PRIMARY KEY (person_name_id),
    UNIQUE KEY uuid (uuid),
    KEY idx_person_name_person (person_id),
    CONSTRAINT person_name_person_fk FOREIGN KEY (person_id)
        REFERENCES person (person_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS person_address;
CREATE TABLE person_address (
    person_address_id INT NOT NULL AUTO_INCREMENT,
    person_id INT NOT NULL,
    address1 VARCHAR(255) DEFAULT NULL,
    address2 VARCHAR(255) DEFAULT NULL,
    preferred TINYINT(1) NOT NULL DEFAULT 0,
    creator INT NOT NULL DEFAULT 0,
    date_created DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    voided TINYINT(1) NOT NULL DEFAULT 0,
    uuid CHAR(38) NOT NULL,
    PRIMARY KEY (person_address_id),
    UNIQUE KEY uuid (uuid),
    KEY idx_person_address_person (person_id),
    CONSTRAINT person_address_person_fk FOREIGN KEY (person_id)
        REFERENCES person (person_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS person_attribute;
CREATE TABLE person_attribute (
    person_attribute_id INT NOT NULL AUTO_INCREMENT,
    person_id INT NOT NULL,
    value VARCHAR(50) NOT NULL DEFAULT '',
    person_attribute_type_id INT NOT NULL,
    creator INT NOT NULL DEFAULT 0,
    date_created DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    voided TINYINT(1) NOT NULL DEFAULT 0,
    uuid CHAR(38) NOT NULL,
    PRIMARY KEY (person_attribute_id),
    UNIQUE KEY uuid (uuid),
    KEY idx_person_attribute_person (person_id),
    CONSTRAINT person_attribute_person_fk FOREIGN KEY (person_id)
        REFERENCES person (person_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS patient;
CREATE TABLE patient (
    patient_id INT NOT NULL,
    creator INT NOT NULL DEFAULT 0,
    date_created DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    voided TINYINT(1) NOT NULL DEFAULT 0,
    PRIMARY KEY (patient_id),
    CONSTRAINT patient_person_fk FOREIGN KEY (patient_id)
        REFERENCES person (person_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS patient_identifier;
CREATE TABLE patient_identifier (
    patient_identifier_id INT NOT NULL AUTO_INCREMENT,
    patient_id INT NOT NULL,
    identifier VARCHAR(50) NOT NULL DEFAULT '',
    identifier_type INT NOT NULL,
    location_id INT DEFAULT NULL,
    preferred TINYINT(1) NOT NULL DEFAULT 0,
    creator INT NOT NULL DEFAULT 0,
    date_created DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    voided TINYINT(1) NOT NULL DEFAULT 0,
    uuid CHAR(38) NOT NULL,
    PRIMARY KEY (patient_identifier_id),
    UNIQUE KEY uuid (uuid),
    KEY idx_patient_identifier_patient (patient_id),
    KEY idx_patient_identifier_type (identifier_type),
    CONSTRAINT patient_identifier_patient_fk FOREIGN KEY (patient_id)
        REFERENCES patient (patient_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS visit;
CREATE TABLE visit (
    visit_id INT NOT NULL AUTO_INCREMENT,
    patient_id INT NOT NULL,
    visit_type_id INT NOT NULL DEFAULT 1,
    date_started DATETIME NOT NULL,
    date_stopped DATETIME DEFAULT NULL,
    location_id INT DEFAULT NULL,
    creator INT NOT NULL DEFAULT 1,
    date_created DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    voided TINYINT(1) NOT NULL DEFAULT 0,
    uuid CHAR(38) NOT NULL,
    PRIMARY KEY (visit_id),
    UNIQUE KEY uuid (uuid),
    KEY idx_visit_patient (patient_id),
    CONSTRAINT visit_patient_fk FOREIGN KEY (patient_id)
        REFERENCES patient (patient_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS encounter;
CREATE TABLE encounter (
    encounter_id INT NOT NULL AUTO_INCREMENT,
    encounter_type INT NOT NULL,
    patient_id INT NOT NULL,
    location_id INT DEFAULT NULL,
    form_id INT DEFAULT NULL,
    encounter_datetime DATETIME NOT NULL,
    creator INT NOT NULL DEFAULT 0,
    date_created DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    voided TINYINT(1) NOT NULL DEFAULT 0,
    visit_id INT DEFAULT NULL,
    uuid CHAR(38) NOT NULL,
    PRIMARY KEY (encounter_id),
    UNIQUE KEY uuid (uuid),
    KEY idx_encounter_patient (patient_id),
    KEY idx_encounter_type (encounter_type),
    KEY idx_encounter_datetime (encounter_datetime),
    KEY idx_encounter_visit (visit_id),
    CONSTRAINT encounter_patient_fk FOREIGN KEY (patient_id)
        REFERENCES patient (patient_id),
    CONSTRAINT encounter_type_fk FOREIGN KEY (encounter_type)
        REFERENCES encounter_type (encounter_type_id),
    CONSTRAINT encounter_visit_fk FOREIGN KEY (visit_id)
        REFERENCES visit (visit_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS encounter_provider;
CREATE TABLE encounter_provider (
    encounter_provider_id INT NOT NULL AUTO_INCREMENT,
    encounter_id INT NOT NULL,
    provider_id INT NOT NULL,
    encounter_role_id INT DEFAULT NULL,
    creator INT NOT NULL DEFAULT 0,
    date_created DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    voided TINYINT(1) NOT NULL DEFAULT 0,
    uuid CHAR(38) NOT NULL,
    PRIMARY KEY (encounter_provider_id),
    UNIQUE KEY uuid (uuid),
    KEY idx_ep_encounter (encounter_id),
    KEY idx_ep_provider (provider_id),
    CONSTRAINT ep_encounter_fk FOREIGN KEY (encounter_id)
        REFERENCES encounter (encounter_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS obs;
CREATE TABLE obs (
    obs_id INT NOT NULL AUTO_INCREMENT,
    person_id INT NOT NULL,
    concept_id INT NOT NULL DEFAULT 0,
    encounter_id INT DEFAULT NULL,
    obs_datetime DATETIME NOT NULL,
    location_id INT DEFAULT NULL,
    obs_group_id INT DEFAULT NULL,
    value_coded INT DEFAULT NULL,
    value_datetime DATETIME DEFAULT NULL,
    value_numeric DOUBLE DEFAULT NULL,
    value_text TEXT,
    comments VARCHAR(255) DEFAULT NULL,
    creator INT NOT NULL DEFAULT 0,
    date_created DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    voided TINYINT(1) NOT NULL DEFAULT 0,
    uuid CHAR(38) NOT NULL,
    PRIMARY KEY (obs_id),
    UNIQUE KEY uuid (uuid),
    KEY idx_obs_person (person_id),
    KEY idx_obs_concept (concept_id),
    KEY idx_obs_encounter (encounter_id),
    KEY idx_obs_datetime (obs_datetime),
    KEY idx_obs_group (obs_group_id),
    CONSTRAINT obs_person_fk FOREIGN KEY (person_id)
        REFERENCES person (person_id),
    CONSTRAINT obs_encounter_fk FOREIGN KEY (encounter_id)
        REFERENCES encounter (encounter_id),
    CONSTRAINT obs_group_fk FOREIGN KEY (obs_group_id)
        REFERENCES obs (obs_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

SET FOREIGN_KEY_CHECKS = 1;
"""
        with open(filepath, 'w') as f:
            f.write(ddl)
        print("  OpenMRS DDL written successfully")

    def write_isanteplus_ddl(
        self, filename: str = 'ddl_reports_isanteplus.sql'
    ) -> None:
        filepath = os.path.join(self.output_dir, filename)
        print(f"Writing iSantePlus DDL to {filepath}...")
        ddl = """\
-- =============================================================================
-- iSantePlus Schema DDL for Reports ETL Testing
-- Source tables (populated before ETL runs) and destination tables
-- =============================================================================
CREATE DATABASE IF NOT EXISTS isanteplus;
USE isanteplus;
SET FOREIGN_KEY_CHECKS = 0;

-- =========================================================================
-- All tables below match isanteplusreportsddlscript.sql (CREATE TABLE +
-- subsequent ALTER TABLE statements merged into a single definition).
-- =========================================================================

-- -------------------------------------------------------------------------
-- patient (lines 7-40 + ALTERs: voided 773, isante_id 846, contact_name
-- 849, site_code 1131, pc_id from DML preamble, date_transferred_in &
-- date_started_arv_other_site written by DML)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS patient;
CREATE TABLE patient (
    identifier VARCHAR(50) DEFAULT NULL,
    st_id VARCHAR(50) DEFAULT NULL,
    national_id VARCHAR(50) DEFAULT NULL,
    isante_id VARCHAR(50) DEFAULT NULL,
    patient_id INT NOT NULL,
    location_id INT DEFAULT NULL,
    site_code TEXT DEFAULT NULL,
    given_name LONGTEXT,
    family_name LONGTEXT,
    gender VARCHAR(10) DEFAULT NULL,
    birthdate DATE DEFAULT NULL,
    telephone VARCHAR(50) DEFAULT NULL,
    last_address LONGTEXT,
    degree LONGTEXT,
    vih_status INT DEFAULT 0,
    arv_status INT,
    mother_name LONGTEXT,
    contact_name TEXT DEFAULT NULL,
    pc_id VARCHAR(50) DEFAULT NULL,
    occupation INT,
    maritalStatus INT,
    place_of_birth LONGTEXT,
    creator VARCHAR(20) DEFAULT NULL,
    date_created DATE DEFAULT NULL,
    death_date DATE DEFAULT NULL,
    cause_of_death LONGTEXT,
    first_visit_date DATETIME,
    last_visit_date DATETIME,
    date_started_arv DATETIME,
    next_visit_date DATE,
    last_inserted_date DATETIME DEFAULT NULL,
    last_updated_date DATETIME DEFAULT NULL,
    transferred_in INT,
    date_transferred_in DATETIME DEFAULT NULL,
    date_started_arv_other_site DATETIME DEFAULT NULL,
    voided TINYINT(1) DEFAULT NULL,
    PRIMARY KEY (patient_id),
    KEY location_id (location_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -------------------------------------------------------------------------
-- patient_visit (lines 42-65 + voided ALTER 776)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS patient_visit;
CREATE TABLE patient_visit (
    visit_date DATE DEFAULT NULL,
    visit_id INT,
    encounter_id INT DEFAULT NULL,
    location_id INT DEFAULT NULL,
    patient_id INT,
    start_date DATE DEFAULT NULL,
    stop_date DATE DEFAULT NULL,
    creator VARCHAR(20) DEFAULT NULL,
    encounter_type INT DEFAULT NULL,
    form_id INT DEFAULT NULL,
    next_visit_date DATE DEFAULT NULL,
    last_insert_date DATE DEFAULT NULL,
    last_updated_date DATETIME,
    voided TINYINT(1),
    PRIMARY KEY (patient_id, encounter_id, location_id),
    KEY location_id (location_id),
    KEY visit_id (visit_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -------------------------------------------------------------------------
-- patient_tb_diagnosis (lines 69-96 + voided 780 + ALTERs 884-957)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS patient_tb_diagnosis;
CREATE TABLE patient_tb_diagnosis (
    patient_id INT NOT NULL,
    provider_id INT,
    location_id INT,
    visit_id INT,
    visit_date DATETIME,
    encounter_type_id INT,
    encounter_id INT NOT NULL,
    tb_diag INT,
    mdr_tb_diag INT,
    tb_new_diag INT,
    tb_class_pulmonary TINYINT(1),
    tb_class_extrapulmonary TINYINT(1),
    tb_extra_meningitis TINYINT(1),
    tb_extra_genital TINYINT(1),
    tb_extra_pleural TINYINT(1),
    tb_extra_miliary TINYINT(1),
    tb_extra_gangliponic TINYINT(1),
    tb_extra_intestinal TINYINT(1),
    tb_extra_other TINYINT(1),
    tb_follow_up_diag INT,
    cough_for_2wks_or_more INT,
    dyspnea TINYINT(1),
    tb_diag_sputum TINYINT(1),
    tb_diag_xray TINYINT(1),
    tb_test_result_mon_0 INT,
    tb_test_result_mon_2 INT,
    tb_test_result_mon_3 INT,
    tb_test_result_mon_5 INT,
    tb_test_result_end INT,
    age_at_visit_years INT,
    age_at_visit_months INT,
    tb_pulmonaire INT,
    tb_multiresistante INT,
    tb_extrapul_ou_diss INT,
    tb_treatment_start_date DATE,
    tb_started_treatment TINYINT(1),
    tb_medication_provided TINYINT(1),
    status_tb_treatment INT DEFAULT 0,
    tb_hiv_test_result TINYINT(1),
    tb_prophy_cotrimoxazole TINYINT(1),
    on_arv TINYINT(1),
    tb_treatment_stop_date DATE,
    last_updated_date DATETIME,
    voided TINYINT(1),
    PRIMARY KEY (encounter_id, location_id),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(patient_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -------------------------------------------------------------------------
-- patient_dispensing (lines 99-120 + voided 783, obs_id 851,
-- obs_group_id 855, treatment_regime_lines 1101, pills_amount->double 1087)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS patient_dispensing;
CREATE TABLE patient_dispensing (
    patient_id INT NOT NULL,
    visit_id INT,
    location_id INT,
    obs_id INT,
    obs_group_id INT,
    visit_date DATETIME,
    encounter_id INT NOT NULL,
    provider_id INT,
    drug_id INT NOT NULL,
    dose_day INT,
    pills_amount DOUBLE,
    dispensation_date DATE,
    next_dispensation_date DATE,
    dispensation_location INT DEFAULT 0,
    arv_drug INT DEFAULT 1066,
    rx_or_prophy INT,
    ddp INT DEFAULT NULL,
    treatment_regime_lines TEXT,
    last_updated_date DATETIME,
    voided TINYINT(1),
    PRIMARY KEY (encounter_id, location_id, drug_id),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(patient_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -------------------------------------------------------------------------
-- patient_imagerie (lines 123-138 + voided 785)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS patient_imagerie;
CREATE TABLE patient_imagerie (
    patient_id INT NOT NULL,
    location_id INT,
    visit_id INT NOT NULL,
    encounter_id INT NOT NULL,
    visit_date DATETIME,
    radiographie_pul INT DEFAULT 0,
    radiographie_autre INT,
    crachat_barr INT,
    last_updated_date DATETIME,
    voided TINYINT(1),
    PRIMARY KEY (location_id, encounter_id),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(patient_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -------------------------------------------------------------------------
-- arv_drugs (lines 140-170)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS arv_drugs;
CREATE TABLE arv_drugs (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    drug_id INT NOT NULL UNIQUE,
    drug_name LONGTEXT NOT NULL,
    date_inserted DATE NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -------------------------------------------------------------------------
-- patient_on_arv (lines 197-204 + voided ALTER 788)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS patient_on_arv;
CREATE TABLE patient_on_arv (
    patient_id INT,
    visit_id INT,
    visit_date DATE,
    last_updated_date DATETIME,
    voided TINYINT(1),
    PRIMARY KEY (patient_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -------------------------------------------------------------------------
-- discontinuation_reason (lines 212-221)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS discontinuation_reason;
CREATE TABLE discontinuation_reason (
    patient_id INT,
    visit_id INT,
    visit_date DATE,
    reason INT,
    reason_name LONGTEXT,
    last_updated_date DATETIME,
    PRIMARY KEY (patient_id, visit_id, reason)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -------------------------------------------------------------------------
-- stopping_reason (lines 228-238)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS stopping_reason;
CREATE TABLE stopping_reason (
    patient_id INT,
    visit_id INT,
    visit_date DATE,
    reason INT,
    reason_name LONGTEXT,
    other_reason LONGTEXT,
    last_updated_date DATETIME,
    PRIMARY KEY (patient_id, visit_id, reason)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -------------------------------------------------------------------------
-- patient_status_arv (lines 240-249 + ALTERs 831, 834, 864-866)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS patient_status_arv;
CREATE TABLE patient_status_arv (
    patient_id INT,
    id_status INT,
    start_date DATE,
    encounter_id INT,
    end_date DATE,
    dis_reason INT,
    last_updated_date DATETIME,
    date_started_status DATETIME,
    PRIMARY KEY (patient_id, id_status, start_date, date_started_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -------------------------------------------------------------------------
-- patient_prescription (lines 253-273 + voided 791, dispensation_date 837,
-- number_day_dispense 840, pills_amount_dispense 843, obs_id 858,
-- obs_group_id 862, posology_alt 1134, posology_alt_disp 1137,
-- number_day->double 1219)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS patient_prescription;
CREATE TABLE patient_prescription (
    patient_id INT NOT NULL,
    visit_id INT,
    location_id INT,
    obs_id INT,
    obs_group_id INT,
    visit_date DATETIME,
    encounter_id INT NOT NULL,
    provider_id INT,
    drug_id INT NOT NULL,
    dispensation_date DATE,
    next_dispensation_date DATE,
    dispensation_location INT DEFAULT 0,
    arv_drug INT DEFAULT 1066,
    dispense INT,
    rx_or_prophy INT,
    posology TEXT,
    posology_alt TEXT,
    posology_alt_disp TEXT,
    number_day DOUBLE,
    number_day_dispense INT,
    pills_amount_dispense DOUBLE,
    last_updated_date DATETIME,
    voided TINYINT(1),
    PRIMARY KEY (encounter_id, location_id, drug_id),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(patient_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -------------------------------------------------------------------------
-- patient_laboratory (lines 277-296 + voided 794,
-- viral_load_target_or_routine 1097)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS patient_laboratory;
CREATE TABLE patient_laboratory (
    patient_id INT NOT NULL,
    visit_id INT,
    location_id INT,
    visit_date DATETIME,
    encounter_id INT NOT NULL,
    provider_id INT,
    test_id INT NOT NULL,
    test_done INT DEFAULT 0,
    test_result TEXT,
    date_test_done DATE,
    comment_test_done TEXT,
    viral_load_target_or_routine INT,
    order_destination VARCHAR(50),
    test_name TEXT,
    creation_date DATETIME,
    last_updated_date DATETIME,
    voided TINYINT(1),
    PRIMARY KEY (patient_id, encounter_id, test_id),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(patient_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -------------------------------------------------------------------------
-- patient_pregnancy (lines 299-305 + voided ALTER 797)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS patient_pregnancy;
CREATE TABLE patient_pregnancy (
    patient_id INT,
    encounter_id INT,
    start_date DATE,
    end_date DATE,
    last_updated_date DATETIME,
    voided TINYINT(1),
    PRIMARY KEY (patient_id, encounter_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -------------------------------------------------------------------------
-- alert_lookup (lines 308-342)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS alert_lookup;
CREATE TABLE alert_lookup (
    id INT PRIMARY KEY AUTO_INCREMENT,
    message_fr TEXT,
    message_en TEXT,
    libelle TEXT,
    insert_date DATE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -------------------------------------------------------------------------
-- alert (lines 347-354)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS alert;
CREATE TABLE alert (
    id INT PRIMARY KEY AUTO_INCREMENT,
    patient_id INT,
    id_alert INT,
    encounter_id INT,
    date_alert DATE,
    last_updated_date DATETIME
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -------------------------------------------------------------------------
-- patient_diagnosis (lines 358-372 + voided ALTER 800)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS patient_diagnosis;
CREATE TABLE patient_diagnosis (
    patient_id INT,
    encounter_id INT,
    location_id INT,
    encounter_date DATE,
    concept_group INT,
    obs_group_id INT,
    concept_id INT,
    answer_concept_id INT,
    suspected_confirmed INT,
    primary_secondary INT,
    last_updated_date DATETIME,
    voided TINYINT(1),
    PRIMARY KEY (encounter_id, location_id, concept_group, concept_id, answer_concept_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -------------------------------------------------------------------------
-- visit_type (lines 377-388 + voided ALTER 803)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS visit_type;
CREATE TABLE visit_type (
    patient_id INT,
    encounter_id INT,
    location_id INT,
    visit_id INT,
    obs_group INT DEFAULT 0,
    concept_id INT,
    v_type INT,
    encounter_date DATE,
    last_updated_date DATETIME,
    voided TINYINT(1),
    PRIMARY KEY (encounter_id, location_id, obs_group, concept_id, v_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -------------------------------------------------------------------------
-- virological_tests (lines 392-406 + voided ALTER 806)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS virological_tests;
CREATE TABLE virological_tests (
    patient_id INT,
    encounter_id INT,
    location_id INT,
    encounter_date DATE,
    concept_group INT,
    obs_group_id INT,
    test_id INT,
    answer_concept_id INT,
    test_result INT,
    age INT,
    age_unit INT,
    test_date DATE,
    last_updated_date DATETIME,
    voided TINYINT(1),
    PRIMARY KEY (encounter_id, location_id, obs_group_id, test_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -------------------------------------------------------------------------
-- patient_delivery (lines 410-422 + voided ALTER 809)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS patient_delivery;
CREATE TABLE patient_delivery (
    patient_id INT,
    encounter_id INT,
    location_id INT,
    delivery_date DATETIME,
    delivery_location INT,
    vaginal INT,
    forceps INT,
    vacuum INT,
    delivrance INT,
    encounter_date DATE,
    last_updated_date DATETIME,
    voided TINYINT(1),
    PRIMARY KEY (encounter_id, location_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -------------------------------------------------------------------------
-- pediatric_hiv_visit (lines 424-433 + voided ALTER 812)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS pediatric_hiv_visit;
CREATE TABLE pediatric_hiv_visit (
    patient_id INT,
    encounter_id INT,
    location_id INT,
    ptme INT,
    prophylaxie72h INT,
    actual_vih_status INT,
    encounter_date DATE,
    voided TINYINT(1),
    PRIMARY KEY (patient_id, encounter_id, location_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -------------------------------------------------------------------------
-- patient_menstruation (lines 437-446 + voided ALTER 815)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS patient_menstruation;
CREATE TABLE patient_menstruation (
    patient_id INT,
    encounter_id INT,
    location_id INT,
    duree_regle INT,
    duree_cycle INT,
    ddr DATE,
    encounter_date DATE,
    last_updated_date DATETIME,
    voided TINYINT(1),
    PRIMARY KEY (patient_id, encounter_id, location_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -------------------------------------------------------------------------
-- vih_risk_factor (lines 449-457 + voided ALTER 818)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS vih_risk_factor;
CREATE TABLE vih_risk_factor (
    patient_id INT,
    encounter_id INT,
    location_id INT,
    risk_factor INT,
    encounter_date DATE,
    last_updated_date DATETIME,
    voided TINYINT(1),
    PRIMARY KEY (patient_id, encounter_id, location_id, risk_factor)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -------------------------------------------------------------------------
-- vaccination (lines 460-468 + voided ALTER 821)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS vaccination;
CREATE TABLE vaccination (
    patient_id INT,
    encounter_id INT,
    encounter_date DATE,
    location_id INT,
    age_range INT,
    vaccination_done BOOLEAN DEFAULT FALSE,
    voided TINYINT(1),
    PRIMARY KEY (patient_id, encounter_id, location_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -------------------------------------------------------------------------
-- health_qual_patient_visit (lines 472-488 + voided ALTER 824)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS health_qual_patient_visit;
CREATE TABLE health_qual_patient_visit (
    patient_id INT,
    encounter_id INT,
    visit_date DATE,
    visit_id INT,
    location_id INT,
    encounter_type INT DEFAULT NULL,
    patient_bmi DOUBLE DEFAULT NULL,
    adherence_evaluation INT DEFAULT NULL,
    family_planning_method_used BOOLEAN DEFAULT FALSE,
    evaluated_of_tb BOOLEAN DEFAULT FALSE,
    nutritional_assessment_completed BOOLEAN DEFAULT FALSE,
    is_active_tb BOOLEAN DEFAULT FALSE,
    age_in_years INT,
    last_insert_date DATE DEFAULT NULL,
    last_updated_date DATETIME,
    voided TINYINT(1),
    PRIMARY KEY (patient_id, encounter_id, location_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -------------------------------------------------------------------------
-- exposed_infants (lines 492-499)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS exposed_infants;
CREATE TABLE exposed_infants (
    patient_id INT,
    location_id INT,
    encounter_id INT,
    visit_date DATE,
    condition_exposee INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -------------------------------------------------------------------------
-- serological_tests (lines 501-516 + voided ALTER 827)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS serological_tests;
CREATE TABLE serological_tests (
    patient_id INT,
    encounter_id INT,
    location_id INT,
    encounter_date DATE,
    concept_group INT,
    obs_group_id INT,
    test_id INT,
    answer_concept_id INT,
    test_result INT,
    age INT,
    age_unit INT,
    test_date DATE,
    last_updated_date DATETIME,
    voided TINYINT(1),
    PRIMARY KEY (encounter_id, location_id, obs_group_id, test_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -------------------------------------------------------------------------
-- patient_pcr (lines 519-528)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS patient_pcr;
CREATE TABLE patient_pcr (
    patient_id INT,
    encounter_id INT,
    location_id INT,
    visit_date DATE,
    pcr_result INT,
    test_date DATE,
    last_updated_date DATETIME
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -------------------------------------------------------------------------
-- regimen (lines 531-541 — data inserted separately by ETL setup)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS regimen;
CREATE TABLE regimen (
    regID INT PRIMARY KEY,
    regimenName VARCHAR(255),
    drugID1 INT,
    drugID2 INT,
    drugID3 INT,
    shortName VARCHAR(255) NOT NULL,
    regGroup VARCHAR(255)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -------------------------------------------------------------------------
-- pepfarTable (lines 543-551)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS pepfarTable;
CREATE TABLE pepfarTable (
    location_id INT,
    patient_id INT,
    visit_date DATE,
    regimen VARCHAR(255),
    rx_or_prophy INT,
    last_updated_date DATETIME,
    PRIMARY KEY (location_id, patient_id, visit_date, regimen)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -------------------------------------------------------------------------
-- patient_malaria (lines 690-701 + ALTERs 704-754)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS patient_malaria;
CREATE TABLE patient_malaria (
    patient_id INT NOT NULL,
    location_id INT NOT NULL,
    visit_id INT NOT NULL,
    visit_date DATE NOT NULL,
    encounter_id INT NOT NULL,
    encounter_type_id INT NOT NULL,
    fever_for_less_than_2wks TINYINT(1),
    suspected_malaria TINYINT(1),
    confirmed_malaria TINYINT(1),
    treated_with_chloroquine TINYINT(1),
    treated_with_primaquine TINYINT(1),
    treated_with_quinine TINYINT(1),
    microscopic_test TINYINT(1),
    positive_microscopic_test_result TINYINT(1),
    negative_microscopic_test_result TINYINT(1),
    positive_plasmodium_falciparum_test_result TINYINT(1),
    mixed_positive_test_result TINYINT(1),
    positive_plasmodium_vivax_test_result TINYINT(1),
    rapid_test TINYINT(1),
    positve_rapid_test_result TINYINT(1),
    severe_malaria TINYINT(1),
    hospitallized TINYINT(1),
    confirmed_malaria_preganancy TINYINT(1),
    last_updated_date DATE NOT NULL,
    voided TINYINT(1) NOT NULL DEFAULT 0,
    PRIMARY KEY (patient_id)
) ENGINE=INNODB DEFAULT CHARSET=utf8;

-- -------------------------------------------------------------------------
-- patient_nutrition (lines 962-984)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS patient_nutrition;
CREATE TABLE patient_nutrition (
    patient_id INT NOT NULL,
    location_id INT,
    visit_id INT,
    visit_date DATE,
    encounter_id INT NOT NULL,
    encounter_type_id INT NOT NULL,
    age_at_visit_years INT,
    age_at_visit_months INT,
    weight DOUBLE,
    height DOUBLE,
    bmi DOUBLE,
    bmi_for_age INT,
    weight_for_height INT,
    edema TINYINT(1),
    last_updated_date DATETIME,
    voided TINYINT(1),
    PRIMARY KEY (encounter_id, location_id),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(patient_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -------------------------------------------------------------------------
-- patient_ob_gyn (lines 988-1018)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS patient_ob_gyn;
CREATE TABLE patient_ob_gyn (
    patient_id INT NOT NULL,
    location_id INT,
    visit_id INT,
    visit_date DATE,
    encounter_id INT NOT NULL,
    encounter_type_id INT NOT NULL,
    muac INT,
    pregnant INT(1),
    next_visit_date DATE,
    edd DATE,
    birth_plan INT(1),
    high_risk INT(1),
    gestation_greater_than_12_wks INT(1),
    iron_supplement INT(1),
    folic_acid_supplement INT(1),
    tetanus_toxoid_vaccine INT(1),
    iron_defiency_anemia INT(1),
    prescribed_iron INT(1),
    prescribed_folic_acid INT(1),
    elevated_blood_pressure INT(1),
    toxemia_signs INT(1),
    over_20_weeks_pregnancy INT(1),
    last_updated_date DATETIME,
    voided TINYINT(1),
    PRIMARY KEY (encounter_id, location_id),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(patient_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -------------------------------------------------------------------------
-- patient_on_art (lines 1024-1084)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS patient_on_art;
CREATE TABLE patient_on_art (
    patient_id INT,
    date_completed_preventive_tb_treatment DATETIME,
    enrolled_on_art INT DEFAULT NULL,
    gender VARCHAR(10) DEFAULT NULL,
    key_population VARCHAR(255),
    tested_hiv_postive INT DEFAULT NULL,
    date_tested_hiv_postive DATETIME,
    reason_non_enrollment VARCHAR(255),
    date_non_enrollment DATETIME,
    date_enrolled_on_tb_treatment DATETIME,
    transferred INT DEFAULT NULL,
    tb_screened INT DEFAULT NULL,
    date_tb_screened DATETIME,
    tb_status VARCHAR(10) DEFAULT NULL,
    tb_genexpert_test INT DEFAULT NULL,
    tb_other_test INT DEFAULT NULL,
    tb_crachat_test INT DEFAULT NULL,
    date_sample_sent_for_diagnositic_tb DATETIME,
    started_anti_tb_treatment INT DEFAULT NULL,
    date_started_anti_tb_treatment DATETIME,
    tb_bacteriological_test_status VARCHAR(10) DEFAULT NULL,
    lost INT DEFAULT NULL,
    date_inactive DATETIME,
    inactive_reason VARCHAR(20) DEFAULT NULL,
    inactive INT DEFAULT NULL,
    deceased INT DEFAULT NULL,
    receive_arv INT DEFAULT NULL,
    date_started_arv DATETIME,
    date_started_receiving_arv DATETIME,
    receive_clinical_followup INT DEFAULT NULL,
    treatment_regime_lines TEXT DEFAULT NULL,
    date_started_regime_treatment DATETIME,
    lost_reason VARCHAR(10) DEFAULT NULL,
    date_lost DATETIME,
    period_lost VARCHAR(10) DEFAULT NULL,
    cause_of_death_for_lost VARCHAR(10) DEFAULT NULL,
    viral_load_targeted INT DEFAULT NULL,
    viral_load_targeted_result INT DEFAULT NULL,
    resumed_arv_after_lost INT DEFAULT NULL,
    recomended_family_planning INT DEFAULT NULL,
    accepted_family_planning_method VARCHAR(10) DEFAULT NULL,
    date_accepted_family_planning_method DATETIME,
    using_family_planning_method VARCHAR(10) DEFAULT NULL,
    date_using_family_planning_method VARCHAR(10) DEFAULT NULL,
    first_vist_date DATETIME,
    second_last_folowup_vist_date DATETIME,
    last_folowup_vist_date DATETIME,
    date_started_arv_for_transfered DATETIME,
    screened_cervical_cancer INT DEFAULT NULL,
    date_screened_cervical_cancer DATETIME,
    cervical_cancer_status VARCHAR(10) DEFAULT NULL,
    date_started_cervical_cancer_status DATETIME,
    cervical_cancer_treatment VARCHAR(10) DEFAULT NULL,
    date_cervical_cancer_treatment DATETIME,
    breast_feeding INT DEFAULT NULL,
    date_breast_feeding DATETIME,
    date_started_breast_feeding DATETIME,
    date_full_6_months_of_inh_has_px DATETIME,
    migrated INT DEFAULT NULL,
    PRIMARY KEY (patient_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -------------------------------------------------------------------------
-- key_populations (lines 1104-1114)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS key_populations;
CREATE TABLE key_populations (
    patient_id INT,
    encounter_id INT,
    location_id INT,
    key_population INT,
    encounter_date DATETIME,
    voided TINYINT(1) NOT NULL DEFAULT 0,
    last_updated_date DATETIME,
    PRIMARY KEY (patient_id, encounter_id, key_population, voided)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -------------------------------------------------------------------------
-- family_planning (lines 1117-1128)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS family_planning;
CREATE TABLE family_planning (
    patient_id INT,
    encounter_id INT,
    location_id INT,
    planning INT,
    encounter_date DATETIME,
    family_planning_method_name TEXT,
    accepting_or_using_fp INT,
    voided TINYINT(1) NOT NULL DEFAULT 0,
    last_updated_date DATETIME,
    PRIMARY KEY (patient_id, encounter_id, planning, voided)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -------------------------------------------------------------------------
-- patient_immunization (lines 1140-1154)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS patient_immunization;
CREATE TABLE patient_immunization (
    patient_id INT NOT NULL,
    location_id INT NOT NULL,
    encounter_id INT NOT NULL,
    vaccine_obs_group_id INT,
    vaccine_concept_id INT NOT NULL,
    dose DOUBLE,
    vaccine_date DATETIME,
    encounter_date DATETIME,
    lot_number TEXT,
    manufacturer TEXT,
    vaccine_uuid VARCHAR(255),
    voided TINYINT(1) NOT NULL DEFAULT 0,
    PRIMARY KEY (patient_id, vaccine_obs_group_id, vaccine_concept_id)
) ENGINE=INNODB DEFAULT CHARSET=utf8;

-- -------------------------------------------------------------------------
-- immunization_dose (lines 1204-1217)
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS immunization_dose;
CREATE TABLE immunization_dose (
    patient_id INT NOT NULL,
    vaccine_concept_id INT NOT NULL,
    dose0 DATETIME,
    dose1 DATETIME,
    dose2 DATETIME,
    dose3 DATETIME,
    dose4 DATETIME,
    dose5 DATETIME,
    dose6 DATETIME,
    dose7 DATETIME,
    dose8 DATETIME,
    PRIMARY KEY (patient_id, vaccine_concept_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

SET FOREIGN_KEY_CHECKS = 1;
"""
        with open(filepath, 'w') as f:
            f.write(ddl)
        print("  iSantePlus DDL written successfully")


# =============================================================================
# DATABASE OUTPUT
# =============================================================================

class DatabaseWriter:
    """Writes generated data directly to a MySQL database."""

    def __init__(
        self,
        generator: TestDataGenerator,
        host: str,
        user: str,
        password: str,
        port: int = 3306,
    ):
        if not HAS_MYSQL:
            raise RuntimeError(
                "mysql-connector-python is required for database mode. "
                "Install with: pip install mysql-connector-python"
            )
        self.gen = generator
        self.conn_params = {'host': host, 'user': user, 'password': password,
                            'port': port}
        self.batch_size = generator.config.batch_size

    def _get_connection(self, database: Optional[str] = None):
        params = self.conn_params.copy()
        if database:
            params['database'] = database
        return mysql.connector.connect(**params)

    def _execute_batch_insert(
        self, cursor, table: str, columns: List[str],
        rows: List[tuple], batch_size: Optional[int] = None
    ) -> None:
        if not rows:
            return
        batch_size = batch_size or self.batch_size
        placeholders = ', '.join(['%s'] * len(columns))
        sql = (f"INSERT INTO {table} ({', '.join(columns)}) "
               f"VALUES ({placeholders})")
        for i in range(0, len(rows), batch_size):
            cursor.executemany(sql, rows[i:i + batch_size])

    def write_openmrs_data(self) -> None:
        print("Writing OpenMRS data to database...")
        now = datetime.now()
        conn = self._get_connection('openmrs')
        cursor = conn.cursor()
        try:
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            cursor.execute("SET SQL_MODE = 'NO_AUTO_VALUE_ON_ZERO'")

            cursor.execute("TRUNCATE TABLE encounter_type")
            self._execute_batch_insert(
                cursor, 'encounter_type',
                ['encounter_type_id', 'name', 'uuid', 'creator', 'date_created', 'retired'],
                [(et['encounter_type_id'], et['name'], et['uuid'],
                  et['creator'], et['date_created'], 0)
                 for et in self.gen.encounter_types])

            cursor.execute("TRUNCATE TABLE concept")
            self._execute_batch_insert(
                cursor, 'concept', ['concept_id', 'uuid'],
                [(c['concept_id'], c['uuid']) for c in self.gen.concepts])

            cursor.execute("TRUNCATE TABLE concept_name")
            self._execute_batch_insert(
                cursor, 'concept_name',
                ['concept_name_id', 'concept_id', 'locale', 'name',
                 'concept_name_type', 'uuid'],
                [(cn['concept_name_id'], cn['concept_id'], cn['locale'],
                  cn['name'], cn['concept_name_type'], cn['uuid'])
                 for cn in self.gen.concept_names])

            cursor.execute("TRUNCATE TABLE location_attribute_type")
            self._execute_batch_insert(
                cursor, 'location_attribute_type',
                ['location_attribute_type_id', 'name', 'uuid', 'creator', 'date_created'],
                [(la['location_attribute_type_id'], la['name'], la['uuid'],
                  la['creator'], la['date_created'])
                 for la in self.gen.location_attr_types])

            cursor.execute("TRUNCATE TABLE location")
            self._execute_batch_insert(
                cursor, 'location',
                ['location_id', 'name', 'uuid', 'creator', 'date_created', 'retired'],
                [(loc['location_id'], loc['name'], loc['uuid'],
                  loc['creator'], loc['date_created'], loc['retired'])
                 for loc in self.gen.locations])

            cursor.execute("TRUNCATE TABLE location_attribute")
            self._execute_batch_insert(
                cursor, 'location_attribute',
                ['location_attribute_id', 'location_id', 'attribute_type_id',
                 'value_reference', 'uuid', 'creator', 'date_created'],
                [(la['location_attribute_id'], la['location_id'],
                  la['attribute_type_id'], la['value_reference'],
                  la['uuid'], la['creator'], la['date_created'])
                 for la in self.gen.location_attributes])

            cursor.execute("TRUNCATE TABLE patient_identifier_type")
            self._execute_batch_insert(
                cursor, 'patient_identifier_type',
                ['patient_identifier_type_id', 'name', 'uuid', 'creator',
                 'date_created', 'required'],
                [(it['patient_identifier_type_id'], it['name'], it['uuid'],
                  it['creator'], it['date_created'], it['required'])
                 for it in self.gen.identifier_types])

            cursor.execute("TRUNCATE TABLE person_attribute_type")
            self._execute_batch_insert(
                cursor, 'person_attribute_type',
                ['person_attribute_type_id', 'name', 'uuid', 'creator', 'date_created'],
                [(pt['person_attribute_type_id'], pt['name'], pt['uuid'],
                  pt['creator'], pt['date_created'])
                 for pt in self.gen.person_attr_types])

            print("  Writing person records...")
            cursor.execute("TRUNCATE TABLE person")
            self._execute_batch_insert(
                cursor, 'person',
                ['person_id', 'gender', 'birthdate', 'creator',
                 'date_created', 'voided', 'uuid'],
                [(p.person_id, p.gender, p.birthdate, 1, now, 0, p.person_uuid)
                 for p in self.gen.patients])

            cursor.execute("TRUNCATE TABLE person_name")
            self._execute_batch_insert(
                cursor, 'person_name',
                ['person_name_id', 'person_id', 'given_name', 'family_name',
                 'preferred', 'creator', 'date_created', 'voided', 'uuid'],
                [(pn['person_name_id'], pn['person_id'], pn['given_name'],
                  pn['family_name'], pn['preferred'], pn['creator'],
                  pn['date_created'], pn['voided'], pn['uuid'])
                 for pn in self.gen.person_names])

            cursor.execute("TRUNCATE TABLE person_address")
            self._execute_batch_insert(
                cursor, 'person_address',
                ['person_address_id', 'person_id', 'address1', 'address2',
                 'preferred', 'creator', 'date_created', 'voided', 'uuid'],
                [(pa['person_address_id'], pa['person_id'], pa['address1'],
                  pa['address2'], pa['preferred'], pa['creator'],
                  pa['date_created'], pa['voided'], pa['uuid'])
                 for pa in self.gen.person_addresses])

            cursor.execute("TRUNCATE TABLE person_attribute")
            self._execute_batch_insert(
                cursor, 'person_attribute',
                ['person_attribute_id', 'person_id', 'value',
                 'person_attribute_type_id', 'creator', 'date_created',
                 'voided', 'uuid'],
                [(pa['person_attribute_id'], pa['person_id'], pa['value'],
                  pa['person_attribute_type_id'], pa['creator'],
                  pa['date_created'], pa['voided'], pa['uuid'])
                 for pa in self.gen.person_attributes])

            cursor.execute("TRUNCATE TABLE patient")
            self._execute_batch_insert(
                cursor, 'patient',
                ['patient_id', 'creator', 'date_created', 'voided'],
                [(p.patient_id, 1, now, 0) for p in self.gen.patients])

            cursor.execute("TRUNCATE TABLE patient_identifier")
            self._execute_batch_insert(
                cursor, 'patient_identifier',
                ['patient_identifier_id', 'patient_id', 'identifier',
                 'identifier_type', 'location_id', 'preferred', 'creator',
                 'date_created', 'voided', 'uuid'],
                [(pi['patient_identifier_id'], pi['patient_id'],
                  pi['identifier'], pi['identifier_type'], pi['location_id'],
                  pi['preferred'],
                  pi['creator'], pi['date_created'], pi['voided'], pi['uuid'])
                 for pi in self.gen.patient_identifiers])

            print("  Writing visit/encounter records...")
            cursor.execute("TRUNCATE TABLE visit")
            self._execute_batch_insert(
                cursor, 'visit',
                ['visit_id', 'patient_id', 'date_started', 'date_stopped',
                 'location_id', 'creator', 'date_created', 'voided', 'uuid'],
                [(v.visit_id, v.patient_id, v.date_started, v.date_stopped,
                  v.location_id, 1, now, 0, v.uuid) for v in self.gen.visits])

            cursor.execute("TRUNCATE TABLE encounter")
            self._execute_batch_insert(
                cursor, 'encounter',
                ['encounter_id', 'encounter_type', 'patient_id', 'location_id',
                 'form_id', 'encounter_datetime', 'visit_id', 'creator',
                 'date_created', 'voided', 'uuid'],
                [(e.encounter_id, e.encounter_type_id, e.patient_id,
                  e.location_id, e.form_id, e.encounter_datetime, e.visit_id,
                  1, now, 0, e.uuid) for e in self.gen.encounters])

            cursor.execute("TRUNCATE TABLE encounter_provider")
            self._execute_batch_insert(
                cursor, 'encounter_provider',
                ['encounter_provider_id', 'encounter_id', 'provider_id',
                 'encounter_role_id', 'creator', 'date_created', 'voided',
                 'uuid'],
                [(ep['encounter_provider_id'], ep['encounter_id'],
                  ep['provider_id'], ep['encounter_role_id'], ep['creator'],
                  ep['date_created'], ep['voided'], ep['uuid'])
                 for ep in self.gen.encounter_providers])

            print(f"  Writing {len(self.gen.observations)} observation records...")
            cursor.execute("TRUNCATE TABLE obs")
            self._execute_batch_insert(
                cursor, 'obs',
                ['obs_id', 'person_id', 'encounter_id', 'concept_id',
                 'obs_datetime', 'location_id', 'value_coded',
                 'value_numeric', 'value_datetime', 'value_text',
                 'obs_group_id', 'creator', 'date_created', 'voided', 'uuid'],
                [(o.obs_id, o.person_id, o.encounter_id, o.concept_id,
                  o.obs_datetime, o.location_id, o.value_coded,
                  o.value_numeric, o.value_datetime, o.value_text,
                  o.obs_group_id, 1, now, 0, o.uuid)
                 for o in self.gen.observations])

            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            conn.commit()
            print("  OpenMRS data written successfully")
        except MySQLError as e:
            conn.rollback()
            raise RuntimeError(f"Failed to write OpenMRS data: {e}")
        finally:
            cursor.close()
            conn.close()

    def write_isanteplus_data(self) -> None:
        print("Writing iSantePlus data to database...")
        conn = self._get_connection('isanteplus')
        cursor = conn.cursor()
        try:
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0")

            cursor.execute("TRUNCATE TABLE patient")
            self._execute_batch_insert(
                cursor, 'patient',
                ['patient_id', 'location_id', 'vih_status',
                 'date_started_arv', 'voided'],
                [(p['patient_id'], p['location_id'], p['vih_status'],
                  p['date_started_arv'],
                  p['voided']) for p in self.gen.isanteplus_patients])

            cursor.execute("TRUNCATE TABLE patient_on_arv")
            self._execute_batch_insert(
                cursor, 'patient_on_arv', ['patient_id'],
                [(pid,) for pid in self.gen.patient_on_arv])

            cursor.execute("TRUNCATE TABLE discontinuation_reason")
            self._execute_batch_insert(
                cursor, 'discontinuation_reason',
                ['patient_id', 'reason', 'visit_date', 'visit_id'],
                [(dr['patient_id'], dr['reason'], dr['visit_date'],
                  dr.get('visit_id'))
                 for dr in self.gen.discontinuation_reasons])

            cursor.execute("TRUNCATE TABLE patient_dispensing")
            self._execute_batch_insert(
                cursor, 'patient_dispensing',
                ['patient_id', 'encounter_id', 'visit_id', 'visit_date',
                 'next_dispensation_date', 'arv_drug', 'rx_or_prophy',
                 'drug_id', 'voided', 'location_id'],
                [(pd['patient_id'], pd['encounter_id'], pd['visit_id'],
                  pd['visit_date'], pd['next_dispensation_date'],
                  pd['arv_drug'], pd['rx_or_prophy'], pd['drug_id'],
                  pd['voided'], pd['location_id'])
                 for pd in self.gen.patient_dispensing])

            cursor.execute("TRUNCATE TABLE patient_prescription")
            self._execute_batch_insert(
                cursor, 'patient_prescription',
                ['patient_id', 'encounter_id', 'location_id', 'visit_date',
                 'drug_id', 'arv_drug', 'rx_or_prophy', 'voided'],
                [(pp['patient_id'], pp['encounter_id'], pp['location_id'],
                  pp['visit_date'], pp['drug_id'], pp['arv_drug'],
                  pp['rx_or_prophy'], pp['voided'])
                 for pp in self.gen.patient_prescription])

            cursor.execute("TRUNCATE TABLE patient_laboratory")
            self._execute_batch_insert(
                cursor, 'patient_laboratory',
                ['patient_id', 'encounter_id', 'location_id', 'test_id',
                 'test_done', 'test_result', 'visit_date', 'date_test_done',
                 'voided'],
                [(pl['patient_id'], pl['encounter_id'], pl['location_id'],
                  pl['test_id'], pl['test_done'], pl['test_result'],
                  pl['visit_date'], pl['date_test_done'], pl['voided'])
                 for pl in self.gen.patient_laboratory])

            cursor.execute("TRUNCATE TABLE patient_pregnancy")
            self._execute_batch_insert(
                cursor, 'patient_pregnancy',
                ['patient_id', 'encounter_id', 'start_date', 'voided'],
                [(pp['patient_id'], pp['encounter_id'], pp['start_date'],
                  pp['voided'])
                 for pp in self.gen.patient_pregnancy_records])

            # Clear destination tables
            for table in [
                'alert', 'visit_type', 'patient_visit', 'patient_delivery',
                'virological_tests', 'pediatric_hiv_visit',
                'patient_menstruation', 'vih_risk_factor', 'vaccination',
                'serological_tests', 'patient_pcr', 'patient_malaria',
                'patient_on_art', 'key_populations', 'family_planning',
                'patient_tb_diagnosis', 'patient_nutrition', 'patient_ob_gyn',
                'patient_imagerie', 'stopping_reason',
                'health_qual_patient_visit', 'regimen', 'pepfarTable',
                'patient_immunization', 'immunization_dose',
            ]:
                cursor.execute(f"TRUNCATE TABLE {table}")

            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            conn.commit()
            print("  iSantePlus data written successfully")
        except MySQLError as e:
            conn.rollback()
            raise RuntimeError(f"Failed to write iSantePlus data: {e}")
        finally:
            cursor.close()
            conn.close()


# =============================================================================
# MAIN
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description='Generate test data for iSantePlus reports ETL script',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument('--patients', '-n', type=int, default=100000,
                        help='Number of patients to generate (default: 100000)')
    parser.add_argument('--seed', '-s', type=int, default=None,
                        help='Random seed for reproducibility (default: random)')
    parser.add_argument('--batch-size', '-b', type=int, default=10000,
                        help='Batch size for database inserts (default: 10000)')

    db_group = parser.add_argument_group('Database connection')
    db_group.add_argument('--host', '-H',
                          help='MySQL host (required for database mode)')
    db_group.add_argument('--port', '-P', type=int, default=3306,
                          help='MySQL port (default: 3306)')
    db_group.add_argument('--user', '-u', help='MySQL username')
    db_group.add_argument('--password', '-p', help='MySQL password')

    out_group = parser.add_argument_group('Output options')
    out_group.add_argument('--sql-output', '-o', action='store_true',
                           help='Generate SQL data files (INSERT statements)')
    out_group.add_argument('--ddl-output', action='store_true',
                           help='Generate DDL files (CREATE TABLE statements)')
    out_group.add_argument('--output-dir', '-d', default='.',
                           help='Directory for output files (default: .)')
    return parser.parse_args()


def main():
    args = parse_args()

    db_mode  = args.host is not None
    sql_mode = args.sql_output
    ddl_mode = args.ddl_output

    if not db_mode and not sql_mode and not ddl_mode:
        print("Error: Must specify at least one of:")
        print("  --ddl-output     Generate DDL (CREATE TABLE statements)")
        print("  --sql-output     Generate test data (INSERT statements)")
        print("  --host           Direct database connection")
        sys.exit(1)

    if db_mode and not HAS_MYSQL:
        print("Error: mysql-connector-python is required for database mode.")
        print("Install with: pip install mysql-connector-python")
        sys.exit(1)

    if db_mode and (not args.user or not args.password):
        print("Error: Database mode requires --user and --password")
        sys.exit(1)

    if ddl_mode:
        ddl_writer = DDLWriter(args.output_dir)
        ddl_writer.write_openmrs_ddl()
        ddl_writer.write_isanteplus_ddl()

    if sql_mode or db_mode:
        config = GeneratorConfig(
            num_patients=args.patients,
            seed=args.seed,
            batch_size=args.batch_size,
        )
        generator = TestDataGenerator(config)
        generator.generate()

        if sql_mode:
            sql_writer = SQLWriter(generator, args.output_dir)
            sql_writer.write_openmrs_data()
            sql_writer.write_isanteplus_data()

        if db_mode:
            db_writer = DatabaseWriter(
                generator,
                host=args.host,
                user=args.user,
                password=args.password,
                port=args.port,
            )
            db_writer.write_openmrs_data()
            db_writer.write_isanteplus_data()

    print("\nGeneration complete!")
    if ddl_mode:
        print("\nDDL files generated:")
        print(f"  - {args.output_dir}/ddl_reports_openmrs.sql")
        print(f"  - {args.output_dir}/ddl_reports_isanteplus.sql")
    if sql_mode or db_mode:
        print("\nTo test the ETL script:")
        print("  1. Run DDL files first to create tables (if needed)")
        print("  2. Run data files or use database mode to populate test data")
        print("  3. Execute: SOURCE sql_files/isanteplusreportsdmlscript.sql")
        print("  4. Query all isanteplus.* destination tables for results")


if __name__ == '__main__':
    main()
