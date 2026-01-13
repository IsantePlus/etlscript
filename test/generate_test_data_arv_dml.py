#!/usr/bin/env python3
"""
Test Data Generator for iSantePlus ETL Stored Procedures

Generates synthetic test data for the patient_status_arv and alert_viral_load
stored procedures. Creates data for both openmrs.* and isanteplus.* schemas.

Usage:
    # Direct database insert:
    python generate_test_data_arv_dml.py --host localhost --user root --password secret --patients 100000

    # Generate SQL files:
    python generate_test_data_arv_dml.py --sql-output --patients 100000

    # Generate DDL (CREATE TABLE statements) only:
    python generate_test_data_arv_dml.py --ddl-output

    # Generate both DDL and test data:
    python generate_test_data_arv_dml.py --ddl-output --sql-output --patients 100000

    # All modes:
    python generate_test_data_arv_dml.py --host localhost --user root --password secret --ddl-output --sql-output --patients 100000
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
# CONSTANTS - Concept IDs and UUIDs used by the stored procedures
# =============================================================================

# Encounter type UUIDs (must match what procedures expect)
ENCOUNTER_TYPES = {
    'pediatric': '349ae0b4-65c1-4122-aa06-480f186c8350',
    'lab': 'f037e97b-471e-4898-a07c-b8e169e0ddc4',
    'discontinuation': '9d0113c6-f23a-4461-8428-7e9a7344f2ba',
    'pediatric_followup': '33491314-c352-42d0-bd5d-a9d0bffc9bf1',
    'first_visit': '17536ba6-dd7c-4f58-8014-08c7cb798ac7',
    'followup': '204ad066-c5c2-4229-9a62-644bc5617ca2',
    'dispensing1': '10d73929-54b6-4d18-a647-8b7316bc1ae3',
    'dispensing2': 'a9392241-109f-4d67-885b-57cc4b8c638f',
}

# Concept UUIDs for specific lookups
CONCEPT_UUIDS = {
    'isoniazid_group': 'fee8bd39-2a95-47f9-b1f5-3f9e9b3ee959',
    'rifampicin_group': '2b2053bd-37f3-429d-be0b-f1f8952fe55e',
    'ddp': 'c2aacdc8-156e-4527-8934-a8fb94162419',
}

# Concept IDs used in queries
class ConceptID(IntEnum):
    # PCR tests
    PCR_TEST_1 = 1030
    PCR_TEST_2 = 844

    # Viral load
    VIRAL_LOAD_NUMERIC = 856
    VIRAL_LOAD_CODED = 1305

    # Test results
    NEGATIVE = 664
    NEGATIVE_ALT = 1302
    POSITIVE = 703
    POSITIVE_CODED = 1301
    SUPPRESSED = 1306

    # HIV test
    HIV_TEST = 1040
    HIV_CONFIRMED_SEROLOGICAL = 163717

    # Discontinuation
    DISCONTINUATION_REASON = 161555
    DISCONTINUATION_SUB = 1667
    DECEASED = 159
    TRANSFERRED = 159492
    STOPPED_REASON_1 = 115198
    STOPPED_REASON_2 = 159737
    SEROREVERSION = 165439

    # Exposed infants
    CONDITION_CHECKBOX = 1401
    EXPOSED_INFANT = 1405

    # ARV/Drug related
    ARV_DRUG = 1065
    RX_TREATMENT = 138405
    RX_PROPHYLAXIS = 163768
    DRUG_PRESCRIBED = 1282
    ISONIAZID = 78280
    RIFAMPICIN = 767
    DRUG_GIVEN = 159367

    # Immunization (for obs groups)
    IMMUNIZATION_GROUP = 1421
    IMMUNIZATION_GIVEN = 984
    IMMUNIZATION_SEQUENCE = 1418
    IMMUNIZATION_DATE = 1410


# Discontinuation reasons for tmp_discontinued_patients
DISCONTINUATION_REASONS = [ConceptID.DECEASED, ConceptID.DISCONTINUATION_SUB, ConceptID.TRANSFERRED]


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class GeneratorConfig:
    """Configuration for the data generator."""
    num_patients: int = 100000
    start_date: datetime = None
    end_date: datetime = None
    seed: int = None  # None means generate a random seed
    batch_size: int = 10000

    # Distribution percentages
    pct_on_arv: float = 0.60          # 60% of HIV+ patients on ARV
    pct_hiv_positive: float = 0.70    # 70% of patients are HIV+
    pct_discontinued: float = 0.10    # 10% discontinued
    pct_with_viral_load: float = 0.50 # 50% have viral load results
    pct_pregnant: float = 0.05        # 5% of female patients pregnant
    pct_pediatric: float = 0.15       # 15% are pediatric patients
    pct_exposed_infant: float = 0.03  # 3% are exposed infants
    pct_tb_coinfection: float = 0.05  # 5% of ARV patients have TB co-infection

    def __post_init__(self):
        if self.start_date is None:
            self.start_date = datetime.now() - timedelta(days=5*365)  # 5 years ago
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
    is_exposed_infant: bool
    date_started_arv: Optional[datetime]
    location_id: int
    visits: List['Visit'] = field(default_factory=list)
    first_visit_date: Optional[datetime] = None
    person_uuid: str = field(default_factory=_generate_uuid)

    @property
    def person_id(self) -> int:
        """Person ID is same as patient ID in OpenMRS."""
        return self.patient_id


@dataclass
class Visit:
    """Represents a visit with encounters."""
    visit_id: int
    patient_id: int
    date_started: datetime
    location_id: int
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
    observations: List['Observation'] = field(default_factory=list)
    uuid: str = field(default_factory=_generate_uuid)


@dataclass
class Observation:
    """Represents an observation."""
    obs_id: int
    person_id: int
    encounter_id: int
    concept_id: int
    value_coded: Optional[int] = None
    value_numeric: Optional[float] = None
    value_datetime: Optional[datetime] = None
    obs_datetime: datetime = None
    obs_group_id: Optional[int] = None
    location_id: int = 1
    uuid: str = field(default_factory=_generate_uuid)


# =============================================================================
# ID GENERATORS
# =============================================================================

class IDGenerator:
    """Generates sequential IDs for various entities."""

    def __init__(self, start_id: int = 1):
        self._counters = {}
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
    """Generates test data for patient_status_arv and alert_viral_load procedures."""

    def __init__(self, config: GeneratorConfig):
        self.config = config
        self.id_gen = IDGenerator()
        self.encounter_type_ids: Dict[str, int] = {}
        self.encounter_type_names: Dict[int, str] = {}
        self.concept_ids: Dict[str, int] = {}

        # Handle seed: use provided seed or generate a random one
        if config.seed is None:
            self.seed = random.randint(0, 2**32 - 1)
        else:
            self.seed = config.seed
        random.seed(self.seed)

        # Storage for generated data
        self.patients: List[Patient] = []
        self.visits: List[Visit] = []
        self.encounters: List[Encounter] = []
        self.observations: List[Observation] = []

        # iSantePlus derived data
        self.patient_dispensing: List[Dict] = []
        self.patient_laboratory: List[Dict] = []
        self.patient_on_arv: List[int] = []
        self.discontinuation_reasons: List[Dict] = []
        self.patient_pregnancy: List[Dict] = []

    def _random_date(self, start: datetime, end: datetime) -> datetime:
        """Generate a random datetime between start and end."""
        delta = end - start
        random_days = random.randint(0, delta.days)
        random_seconds = random.randint(0, 86400)
        return start + timedelta(days=random_days, seconds=random_seconds)

    def _random_date_after(self, after: datetime, max_days: int = 365) -> datetime:
        """Generate a random date after a given date."""
        days_ahead = random.randint(1, max_days)
        return after + timedelta(days=days_ahead)

    def _setup_encounter_types(self) -> List[Dict]:
        """Create encounter type records."""
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
        return enc_types

    def _setup_concepts(self) -> List[Dict]:
        """Create concept records for UUID lookups."""
        concepts = []
        for name, uid in CONCEPT_UUIDS.items():
            concept_id = self.id_gen.next('concept')
            self.concept_ids[name] = concept_id
            concepts.append({
                'concept_id': concept_id,
                'uuid': uid,
            })
        return concepts

    def _generate_patient(self) -> Patient:
        """Generate a single patient with characteristics."""
        patient_id = self.id_gen.next('patient')

        # Determine if pediatric (affects birthdate)
        is_pediatric = random.random() < self.config.pct_pediatric

        if is_pediatric:
            # Pediatric: 0-18 years old
            age_days = random.randint(0, 18 * 365)
            birthdate = datetime.now() - timedelta(days=age_days)
        else:
            # Adult: 18-80 years old
            age_days = random.randint(18 * 365, 80 * 365)
            birthdate = datetime.now() - timedelta(days=age_days)

        gender = random.choice(['M', 'F'])
        is_hiv_positive = random.random() < self.config.pct_hiv_positive

        # ARV status only for HIV+ patients
        is_on_arv = is_hiv_positive and random.random() < self.config.pct_on_arv

        # Discontinuation
        is_discontinued = is_hiv_positive and random.random() < self.config.pct_discontinued
        discontinuation_reason = None
        if is_discontinued:
            discontinuation_reason = random.choice(DISCONTINUATION_REASONS)

        # Pregnancy only for adult females
        is_pregnant = (
            gender == 'F' and
            not is_pediatric and
            random.random() < self.config.pct_pregnant
        )

        # Exposed infant (pediatric, HIV-exposed)
        is_exposed_infant = (
            is_pediatric and
            not is_hiv_positive and
            random.random() < self.config.pct_exposed_infant / self.config.pct_pediatric
        )

        # ARV start date
        date_started_arv = None
        if is_on_arv:
            # Started ARV sometime in the past
            months_on_arv = random.randint(1, 60)  # 1 month to 5 years
            date_started_arv = datetime.now() - timedelta(days=months_on_arv * 30)

        location_id = random.randint(1, 10)  # 10 locations

        return Patient(
            patient_id=patient_id,
            birthdate=birthdate,
            gender=gender,
            is_hiv_positive=is_hiv_positive,
            is_on_arv=is_on_arv,
            is_discontinued=is_discontinued,
            discontinuation_reason=discontinuation_reason,
            is_pregnant=is_pregnant,
            is_pediatric=is_pediatric,
            is_exposed_infant=is_exposed_infant,
            date_started_arv=date_started_arv,
            location_id=location_id,
        )

    def _generate_visits_for_patient(self, patient: Patient) -> List[Visit]:
        """Generate visits for a patient."""
        visits = []

        # Number of visits based on how long they've been in care
        if patient.date_started_arv:
            months_in_care = (datetime.now() - patient.date_started_arv).days // 30
            num_visits = min(months_in_care + 1, random.randint(2, 24))
        else:
            num_visits = random.randint(1, 10)

        visit_date = (
            patient.first_visit_date or
            patient.date_started_arv or
            self._random_date(self.config.start_date, self.config.end_date - timedelta(days=30))
        )

        for _ in range(num_visits):
            visit = Visit(
                visit_id=self.id_gen.next('visit'),
                patient_id=patient.patient_id,
                date_started=visit_date,
                location_id=patient.location_id,
            )
            visits.append(visit)

            # Next visit 1-3 months later
            visit_date = self._random_date_after(visit_date, max_days=90)
            if visit_date > self.config.end_date:
                break

        return visits

    def _generate_encounters_for_visit(
        self, visit: Visit, patient: Patient, is_first_visit: bool = False
    ) -> List[Encounter]:
        """Generate encounters for a visit."""
        encounters = []

        # Determine which encounter types to create
        encounter_types_to_create = []

        # First visit or follow-up
        if is_first_visit:
            if patient.is_pediatric:
                encounter_types_to_create.append('pediatric')
            else:
                encounter_types_to_create.append('first_visit')
        else:
            if patient.is_pediatric:
                encounter_types_to_create.append('pediatric_followup')
            else:
                encounter_types_to_create.append('followup')

        # Add dispensing encounter if on ARV
        if patient.is_on_arv:
            encounter_types_to_create.append(random.choice(['dispensing1', 'dispensing2']))

        # Add lab encounter sometimes
        if random.random() < 0.3:
            encounter_types_to_create.append('lab')

        # Add discontinuation encounter if discontinued
        if patient.is_discontinued and random.random() < 0.5:
            encounter_types_to_create.append('discontinuation')

        for enc_type in encounter_types_to_create:
            enc = Encounter(
                encounter_id=self.id_gen.next('encounter'),
                visit_id=visit.visit_id,
                patient_id=patient.patient_id,
                encounter_type_id=self.encounter_type_ids[enc_type],
                encounter_datetime=visit.date_started,
                location_id=visit.location_id,
            )
            encounters.append(enc)

        return encounters

    def _generate_observations_for_encounter(
        self,
        encounter: Encounter,
        patient: Patient,
        enc_type_name: str
    ) -> List[Observation]:
        """Generate observations for an encounter."""
        observations = []

        def add_obs(concept_id: int, value_coded: int = None,
                   value_numeric: float = None, obs_group_id: int = None) -> Observation:
            obs = Observation(
                obs_id=self.id_gen.next('obs'),
                person_id=patient.patient_id,
                encounter_id=encounter.encounter_id,
                concept_id=concept_id,
                value_coded=value_coded,
                value_numeric=value_numeric,
                obs_datetime=encounter.encounter_datetime,
                obs_group_id=obs_group_id,
                location_id=encounter.location_id,
            )
            observations.append(obs)
            return obs

        # PCR tests for pediatric/lab encounters
        if enc_type_name in ('pediatric', 'lab'):
            if patient.is_exposed_infant or random.random() < 0.2:
                # PCR test with result
                # PCR_TEST_1 (1030): positive=703, negative=664
                # PCR_TEST_2 (844): positive=1301, negative=1302
                pcr_concept = random.choice([ConceptID.PCR_TEST_1, ConceptID.PCR_TEST_2])
                if patient.is_hiv_positive:
                    if pcr_concept == ConceptID.PCR_TEST_1:
                        result = ConceptID.POSITIVE  # 703
                    else:
                        result = ConceptID.POSITIVE_CODED  # 1301
                else:
                    if pcr_concept == ConceptID.PCR_TEST_1:
                        result = ConceptID.NEGATIVE  # 664
                    else:
                        result = ConceptID.NEGATIVE_ALT  # 1302
                add_obs(pcr_concept, value_coded=result)

        # Exposed infant checkbox
        if enc_type_name in ('pediatric', 'pediatric_followup'):
            if patient.is_exposed_infant:
                add_obs(ConceptID.CONDITION_CHECKBOX, value_coded=ConceptID.EXPOSED_INFANT)

        # HIV confirmed serological
        if enc_type_name in ('pediatric', 'pediatric_followup'):
            if patient.is_hiv_positive and random.random() < 0.3:
                add_obs(ConceptID.CONDITION_CHECKBOX, value_coded=ConceptID.HIV_CONFIRMED_SEROLOGICAL)

        # Discontinuation observations
        if enc_type_name == 'discontinuation' and patient.is_discontinued:
            add_obs(ConceptID.DISCONTINUATION_REASON, value_coded=patient.discontinuation_reason)

            # Sub-reason for stopped
            if patient.discontinuation_reason == ConceptID.DISCONTINUATION_SUB:
                add_obs(
                    ConceptID.DISCONTINUATION_SUB,
                    value_coded=random.choice([ConceptID.STOPPED_REASON_1, ConceptID.STOPPED_REASON_2])
                )

            # Seroreversion for exposed infants
            if patient.is_exposed_infant and random.random() < 0.2:
                add_obs(ConceptID.DISCONTINUATION_SUB, value_coded=ConceptID.SEROREVERSION)

        # Viral load for lab encounters
        if enc_type_name == 'lab':
            if patient.is_on_arv and random.random() < self.config.pct_with_viral_load:
                # Numeric viral load
                if random.random() < 0.7:  # 70% suppressed
                    viral_load = random.randint(20, 500)
                else:
                    viral_load = random.randint(1001, 100000)
                add_obs(ConceptID.VIRAL_LOAD_NUMERIC, value_numeric=viral_load)

                # Coded viral load result
                if viral_load < 1000:
                    add_obs(ConceptID.VIRAL_LOAD_CODED, value_coded=ConceptID.SUPPRESSED)
                else:
                    add_obs(ConceptID.VIRAL_LOAD_CODED, value_coded=ConceptID.POSITIVE_CODED)

        # Drug dispensing observations (for TB co-infection alert)
        if enc_type_name in ('dispensing1', 'dispensing2'):
            # Sometimes add INH prophylaxis
            if random.random() < 0.3:
                add_obs(ConceptID.DRUG_PRESCRIBED, value_coded=ConceptID.ISONIAZID)

            # Sometimes add TB treatment (INH + Rifampicin)
            if random.random() < 0.05:
                # Create obs group for drug
                group_obs = add_obs(self.concept_ids.get('isoniazid_group', 99999))
                add_obs(ConceptID.DRUG_PRESCRIBED, value_coded=ConceptID.ISONIAZID, obs_group_id=group_obs.obs_id)
                add_obs(ConceptID.DRUG_GIVEN, value_coded=ConceptID.ARV_DRUG, obs_group_id=group_obs.obs_id)

                group_obs2 = add_obs(self.concept_ids.get('rifampicin_group', 99998))
                add_obs(ConceptID.DRUG_PRESCRIBED, value_coded=ConceptID.RIFAMPICIN, obs_group_id=group_obs2.obs_id)
                add_obs(ConceptID.DRUG_GIVEN, value_coded=ConceptID.ARV_DRUG, obs_group_id=group_obs2.obs_id)

        # DDP subscription
        if random.random() < 0.02:
            add_obs(self.concept_ids.get('ddp', 99997), value_coded=ConceptID.ARV_DRUG)

        return observations

    def _generate_isanteplus_data(self, patient: Patient):
        """Generate iSantePlus derived table data for a patient."""

        # patient_on_arv
        if patient.is_on_arv:
            self.patient_on_arv.append(patient.patient_id)

        # discontinuation_reason
        if patient.is_discontinued and patient.discontinuation_reason:
            self.discontinuation_reasons.append({
                'patient_id': patient.patient_id,
                'reason': patient.discontinuation_reason,
                'visit_date': patient.visits[-1].date_started if patient.visits else datetime.now(),
            })

        # patient_pregnancy
        if patient.is_pregnant:
            self.patient_pregnancy.append({
                'patient_id': patient.patient_id,
                'pregnancy_date': self._random_date(
                    datetime.now() - timedelta(days=270),
                    datetime.now()
                ),
            })

        # patient_dispensing (for ARV patients)
        if patient.is_on_arv and patient.visits:
            for visit in patient.visits:
                next_disp_date = visit.date_started + timedelta(days=random.choice([30, 60, 90]))
                encounter_id = visit.encounters[0].encounter_id if visit.encounters else None

                self.patient_dispensing.append({
                    'patient_id': patient.patient_id,
                    'encounter_id': encounter_id,
                    'visit_id': visit.visit_id,
                    'visit_date': visit.date_started,
                    'next_dispensation_date': next_disp_date,
                    'arv_drug': ConceptID.ARV_DRUG,
                    'rx_or_prophy': random.choice([ConceptID.RX_TREATMENT, None]),
                    'drug_id': random.randint(1, 100),
                    'voided': 0,
                    'location_id': visit.location_id,
                })

                # TB co-infection: add INH + Rifampicin in same encounter
                if random.random() < self.config.pct_tb_coinfection:
                    self.patient_dispensing.append({
                        'patient_id': patient.patient_id,
                        'encounter_id': encounter_id,
                        'visit_id': visit.visit_id,
                        'visit_date': visit.date_started,
                        'next_dispensation_date': next_disp_date,
                        'arv_drug': ConceptID.ARV_DRUG,
                        'rx_or_prophy': ConceptID.RX_TREATMENT,
                        'drug_id': ConceptID.ISONIAZID,
                        'voided': 0,
                        'location_id': visit.location_id,
                    })
                    self.patient_dispensing.append({
                        'patient_id': patient.patient_id,
                        'encounter_id': encounter_id,
                        'visit_id': visit.visit_id,
                        'visit_date': visit.date_started,
                        'next_dispensation_date': next_disp_date,
                        'arv_drug': ConceptID.ARV_DRUG,
                        'rx_or_prophy': ConceptID.RX_TREATMENT,
                        'drug_id': ConceptID.RIFAMPICIN,
                        'voided': 0,
                        'location_id': visit.location_id,
                    })

        # ARV prophylaxis for exposed infants
        if patient.is_exposed_infant and patient.visits:
            visit = patient.visits[0]
            encounter_id = visit.encounters[0].encounter_id if visit.encounters else None
            self.patient_dispensing.append({
                'patient_id': patient.patient_id,
                'encounter_id': encounter_id,
                'visit_id': visit.visit_id,
                'visit_date': visit.date_started,
                'next_dispensation_date': visit.date_started + timedelta(days=30),
                'arv_drug': ConceptID.ARV_DRUG,
                'rx_or_prophy': ConceptID.RX_PROPHYLAXIS,
                'drug_id': random.randint(1, 100),
                'voided': 0,
                'location_id': visit.location_id,
            })

        # patient_laboratory (viral load tests)
        if patient.is_on_arv and random.random() < self.config.pct_with_viral_load:
            num_tests = random.randint(1, 4)
            test_date = patient.date_started_arv + timedelta(days=180)  # First test 6 months after ARV start

            for _ in range(num_tests):
                if test_date > datetime.now():
                    break

                # Viral load result
                if random.random() < 0.7:  # 70% suppressed
                    result = random.randint(20, 500)
                else:
                    result = random.randint(1001, 100000)

                self.patient_laboratory.append({
                    'patient_id': patient.patient_id,
                    'encounter_id': self.id_gen.next('lab_encounter'),
                    'test_id': ConceptID.VIRAL_LOAD_NUMERIC,
                    'test_done': 1,
                    'test_result': result,
                    'visit_date': test_date,
                    'date_test_done': test_date,
                    'voided': 0,
                })

                # Also add coded result
                self.patient_laboratory.append({
                    'patient_id': patient.patient_id,
                    'encounter_id': self.id_gen.current('lab_encounter'),
                    'test_id': ConceptID.VIRAL_LOAD_CODED,
                    'test_done': 1,
                    'test_result': ConceptID.SUPPRESSED if result < 1000 else ConceptID.POSITIVE_CODED,
                    'visit_date': test_date,
                    'date_test_done': test_date,
                    'voided': 0,
                })

                test_date = test_date + timedelta(days=random.randint(180, 365))

            # HIV test for older patients
            if not patient.is_pediatric or (datetime.now() - patient.birthdate).days >= 18 * 30:
                self.patient_laboratory.append({
                    'patient_id': patient.patient_id,
                    'encounter_id': self.id_gen.next('lab_encounter'),
                    'test_id': ConceptID.HIV_TEST,
                    'test_done': 1,
                    'test_result': ConceptID.POSITIVE if patient.is_hiv_positive else ConceptID.NEGATIVE,
                    'visit_date': patient.visits[0].date_started if patient.visits else datetime.now(),
                    'date_test_done': patient.visits[0].date_started if patient.visits else datetime.now(),
                    'voided': 0,
                })

    def _generate_seed_patients(self) -> List[Patient]:
        """Generate patients that ensure coverage of specific stored procedure scenarios."""
        seed_patients = []
        now = datetime.now()

        scenarios = [
            # (is_hiv+, is_on_arv, is_discontinued, disc_reason, months_on_arv, is_pregnant, is_exposed, visit_months_ago)
            # Discontinued on ARV scenarios (status 1, 2, 3)
            (True, True, True, ConceptID.DECEASED, 12, False, False, None),
            (True, True, True, ConceptID.TRANSFERRED, 12, False, False, None),
            (True, True, True, ConceptID.DISCONTINUATION_SUB, 12, False, False, None),
            # Discontinued pre-ARV scenarios (status 4, 5)
            (True, False, True, ConceptID.DECEASED, None, False, False, None),
            (True, False, True, ConceptID.TRANSFERRED, None, False, False, None),
            # Specific ARV durations for alerts
            (True, True, False, None, 5, False, False, None),  # 5 months - alert 2
            (True, True, False, None, 3, False, False, None),  # 3 months - alert 10
            (True, True, False, None, 4, True, False, None),   # 4 months pregnant - alert 3
            # Pre-ARV scenario with recent visit (status 7)
            (True, False, False, None, None, False, False, 3),  # visited 3 months ago
            # Exposed infant with seroreversion
            (False, False, True, ConceptID.DISCONTINUATION_SUB, None, False, True, None),
        ]

        for hiv, arv, disc, reason, months, pregnant, exposed, visit_months in scenarios:
            patient_id = self.id_gen.next('patient')
            date_started = None
            if arv and months:
                date_started = now - timedelta(days=months * 30)

            first_visit = None
            if visit_months is not None:
                first_visit = now - timedelta(days=visit_months * 30)

            is_pediatric = exposed
            if is_pediatric:
                birthdate = now - timedelta(days=365)
            else:
                birthdate = now - timedelta(days=30 * 365)

            patient = Patient(
                patient_id=patient_id,
                birthdate=birthdate,
                gender='F' if pregnant else random.choice(['M', 'F']),
                is_hiv_positive=hiv,
                is_on_arv=arv,
                is_discontinued=disc,
                discontinuation_reason=reason,
                is_pregnant=pregnant,
                is_pediatric=is_pediatric,
                is_exposed_infant=exposed,
                date_started_arv=date_started,
                location_id=1,
                first_visit_date=first_visit,
            )
            seed_patients.append(patient)

        return seed_patients

    def generate(self) -> None:
        """Generate all test data."""
        print(f"Generating test data for {self.config.num_patients} patients...")
        print(f"  Using seed: {self.seed} (use --seed {self.seed} to reproduce)")

        # Setup reference data
        self.encounter_types = self._setup_encounter_types()
        self.concepts = self._setup_concepts()

        # Generate seed patients first to ensure coverage
        seed_patients = self._generate_seed_patients()
        num_random = max(0, self.config.num_patients - len(seed_patients))

        # Generate random patients
        all_patients = seed_patients + [self._generate_patient() for _ in range(num_random)]

        for i, patient in enumerate(all_patients):
            if (i + 1) % 10000 == 0:
                print(f"  Generated {i + 1} patients...")

            self.patients.append(patient)

            # Generate visits
            patient.visits = self._generate_visits_for_patient(patient)
            self.visits.extend(patient.visits)

            # Generate encounters and observations
            for idx, visit in enumerate(patient.visits):
                visit.encounters = self._generate_encounters_for_visit(
                    visit, patient, is_first_visit=(idx == 0)
                )
                self.encounters.extend(visit.encounters)

                for encounter in visit.encounters:
                    enc_type_name = self.encounter_type_names.get(encounter.encounter_type_id)
                    if enc_type_name:
                        obs = self._generate_observations_for_encounter(
                            encounter, patient, enc_type_name
                        )
                        encounter.observations = obs
                        self.observations.extend(obs)

            # Generate iSantePlus data
            self._generate_isanteplus_data(patient)

        print(f"Generated:")
        print(f"  - {len(self.patients)} patients")
        print(f"  - {len(self.visits)} visits")
        print(f"  - {len(self.encounters)} encounters")
        print(f"  - {len(self.observations)} observations")
        print(f"  - {len(self.patient_on_arv)} patients on ARV")
        print(f"  - {len(self.patient_dispensing)} dispensing records")
        print(f"  - {len(self.patient_laboratory)} laboratory records")


# =============================================================================
# SQL OUTPUT
# =============================================================================

class SQLWriter:
    """Writes generated data to SQL files."""

    def __init__(self, generator: TestDataGenerator, output_dir: str = '.'):
        self.gen = generator
        self.output_dir = output_dir
        ensure_directory(output_dir)

    def _escape_value(self, val: Any) -> str:
        """Escape a value for SQL."""
        if val is None:
            return 'NULL'
        elif isinstance(val, bool):
            return '1' if val else '0'
        elif isinstance(val, (int, float)):
            return str(val)
        elif isinstance(val, datetime):
            return f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'"
        else:
            # Escape single quotes
            escaped = str(val).replace("'", "''")
            return f"'{escaped}'"

    def _write_batch_insert(
        self,
        f: TextIO,
        table: str,
        columns: List[str],
        rows: List[tuple],
        batch_size: int = 1000
    ) -> None:
        """Write batch INSERT statements."""
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]

            f.write(f"INSERT INTO {table} ({', '.join(columns)}) VALUES\n")

            value_strings = []
            for row in batch:
                values = ', '.join(self._escape_value(v) for v in row)
                value_strings.append(f"({values})")

            f.write(',\n'.join(value_strings))
            f.write(';\n\n')

    def write_openmrs_data(self, filename: str = 'test_data_openmrs.sql') -> None:
        """Write OpenMRS schema data to SQL file."""
        filepath = f"{self.output_dir}/{filename}"
        print(f"Writing OpenMRS data to {filepath}...")
        now = datetime.now()

        with open(filepath, 'w') as f:
            f.write("-- Generated test data for OpenMRS schema\n")
            f.write("-- Run this against a test database only!\n\n")
            f.write("USE openmrs;\n\n")
            f.write("SET FOREIGN_KEY_CHECKS = 0;\n")
            f.write("SET SQL_MODE = 'NO_AUTO_VALUE_ON_ZERO';\n\n")

            # Encounter types
            f.write("-- Encounter Types\n")
            f.write("TRUNCATE TABLE encounter_type;\n")
            rows = [
                (et['encounter_type_id'], et['name'], et['uuid'], et['creator'], et['date_created'], 0)
                for et in self.gen.encounter_types
            ]
            self._write_batch_insert(
                f, 'encounter_type',
                ['encounter_type_id', 'name', 'uuid', 'creator', 'date_created', 'retired'],
                rows
            )

            # Concepts (for UUID lookups)
            f.write("-- Concepts for UUID lookups\n")
            f.write("TRUNCATE TABLE concept;\n")
            rows = [(c['concept_id'], c['uuid']) for c in self.gen.concepts]
            self._write_batch_insert(f, 'concept', ['concept_id', 'uuid'], rows)

            # Person
            f.write("-- Person records\n")
            f.write("TRUNCATE TABLE person;\n")
            rows = [
                (p.person_id, p.gender, p.birthdate, 1, now, 0, p.person_uuid)
                for p in self.gen.patients
            ]
            self._write_batch_insert(
                f, 'person',
                ['person_id', 'gender', 'birthdate', 'creator', 'date_created', 'voided', 'uuid'],
                rows
            )

            # Patient
            f.write("-- Patient records\n")
            f.write("TRUNCATE TABLE patient;\n")
            rows = [(p.patient_id, 1, now, 0) for p in self.gen.patients]
            self._write_batch_insert(
                f, 'patient',
                ['patient_id', 'creator', 'date_created', 'voided'],
                rows
            )

            # Visit
            f.write("-- Visit records\n")
            f.write("TRUNCATE TABLE visit;\n")
            rows = [
                (v.visit_id, v.patient_id, v.date_started, v.location_id, 1, now, 0, v.uuid)
                for v in self.gen.visits
            ]
            self._write_batch_insert(
                f, 'visit',
                ['visit_id', 'patient_id', 'date_started', 'location_id', 'creator', 'date_created', 'voided', 'uuid'],
                rows
            )

            # Encounter
            f.write("-- Encounter records\n")
            f.write("TRUNCATE TABLE encounter;\n")
            rows = [
                (e.encounter_id, e.encounter_type_id, e.patient_id, e.location_id,
                 e.encounter_datetime, e.visit_id, 1, now, 0, e.uuid)
                for e in self.gen.encounters
            ]
            self._write_batch_insert(
                f, 'encounter',
                ['encounter_id', 'encounter_type', 'patient_id', 'location_id',
                 'encounter_datetime', 'visit_id', 'creator', 'date_created', 'voided', 'uuid'],
                rows
            )

            # Obs
            f.write("-- Observation records\n")
            f.write("TRUNCATE TABLE obs;\n")
            rows = [
                (o.obs_id, o.person_id, o.encounter_id, o.concept_id,
                 o.obs_datetime, o.location_id, o.value_coded, o.value_numeric,
                 o.value_datetime, o.obs_group_id, 1, now, 0, o.uuid)
                for o in self.gen.observations
            ]
            self._write_batch_insert(
                f, 'obs',
                ['obs_id', 'person_id', 'encounter_id', 'concept_id',
                 'obs_datetime', 'location_id', 'value_coded', 'value_numeric',
                 'value_datetime', 'obs_group_id', 'creator', 'date_created', 'voided', 'uuid'],
                rows
            )

            f.write("SET FOREIGN_KEY_CHECKS = 1;\n")

        print(f"  Written {len(rows)} observation records")

    def write_isanteplus_data(self, filename: str = 'test_data_isanteplus.sql') -> None:
        """Write iSantePlus schema data to SQL file."""
        filepath = f"{self.output_dir}/{filename}"
        print(f"Writing iSantePlus data to {filepath}...")

        with open(filepath, 'w') as f:
            f.write("-- Generated test data for iSantePlus schema\n")
            f.write("-- Run this against a test database only!\n\n")
            f.write("USE isanteplus;\n\n")
            f.write("SET FOREIGN_KEY_CHECKS = 0;\n\n")

            # Patient table
            f.write("-- Patient records\n")
            f.write("TRUNCATE TABLE patient;\n")
            rows = [
                (p.patient_id, p.birthdate,
                 1 if p.is_hiv_positive else 0,  # vih_status
                 None,  # arv_status (populated by procedure)
                 p.date_started_arv,
                 0)  # voided
                for p in self.gen.patients
            ]
            self._write_batch_insert(
                f, 'patient',
                ['patient_id', 'birthdate', 'vih_status', 'arv_status', 'date_started_arv', 'voided'],
                rows
            )

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
                (dr['patient_id'], dr['reason'], dr['visit_date'])
                for dr in self.gen.discontinuation_reasons
            ]
            if rows:
                self._write_batch_insert(
                    f, 'discontinuation_reason',
                    ['patient_id', 'reason', 'visit_date'],
                    rows
                )

            # patient_dispensing
            f.write("-- Patient dispensing records\n")
            f.write("TRUNCATE TABLE patient_dispensing;\n")
            rows = [
                (pd['patient_id'], pd['encounter_id'], pd['visit_id'],
                 pd['visit_date'], pd['next_dispensation_date'], pd['arv_drug'],
                 pd['rx_or_prophy'], pd['drug_id'], pd['voided'], pd['location_id'])
                for pd in self.gen.patient_dispensing
            ]
            if rows:
                self._write_batch_insert(
                    f, 'patient_dispensing',
                    ['patient_id', 'encounter_id', 'visit_id', 'visit_date',
                     'next_dispensation_date', 'arv_drug', 'rx_or_prophy',
                     'drug_id', 'voided', 'location_id'],
                    rows
                )

            # patient_laboratory
            f.write("-- Patient laboratory records\n")
            f.write("TRUNCATE TABLE patient_laboratory;\n")
            rows = [
                (pl['patient_id'], pl['encounter_id'], pl['test_id'],
                 pl['test_done'], pl['test_result'], pl['visit_date'],
                 pl['date_test_done'], pl['voided'])
                for pl in self.gen.patient_laboratory
            ]
            if rows:
                self._write_batch_insert(
                    f, 'patient_laboratory',
                    ['patient_id', 'encounter_id', 'test_id', 'test_done',
                     'test_result', 'visit_date', 'date_test_done', 'voided'],
                    rows
                )

            # patient_pregnancy
            f.write("-- Patient pregnancy records\n")
            f.write("TRUNCATE TABLE patient_pregnancy;\n")
            rows = [(pp['patient_id'],) for pp in self.gen.patient_pregnancy]
            if rows:
                self._write_batch_insert(f, 'patient_pregnancy', ['patient_id'], rows)

            # Clear output tables (to be populated by procedures)
            f.write("-- Clear output tables\n")
            f.write("TRUNCATE TABLE patient_status_arv;\n")
            f.write("TRUNCATE TABLE exposed_infants;\n")
            f.write("TRUNCATE TABLE alert;\n\n")

            f.write("SET FOREIGN_KEY_CHECKS = 1;\n")

        print(f"  Written {len(self.gen.patient_dispensing)} dispensing records")
        print(f"  Written {len(self.gen.patient_laboratory)} laboratory records")


# =============================================================================
# DDL OUTPUT
# =============================================================================

class DDLWriter:
    """Writes DDL (CREATE TABLE) statements for required tables."""

    def __init__(self, output_dir: str = '.'):
        self.output_dir = output_dir
        ensure_directory(output_dir)

    def write_openmrs_ddl(self, filename: str = 'ddl_openmrs.sql') -> None:
        """Write OpenMRS schema DDL to SQL file."""
        filepath = f"{self.output_dir}/{filename}"
        print(f"Writing OpenMRS DDL to {filepath}...")

        ddl = """-- =============================================================================
-- OpenMRS Schema DDL for ETL Testing
-- Generated DDL with minimal columns required for stored procedure testing
-- =============================================================================

CREATE DATABASE IF NOT EXISTS openmrs;
USE openmrs;

SET FOREIGN_KEY_CHECKS = 0;

-- -----------------------------------------------------------------------------
-- encounter_type: Defines types of clinical encounters
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS encounter_type;
CREATE TABLE encounter_type (
    encounter_type_id INT NOT NULL AUTO_INCREMENT,
    name VARCHAR(50) NOT NULL DEFAULT '',
    description VARCHAR(255) DEFAULT NULL,
    creator INT NOT NULL DEFAULT 0,
    date_created DATETIME NOT NULL,
    retired TINYINT(1) NOT NULL DEFAULT 0,
    retired_by INT DEFAULT NULL,
    date_retired DATETIME DEFAULT NULL,
    retire_reason VARCHAR(255) DEFAULT NULL,
    uuid CHAR(38) NOT NULL,
    date_changed DATETIME DEFAULT NULL,
    changed_by INT DEFAULT NULL,
    PRIMARY KEY (encounter_type_id),
    UNIQUE KEY uuid (uuid),
    KEY name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -----------------------------------------------------------------------------
-- concept: Medical concepts/terminology
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS concept;
CREATE TABLE concept (
    concept_id INT NOT NULL AUTO_INCREMENT,
    retired TINYINT(1) NOT NULL DEFAULT 0,
    short_name VARCHAR(255) DEFAULT NULL,
    description TEXT,
    form_text TEXT,
    datatype_id INT NOT NULL DEFAULT 0,
    class_id INT NOT NULL DEFAULT 0,
    is_set TINYINT(1) NOT NULL DEFAULT 0,
    creator INT NOT NULL DEFAULT 0,
    date_created DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    version VARCHAR(50) DEFAULT NULL,
    changed_by INT DEFAULT NULL,
    date_changed DATETIME DEFAULT NULL,
    retired_by INT DEFAULT NULL,
    date_retired DATETIME DEFAULT NULL,
    retire_reason VARCHAR(255) DEFAULT NULL,
    uuid CHAR(38) NOT NULL,
    PRIMARY KEY (concept_id),
    UNIQUE KEY uuid (uuid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -----------------------------------------------------------------------------
-- person: Core demographics
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS person;
CREATE TABLE person (
    person_id INT NOT NULL AUTO_INCREMENT,
    gender VARCHAR(50) DEFAULT '',
    birthdate DATE DEFAULT NULL,
    birthdate_estimated TINYINT(1) NOT NULL DEFAULT 0,
    dead TINYINT(1) NOT NULL DEFAULT 0,
    death_date DATETIME DEFAULT NULL,
    cause_of_death INT DEFAULT NULL,
    creator INT DEFAULT NULL,
    date_created DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    changed_by INT DEFAULT NULL,
    date_changed DATETIME DEFAULT NULL,
    voided TINYINT(1) NOT NULL DEFAULT 0,
    voided_by INT DEFAULT NULL,
    date_voided DATETIME DEFAULT NULL,
    void_reason VARCHAR(255) DEFAULT NULL,
    uuid CHAR(38) NOT NULL,
    deathdate_estimated TINYINT(1) NOT NULL DEFAULT 0,
    birthtime TIME DEFAULT NULL,
    PRIMARY KEY (person_id),
    UNIQUE KEY uuid (uuid),
    KEY gender (gender)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -----------------------------------------------------------------------------
-- patient: Links to person, adds patient-specific data
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS patient;
CREATE TABLE patient (
    patient_id INT NOT NULL,
    creator INT NOT NULL DEFAULT 0,
    date_created DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    changed_by INT DEFAULT NULL,
    date_changed DATETIME DEFAULT NULL,
    voided TINYINT(1) NOT NULL DEFAULT 0,
    voided_by INT DEFAULT NULL,
    date_voided DATETIME DEFAULT NULL,
    void_reason VARCHAR(255) DEFAULT NULL,
    allergy_status VARCHAR(50) NOT NULL DEFAULT 'Unknown',
    PRIMARY KEY (patient_id),
    CONSTRAINT patient_person_fk FOREIGN KEY (patient_id) REFERENCES person (person_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -----------------------------------------------------------------------------
-- visit: Patient visit/admission
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS visit;
CREATE TABLE visit (
    visit_id INT NOT NULL AUTO_INCREMENT,
    patient_id INT NOT NULL,
    visit_type_id INT NOT NULL DEFAULT 1,
    date_started DATETIME NOT NULL,
    date_stopped DATETIME DEFAULT NULL,
    indication_concept_id INT DEFAULT NULL,
    location_id INT DEFAULT NULL,
    creator INT NOT NULL,
    date_created DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    changed_by INT DEFAULT NULL,
    date_changed DATETIME DEFAULT NULL,
    voided TINYINT(1) NOT NULL DEFAULT 0,
    voided_by INT DEFAULT NULL,
    date_voided DATETIME DEFAULT NULL,
    void_reason VARCHAR(255) DEFAULT NULL,
    uuid CHAR(38) NOT NULL,
    PRIMARY KEY (visit_id),
    UNIQUE KEY uuid (uuid),
    KEY idx_visit_patient (patient_id),
    KEY idx_visit_date_started (date_started),
    KEY idx_visit_patient_date_voided (patient_id, date_started, voided),
    CONSTRAINT visit_patient_fk FOREIGN KEY (patient_id) REFERENCES patient (patient_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -----------------------------------------------------------------------------
-- encounter: Clinical encounter within a visit
-- -----------------------------------------------------------------------------
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
    voided_by INT DEFAULT NULL,
    date_voided DATETIME DEFAULT NULL,
    void_reason VARCHAR(255) DEFAULT NULL,
    changed_by INT DEFAULT NULL,
    date_changed DATETIME DEFAULT NULL,
    visit_id INT DEFAULT NULL,
    uuid CHAR(38) NOT NULL,
    PRIMARY KEY (encounter_id),
    UNIQUE KEY uuid (uuid),
    KEY idx_encounter_patient (patient_id),
    KEY idx_encounter_type (encounter_type),
    KEY idx_encounter_datetime (encounter_datetime),
    KEY idx_encounter_visit (visit_id),
    KEY idx_encounter_type_patient_voided (encounter_type, patient_id, voided),
    CONSTRAINT encounter_patient_fk FOREIGN KEY (patient_id) REFERENCES patient (patient_id),
    CONSTRAINT encounter_type_fk FOREIGN KEY (encounter_type) REFERENCES encounter_type (encounter_type_id),
    CONSTRAINT encounter_visit_fk FOREIGN KEY (visit_id) REFERENCES visit (visit_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -----------------------------------------------------------------------------
-- obs: Clinical observations/data points
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS obs;
CREATE TABLE obs (
    obs_id INT NOT NULL AUTO_INCREMENT,
    person_id INT NOT NULL,
    concept_id INT NOT NULL DEFAULT 0,
    encounter_id INT DEFAULT NULL,
    order_id INT DEFAULT NULL,
    obs_datetime DATETIME NOT NULL,
    location_id INT DEFAULT NULL,
    obs_group_id INT DEFAULT NULL,
    accession_number VARCHAR(255) DEFAULT NULL,
    value_group_id INT DEFAULT NULL,
    value_coded INT DEFAULT NULL,
    value_coded_name_id INT DEFAULT NULL,
    value_drug INT DEFAULT NULL,
    value_datetime DATETIME DEFAULT NULL,
    value_numeric DOUBLE DEFAULT NULL,
    value_modifier VARCHAR(2) DEFAULT NULL,
    value_text TEXT,
    value_complex VARCHAR(1000) DEFAULT NULL,
    comments VARCHAR(255) DEFAULT NULL,
    creator INT NOT NULL DEFAULT 0,
    date_created DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    voided TINYINT(1) NOT NULL DEFAULT 0,
    voided_by INT DEFAULT NULL,
    date_voided DATETIME DEFAULT NULL,
    void_reason VARCHAR(255) DEFAULT NULL,
    uuid CHAR(38) NOT NULL,
    previous_version INT DEFAULT NULL,
    form_namespace_and_path VARCHAR(255) DEFAULT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'FINAL',
    interpretation VARCHAR(32) DEFAULT NULL,
    PRIMARY KEY (obs_id),
    UNIQUE KEY uuid (uuid),
    KEY idx_obs_person (person_id),
    KEY idx_obs_concept (concept_id),
    KEY idx_obs_encounter (encounter_id),
    KEY idx_obs_datetime (obs_datetime),
    KEY idx_obs_group (obs_group_id),
    KEY idx_obs_concept_value_voided_person (concept_id, value_coded, voided, person_id),
    KEY idx_obs_encounter_concept_voided (encounter_id, concept_id, voided),
    KEY idx_obs_person_concept_voided (person_id, concept_id, voided),
    CONSTRAINT obs_person_fk FOREIGN KEY (person_id) REFERENCES person (person_id),
    CONSTRAINT obs_encounter_fk FOREIGN KEY (encounter_id) REFERENCES encounter (encounter_id),
    CONSTRAINT obs_group_fk FOREIGN KEY (obs_group_id) REFERENCES obs (obs_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -----------------------------------------------------------------------------
-- isanteplus_patient_arv: Stores ARV regimen info (populated by procedures)
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS isanteplus_patient_arv;
CREATE TABLE isanteplus_patient_arv (
    patient_id INT NOT NULL,
    arv_regimen VARCHAR(255) DEFAULT NULL,
    arv_status VARCHAR(255) DEFAULT NULL,
    date_started_arv DATE DEFAULT NULL,
    next_visit_date DATE DEFAULT NULL,
    date_created DATETIME DEFAULT NULL,
    date_changed DATETIME DEFAULT NULL,
    PRIMARY KEY (patient_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

SET FOREIGN_KEY_CHECKS = 1;
"""
        with open(filepath, 'w') as f:
            f.write(ddl)

        print("  OpenMRS DDL written successfully")

    def write_isanteplus_ddl(self, filename: str = 'ddl_isanteplus.sql') -> None:
        """Write iSantePlus schema DDL to SQL file."""
        filepath = f"{self.output_dir}/{filename}"
        print(f"Writing iSantePlus DDL to {filepath}...")

        ddl = """-- =============================================================================
-- iSantePlus Schema DDL for ETL Testing
-- Generated DDL with tables required for stored procedure testing
-- =============================================================================

CREATE DATABASE IF NOT EXISTS isanteplus;
USE isanteplus;

SET FOREIGN_KEY_CHECKS = 0;

-- -----------------------------------------------------------------------------
-- patient: iSantePlus patient record (denormalized from OpenMRS)
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS patient;
CREATE TABLE patient (
    patient_id INT NOT NULL,
    birthdate DATE DEFAULT NULL,
    vih_status INT DEFAULT NULL COMMENT '1=HIV+, 0=HIV-',
    arv_status INT DEFAULT NULL COMMENT 'Current ARV status (populated by procedure)',
    date_started_arv DATE DEFAULT NULL,
    next_visit_date DATE DEFAULT NULL,
    voided TINYINT(1) NOT NULL DEFAULT 0,
    last_updated_date DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (patient_id),
    KEY idx_patient_vih_status (vih_status),
    KEY idx_patient_arv_status (arv_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -----------------------------------------------------------------------------
-- patient_on_arv: Patients currently on ARV treatment
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS patient_on_arv;
CREATE TABLE patient_on_arv (
    patient_id INT NOT NULL,
    date_started DATE DEFAULT NULL,
    last_updated_date DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (patient_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -----------------------------------------------------------------------------
-- discontinuation_reason: Reasons for treatment discontinuation
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS discontinuation_reason;
CREATE TABLE discontinuation_reason (
    id INT NOT NULL AUTO_INCREMENT,
    patient_id INT NOT NULL,
    reason INT NOT NULL COMMENT 'Concept ID for discontinuation reason',
    visit_date DATE DEFAULT NULL,
    encounter_id INT DEFAULT NULL,
    last_updated_date DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY idx_discontinuation_patient (patient_id),
    KEY idx_discontinuation_reason (reason),
    KEY idx_discontinuation_patient_reason (patient_id, reason)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -----------------------------------------------------------------------------
-- patient_dispensing: ARV dispensing records
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS patient_dispensing;
CREATE TABLE patient_dispensing (
    id INT NOT NULL AUTO_INCREMENT,
    patient_id INT NOT NULL,
    encounter_id INT DEFAULT NULL,
    visit_id INT DEFAULT NULL,
    visit_date DATETIME DEFAULT NULL,
    next_dispensation_date DATE DEFAULT NULL,
    arv_drug INT DEFAULT NULL COMMENT '1065=ARV drug',
    rx_or_prophy INT DEFAULT NULL COMMENT '138405=treatment, 163768=prophylaxis',
    drug_id INT DEFAULT NULL,
    voided TINYINT(1) NOT NULL DEFAULT 0,
    location_id INT DEFAULT NULL,
    last_updated_date DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY idx_dispensing_patient (patient_id),
    KEY idx_dispensing_visit_date (visit_date),
    KEY idx_dispensing_next_date (next_dispensation_date),
    KEY idx_dispensing_arv_drug (arv_drug),
    KEY idx_dispensing_patient_arv_voided (patient_id, arv_drug, voided)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -----------------------------------------------------------------------------
-- patient_laboratory: Laboratory test results
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS patient_laboratory;
CREATE TABLE patient_laboratory (
    id INT NOT NULL AUTO_INCREMENT,
    patient_id INT NOT NULL,
    encounter_id INT DEFAULT NULL,
    test_id INT NOT NULL COMMENT 'Concept ID for test type',
    test_done TINYINT(1) DEFAULT 0,
    test_result VARCHAR(255) DEFAULT NULL,
    visit_date DATE DEFAULT NULL,
    date_test_done DATE DEFAULT NULL,
    voided TINYINT(1) NOT NULL DEFAULT 0,
    location_id INT DEFAULT NULL,
    last_updated_date DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY idx_laboratory_patient (patient_id),
    KEY idx_laboratory_test_id (test_id),
    KEY idx_laboratory_visit_date (visit_date),
    KEY idx_laboratory_patient_test_voided (patient_id, test_id, voided)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -----------------------------------------------------------------------------
-- patient_pregnancy: Pregnancy records
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS patient_pregnancy;
CREATE TABLE patient_pregnancy (
    id INT NOT NULL AUTO_INCREMENT,
    patient_id INT NOT NULL,
    pregnancy_date DATE DEFAULT NULL,
    due_date DATE DEFAULT NULL,
    outcome INT DEFAULT NULL,
    voided TINYINT(1) NOT NULL DEFAULT 0,
    last_updated_date DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY idx_pregnancy_patient (patient_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -----------------------------------------------------------------------------
-- patient_status_arv: OUTPUT TABLE - ARV status history (populated by procedure)
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS patient_status_arv;
CREATE TABLE patient_status_arv (
    id INT NOT NULL AUTO_INCREMENT,
    patient_id INT NOT NULL,
    id_status INT NOT NULL COMMENT 'ARV status code (1-11)',
    start_date DATE DEFAULT NULL,
    encounter_id INT DEFAULT NULL,
    dis_reason INT DEFAULT NULL COMMENT 'Discontinuation reason if applicable',
    last_updated_date DATETIME DEFAULT NULL,
    date_started_status DATETIME DEFAULT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_patient_status (patient_id, id_status, start_date),
    KEY idx_status_patient (patient_id),
    KEY idx_status_id (id_status),
    KEY idx_status_date (date_started_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -----------------------------------------------------------------------------
-- exposed_infants: OUTPUT TABLE - HIV-exposed infants (populated by procedure)
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS exposed_infants;
CREATE TABLE exposed_infants (
    id INT NOT NULL AUTO_INCREMENT,
    patient_id INT NOT NULL,
    location_id INT DEFAULT NULL,
    encounter_id INT DEFAULT NULL,
    visit_date DATE DEFAULT NULL,
    condition_exposee INT DEFAULT NULL COMMENT 'Exposure condition code (1-5)',
    last_updated_date DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY idx_exposed_patient (patient_id),
    KEY idx_exposed_condition (condition_exposee)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -----------------------------------------------------------------------------
-- alert: OUTPUT TABLE - Patient alerts (populated by procedure)
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS alert;
CREATE TABLE alert (
    id INT NOT NULL AUTO_INCREMENT,
    patient_id INT NOT NULL,
    id_alert INT NOT NULL COMMENT 'Alert type (1-12)',
    encounter_id INT DEFAULT NULL,
    date_alert DATE DEFAULT NULL,
    last_updated_date DATETIME DEFAULT NULL,
    PRIMARY KEY (id),
    KEY idx_alert_patient (patient_id),
    KEY idx_alert_id (id_alert)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -----------------------------------------------------------------------------
-- arv_status_loockup: Lookup table for ARV status names
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS arv_status_loockup;
CREATE TABLE arv_status_loockup (
    id INT NOT NULL,
    name_en VARCHAR(100) DEFAULT NULL,
    name_fr VARCHAR(100) DEFAULT NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- Insert ARV status lookup values
INSERT INTO arv_status_loockup (id, name_en, name_fr) VALUES
(1, 'Deceased', 'Décédé'),
(2, 'Transferred', 'Transféré'),
(3, 'Stopped', 'Arrêté'),
(4, 'Deceased Pre-ARV', 'Décédé en Pré-ARV'),
(5, 'Transferred Pre-ARV', 'Transféré en Pré-ARV'),
(6, 'Regular', 'Régulier'),
(7, 'Recent Pre-ARV', 'Récent en Pré-ARV'),
(8, 'Missed Appointment', 'Rendez-vous raté'),
(9, 'Lost to Follow-up', 'Perdu de vue'),
(10, 'Lost Pre-ARV', 'Perdu de vue en Pré-ARV'),
(11, 'Active Pre-ARV', 'Actif en Pré-ARV');

-- -----------------------------------------------------------------------------
-- patient_prescription: Drug prescriptions (for regimen calculation)
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS patient_prescription;
CREATE TABLE patient_prescription (
    id INT NOT NULL AUTO_INCREMENT,
    patient_id INT NOT NULL,
    location_id INT DEFAULT NULL,
    visit_date DATETIME DEFAULT NULL,
    drug_id INT NOT NULL,
    arv_drug INT DEFAULT NULL COMMENT '1065=ARV drug',
    rx_or_prophy INT DEFAULT NULL,
    voided TINYINT(1) NOT NULL DEFAULT 0,
    last_updated_date DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY idx_prescription_patient (patient_id),
    KEY idx_prescription_drug (drug_id),
    KEY idx_prescription_patient_visit (patient_id, visit_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -----------------------------------------------------------------------------
-- regimen: Regimen definitions (drug combinations)
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS regimen;
CREATE TABLE regimen (
    id INT NOT NULL AUTO_INCREMENT,
    drugID1 INT NOT NULL,
    drugID2 INT NOT NULL DEFAULT 0,
    drugID3 INT NOT NULL DEFAULT 0,
    shortname VARCHAR(100) DEFAULT NULL,
    description VARCHAR(255) DEFAULT NULL,
    PRIMARY KEY (id),
    KEY idx_regimen_drugs (drugID1, drugID2, drugID3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -----------------------------------------------------------------------------
-- pepfarTable: PEPFAR regimen tracking
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS pepfarTable;
CREATE TABLE pepfarTable (
    id INT NOT NULL AUTO_INCREMENT,
    location_id INT DEFAULT NULL,
    patient_id INT NOT NULL,
    visit_date DATETIME DEFAULT NULL,
    regimen VARCHAR(255) DEFAULT NULL,
    rx_or_prophy INT DEFAULT NULL,
    last_updated_date DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_pepfar_patient_visit (patient_id, visit_date),
    KEY idx_pepfar_patient (patient_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -----------------------------------------------------------------------------
-- patient_immunization: Immunization records
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS patient_immunization;
CREATE TABLE patient_immunization (
    id INT NOT NULL AUTO_INCREMENT,
    patient_id INT NOT NULL,
    location_id INT DEFAULT NULL,
    encounter_id INT DEFAULT NULL,
    vaccine_obs_group_id INT DEFAULT NULL,
    vaccine_concept_id INT DEFAULT NULL,
    encounter_date DATETIME DEFAULT NULL,
    vaccine_date DATE DEFAULT NULL,
    vaccine_uuid VARCHAR(38) DEFAULT NULL,
    dose INT DEFAULT NULL,
    voided TINYINT(1) NOT NULL DEFAULT 0,
    PRIMARY KEY (id),
    UNIQUE KEY uk_immunization_obs_group (vaccine_obs_group_id),
    KEY idx_immunization_patient (patient_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- -----------------------------------------------------------------------------
-- immunization_dose: Pivoted immunization dose data
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS immunization_dose;
CREATE TABLE immunization_dose (
    id INT NOT NULL AUTO_INCREMENT,
    patient_id INT NOT NULL,
    vaccine_concept_id INT NOT NULL,
    dose0 DATE DEFAULT NULL,
    dose1 DATE DEFAULT NULL,
    dose2 DATE DEFAULT NULL,
    dose3 DATE DEFAULT NULL,
    dose4 DATE DEFAULT NULL,
    dose5 DATE DEFAULT NULL,
    dose6 DATE DEFAULT NULL,
    dose7 DATE DEFAULT NULL,
    dose8 DATE DEFAULT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_immunization_patient_vaccine (patient_id, vaccine_concept_id)
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
    """Writes generated data directly to MySQL database."""

    def __init__(
        self,
        generator: TestDataGenerator,
        host: str,
        user: str,
        password: str,
        port: int = 3306
    ):
        if not HAS_MYSQL:
            raise RuntimeError(
                "mysql-connector-python is required for database mode. "
                "Install with: pip install mysql-connector-python"
            )

        self.gen = generator
        self.conn_params = {
            'host': host,
            'user': user,
            'password': password,
            'port': port,
        }
        self.batch_size = generator.config.batch_size

    def _get_connection(self, database: str = None):
        """Get a database connection."""
        params = self.conn_params.copy()
        if database:
            params['database'] = database
        return mysql.connector.connect(**params)

    def _execute_batch_insert(
        self,
        cursor,
        table: str,
        columns: List[str],
        rows: List[tuple],
        batch_size: int = None
    ) -> None:
        """Execute batch inserts."""
        if not rows:
            return

        batch_size = batch_size or self.batch_size
        placeholders = ', '.join(['%s'] * len(columns))
        sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"

        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            cursor.executemany(sql, batch)

    def write_openmrs_data(self) -> None:
        """Write OpenMRS data to database."""
        print("Writing OpenMRS data to database...")
        now = datetime.now()

        conn = self._get_connection('openmrs')
        cursor = conn.cursor()

        try:
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            cursor.execute("SET SQL_MODE = 'NO_AUTO_VALUE_ON_ZERO'")

            # Encounter types
            print("  Writing encounter types...")
            cursor.execute("TRUNCATE TABLE encounter_type")
            rows = [
                (et['encounter_type_id'], et['name'], et['uuid'],
                 et['creator'], et['date_created'], 0)
                for et in self.gen.encounter_types
            ]
            self._execute_batch_insert(
                cursor, 'encounter_type',
                ['encounter_type_id', 'name', 'uuid', 'creator', 'date_created', 'retired'],
                rows
            )

            # Concepts
            print("  Writing concepts...")
            cursor.execute("TRUNCATE TABLE concept")
            rows = [(c['concept_id'], c['uuid']) for c in self.gen.concepts]
            self._execute_batch_insert(cursor, 'concept', ['concept_id', 'uuid'], rows)

            # Person
            print("  Writing person records...")
            cursor.execute("TRUNCATE TABLE person")
            rows = [
                (p.person_id, p.gender, p.birthdate, 1, now, 0, p.person_uuid)
                for p in self.gen.patients
            ]
            self._execute_batch_insert(
                cursor, 'person',
                ['person_id', 'gender', 'birthdate', 'creator', 'date_created', 'voided', 'uuid'],
                rows
            )

            # Patient
            print("  Writing patient records...")
            cursor.execute("TRUNCATE TABLE patient")
            rows = [(p.patient_id, 1, now, 0) for p in self.gen.patients]
            self._execute_batch_insert(
                cursor, 'patient',
                ['patient_id', 'creator', 'date_created', 'voided'],
                rows
            )

            # Visit
            print("  Writing visit records...")
            cursor.execute("TRUNCATE TABLE visit")
            rows = [
                (v.visit_id, v.patient_id, v.date_started, v.location_id, 1, now, 0, v.uuid)
                for v in self.gen.visits
            ]
            self._execute_batch_insert(
                cursor, 'visit',
                ['visit_id', 'patient_id', 'date_started', 'location_id',
                 'creator', 'date_created', 'voided', 'uuid'],
                rows
            )

            # Encounter
            print("  Writing encounter records...")
            cursor.execute("TRUNCATE TABLE encounter")
            rows = [
                (e.encounter_id, e.encounter_type_id, e.patient_id, e.location_id,
                 e.encounter_datetime, e.visit_id, 1, now, 0, e.uuid)
                for e in self.gen.encounters
            ]
            self._execute_batch_insert(
                cursor, 'encounter',
                ['encounter_id', 'encounter_type', 'patient_id', 'location_id',
                 'encounter_datetime', 'visit_id', 'creator', 'date_created', 'voided', 'uuid'],
                rows
            )

            # Obs
            print(f"  Writing {len(self.gen.observations)} observation records...")
            cursor.execute("TRUNCATE TABLE obs")
            rows = [
                (o.obs_id, o.person_id, o.encounter_id, o.concept_id,
                 o.obs_datetime, o.location_id, o.value_coded, o.value_numeric,
                 o.value_datetime, o.obs_group_id, 1, now, 0, o.uuid)
                for o in self.gen.observations
            ]
            self._execute_batch_insert(
                cursor, 'obs',
                ['obs_id', 'person_id', 'encounter_id', 'concept_id',
                 'obs_datetime', 'location_id', 'value_coded', 'value_numeric',
                 'value_datetime', 'obs_group_id', 'creator', 'date_created', 'voided', 'uuid'],
                rows
            )

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
        """Write iSantePlus data to database."""
        print("Writing iSantePlus data to database...")

        conn = self._get_connection('isanteplus')
        cursor = conn.cursor()

        try:
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0")

            # Patient
            print("  Writing patient records...")
            cursor.execute("TRUNCATE TABLE patient")
            rows = [
                (p.patient_id, p.birthdate,
                 1 if p.is_hiv_positive else 0,
                 None, p.date_started_arv, 0)
                for p in self.gen.patients
            ]
            self._execute_batch_insert(
                cursor, 'patient',
                ['patient_id', 'birthdate', 'vih_status', 'arv_status',
                 'date_started_arv', 'voided'],
                rows
            )

            # patient_on_arv
            print("  Writing patient_on_arv records...")
            cursor.execute("TRUNCATE TABLE patient_on_arv")
            rows = [(pid,) for pid in self.gen.patient_on_arv]
            if rows:
                self._execute_batch_insert(cursor, 'patient_on_arv', ['patient_id'], rows)

            # discontinuation_reason
            print("  Writing discontinuation_reason records...")
            cursor.execute("TRUNCATE TABLE discontinuation_reason")
            rows = [
                (dr['patient_id'], dr['reason'], dr['visit_date'])
                for dr in self.gen.discontinuation_reasons
            ]
            if rows:
                self._execute_batch_insert(
                    cursor, 'discontinuation_reason',
                    ['patient_id', 'reason', 'visit_date'],
                    rows
                )

            # patient_dispensing
            print(f"  Writing {len(self.gen.patient_dispensing)} patient_dispensing records...")
            cursor.execute("TRUNCATE TABLE patient_dispensing")
            rows = [
                (pd['patient_id'], pd['encounter_id'], pd['visit_id'],
                 pd['visit_date'], pd['next_dispensation_date'], pd['arv_drug'],
                 pd['rx_or_prophy'], pd['drug_id'], pd['voided'], pd['location_id'])
                for pd in self.gen.patient_dispensing
            ]
            if rows:
                self._execute_batch_insert(
                    cursor, 'patient_dispensing',
                    ['patient_id', 'encounter_id', 'visit_id', 'visit_date',
                     'next_dispensation_date', 'arv_drug', 'rx_or_prophy',
                     'drug_id', 'voided', 'location_id'],
                    rows
                )

            # patient_laboratory
            print(f"  Writing {len(self.gen.patient_laboratory)} patient_laboratory records...")
            cursor.execute("TRUNCATE TABLE patient_laboratory")
            rows = [
                (pl['patient_id'], pl['encounter_id'], pl['test_id'],
                 pl['test_done'], pl['test_result'], pl['visit_date'],
                 pl['date_test_done'], pl['voided'])
                for pl in self.gen.patient_laboratory
            ]
            if rows:
                self._execute_batch_insert(
                    cursor, 'patient_laboratory',
                    ['patient_id', 'encounter_id', 'test_id', 'test_done',
                     'test_result', 'visit_date', 'date_test_done', 'voided'],
                    rows
                )

            # patient_pregnancy
            print("  Writing patient_pregnancy records...")
            cursor.execute("TRUNCATE TABLE patient_pregnancy")
            rows = [(pp['patient_id'],) for pp in self.gen.patient_pregnancy]
            if rows:
                self._execute_batch_insert(cursor, 'patient_pregnancy', ['patient_id'], rows)

            # Clear output tables
            print("  Clearing output tables...")
            cursor.execute("TRUNCATE TABLE patient_status_arv")
            cursor.execute("TRUNCATE TABLE exposed_infants")
            cursor.execute("TRUNCATE TABLE alert")

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
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Generate test data for iSantePlus ETL stored procedures',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument(
        '--patients', '-n',
        type=int,
        default=100000,
        help='Number of patients to generate (default: 100000)'
    )

    parser.add_argument(
        '--seed', '-s',
        type=int,
        default=None,
        help='Random seed for reproducibility (default: random)'
    )

    parser.add_argument(
        '--batch-size', '-b',
        type=int,
        default=10000,
        help='Batch size for database inserts (default: 10000)'
    )

    # Database connection options
    db_group = parser.add_argument_group('Database connection')
    db_group.add_argument(
        '--host', '-H',
        help='MySQL host (required for database mode)'
    )
    db_group.add_argument(
        '--port', '-P',
        type=int,
        default=3306,
        help='MySQL port (default: 3306)'
    )
    db_group.add_argument(
        '--user', '-u',
        help='MySQL username'
    )
    db_group.add_argument(
        '--password', '-p',
        help='MySQL password'
    )

    # Output options
    output_group = parser.add_argument_group('Output options')
    output_group.add_argument(
        '--sql-output', '-o',
        action='store_true',
        help='Generate SQL data files (INSERT statements)'
    )
    output_group.add_argument(
        '--ddl-output',
        action='store_true',
        help='Generate DDL files (CREATE TABLE statements)'
    )
    output_group.add_argument(
        '--output-dir', '-d',
        default='.',
        help='Directory for SQL output files (default: current directory)'
    )

    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()

    # Validate arguments
    db_mode = args.host is not None
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

    # Write DDL files if requested (doesn't require data generation)
    if ddl_mode:
        ddl_writer = DDLWriter(args.output_dir)
        ddl_writer.write_openmrs_ddl()
        ddl_writer.write_isanteplus_ddl()

    # Only generate data if needed for SQL or database mode
    if sql_mode or db_mode:
        # Create configuration
        config = GeneratorConfig(
            num_patients=args.patients,
            seed=args.seed,
            batch_size=args.batch_size,
        )

        # Generate data
        generator = TestDataGenerator(config)
        generator.generate()

        # Write SQL files if requested
        if sql_mode:
            sql_writer = SQLWriter(generator, args.output_dir)
            sql_writer.write_openmrs_data()
            sql_writer.write_isanteplus_data()

        # Write to database if requested
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
        print(f"  - {args.output_dir}/ddl_openmrs.sql")
        print(f"  - {args.output_dir}/ddl_isanteplus.sql")
    if sql_mode or db_mode:
        print("\nTo test the stored procedures:")
        print("  1. Run DDL files first to create tables (if needed)")
        print("  2. Run data files or use database mode to populate test data")
        print("  3. Execute: CALL patient_status_arv();")
        print("  4. Execute: CALL alert_viral_load();")
        print("  5. Query patient_status_arv, exposed_infants, and alert tables for results")


if __name__ == '__main__':
    main()
