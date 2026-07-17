"""Generate deterministic operational activity in the synthetic source database."""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import sys
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from datetime import time as datetime_time
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Callable

import psycopg2
from faker import Faker
from psycopg2.extensions import connection as Connection
from psycopg2.extras import Json, RealDictCursor

RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,80}$")
BRANCH_CODES = ("IST001", "ANK001", "IZM001")
PRODUCT_CODES = ("CONSUMER_TRY", "MORTGAGE_TRY", "SME_WORKING_TRY")
PRODUCT_RATES = {
    "CONSUMER_TRY": Decimal("0.420000"),
    "MORTGAGE_TRY": Decimal("0.360000"),
    "SME_WORKING_TRY": Decimal("0.390000"),
}
PRODUCT_TERMS = {"CONSUMER_TRY": 12, "MORTGAGE_TRY": 24, "SME_WORKING_TRY": 18}


@dataclass(frozen=True)
class WorkloadConfig:
    mode: str
    run_id: str
    seed: int
    base_date: date
    customer_count: int
    application_count: int
    installments_per_loan: int

    def audit_config(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["base_date"] = self.base_date.isoformat()
        return payload


EventAction = Callable[[Connection], dict[str, Any]]


def log(message: str, **details: Any) -> None:
    print(json.dumps({"message": message, **details}, default=str, sort_keys=True))


def utc_at(day: date, hour: int = 9) -> datetime:
    return datetime.combine(day, datetime_time(hour=hour), tzinfo=timezone.utc)


def money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def connect_with_retry() -> Connection:
    parameters = {
        "host": os.getenv("SOURCE_DB_HOST", "core-banking-source"),
        "port": int(os.getenv("SOURCE_DB_PORT", "5432")),
        "dbname": os.getenv("SOURCE_DB_NAME", "core_banking"),
        "user": os.getenv("SOURCE_DB_USER", "core_banking"),
        "password": os.getenv("SOURCE_DB_PASSWORD", "core_banking_local"),
        "connect_timeout": 5,
        "application_name": "synthetic-core-banking-workload",
    }
    last_error: Exception | None = None
    for attempt in range(1, 11):
        try:
            connection = psycopg2.connect(**parameters)
            connection.autocommit = False
            return connection
        except psycopg2.OperationalError as error:
            last_error = error
            log("database_connection_retry", attempt=attempt)
            time.sleep(min(attempt, 5))
    raise RuntimeError("Source database did not become available") from last_error


def validate_control_schema(connection: Connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute("SELECT to_regclass('simulator.workload_runs')")
        if cursor.fetchone()[0] is None:
            raise RuntimeError(
                "Missing simulator control tables. Apply source/init/004_create_workload_control.sql "
                "or recreate the local source volume."
            )
    connection.commit()


def start_run(connection: Connection, config: WorkloadConfig) -> bool:
    audit_config = config.audit_config()
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(
            "SELECT workload_type, random_seed, status, config "
            "FROM simulator.workload_runs WHERE run_id = %s",
            (config.run_id,),
        )
        existing = cursor.fetchone()
        if existing:
            if existing["workload_type"] != config.mode or existing["random_seed"] != config.seed:
                raise ValueError(f"Run ID {config.run_id!r} already belongs to a different workload")
            if existing["config"] != audit_config:
                raise ValueError(f"Run ID {config.run_id!r} was previously used with different options")
            if existing["status"] == "COMPLETED":
                connection.commit()
                log("run_already_completed", run_id=config.run_id)
                return False
            cursor.execute(
                "UPDATE simulator.workload_runs SET status = 'RUNNING', "
                "error_message = NULL, completed_at = NULL WHERE run_id = %s",
                (config.run_id,),
            )
        else:
            cursor.execute(
                """
                INSERT INTO simulator.workload_runs (
                    run_id, workload_type, random_seed, status, config
                ) VALUES (%s, %s, %s, 'RUNNING', %s)
                """,
                (config.run_id, config.mode, config.seed, Json(audit_config)),
            )
    connection.commit()
    log("run_started", mode=config.mode, run_id=config.run_id, seed=config.seed)
    return True


def finish_run(connection: Connection, run_id: str) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE simulator.workload_runs SET status = 'COMPLETED', "
            "completed_at = CURRENT_TIMESTAMP, error_message = NULL WHERE run_id = %s",
            (run_id,),
        )
    connection.commit()
    log("run_completed", run_id=run_id)


def fail_run(connection: Connection, run_id: str, error: Exception) -> None:
    connection.rollback()
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE simulator.workload_runs SET status = 'FAILED', error_message = %s "
            "WHERE run_id = %s",
            (str(error)[:2000], run_id),
        )
    connection.commit()


def execute_event(
    connection: Connection,
    config: WorkloadConfig,
    event_key: str,
    scenario: str,
    expected_result: str,
    action: EventAction,
) -> None:
    event_id = f"{config.run_id}:{event_key}"
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT status FROM simulator.workload_events WHERE run_id = %s AND event_key = %s",
            (config.run_id, event_key),
        )
        existing = cursor.fetchone()
        if existing and existing[0] == "COMPLETED":
            connection.commit()
            log("event_already_completed", event_key=event_key, run_id=config.run_id)
            return
        cursor.execute(
            """
            INSERT INTO simulator.workload_events (
                event_id, run_id, event_key, scenario, status, expected_result
            ) VALUES (%s, %s, %s, %s, 'RUNNING', %s)
            ON CONFLICT (run_id, event_key) DO UPDATE
            SET status = 'RUNNING', error_message = NULL, completed_at = NULL
            """,
            (event_id, config.run_id, event_key, scenario, expected_result),
        )
    connection.commit()

    try:
        actual_result = action(connection)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE simulator.workload_events
                SET status = 'COMPLETED', actual_result = %s,
                    completed_at = CURRENT_TIMESTAMP, error_message = NULL
                WHERE run_id = %s AND event_key = %s
                """,
                (Json(actual_result), config.run_id, event_key),
            )
        connection.commit()
        log("event_completed", actual_result=actual_result, event_key=event_key, run_id=config.run_id)
    except Exception as error:
        connection.rollback()
        with connection.cursor() as cursor:
            cursor.execute(
                "UPDATE simulator.workload_events SET status = 'FAILED', error_message = %s "
                "WHERE run_id = %s AND event_key = %s",
                (str(error)[:2000], config.run_id, event_key),
            )
        connection.commit()
        raise


def customer_no(seed: int, index: int) -> str:
    return f"C{seed:06d}{index:08d}"


def application_no(seed: int, index: int) -> str:
    return f"A{seed:06d}{index:08d}"


def loan_no(seed: int, index: int) -> str:
    return f"L{seed:06d}{index:08d}"


def insert_installments(
    cursor: Any,
    loan_id: int,
    principal: Decimal,
    annual_rate: Decimal,
    installment_count: int,
    first_due_date: date,
    event_time: datetime,
) -> int:
    principal_part = money(principal / installment_count)
    interest_part = money(principal_part * (annual_rate / Decimal("12")))
    for number in range(1, installment_count + 1):
        cursor.execute(
            """
            INSERT INTO krd.installments (
                loan_id, installment_no, due_date, principal_amount,
                interest_amount, paid_amount, status_code, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, 0, 'PENDING', %s, %s)
            """,
            (
                loan_id,
                number,
                first_due_date + timedelta(days=30 * (number - 1)),
                principal_part,
                interest_part,
                event_time,
                event_time,
            ),
        )
    return installment_count


def snapshot_action(config: WorkloadConfig) -> EventAction:
    def action(connection: Connection) -> dict[str, Any]:
        rng = random.Random(config.seed)
        fake = Faker("tr_TR")
        fake.seed_instance(config.seed)
        base_time = utc_at(config.base_date)
        customer_ids: list[int] = []
        counters = {key: 0 for key in (
            "customers", "addresses", "contacts", "relations", "applications",
            "loans", "installments", "collaterals"
        )}

        with connection.cursor() as cursor:
            for index in range(1, config.customer_count + 1):
                number = customer_no(config.seed, index)
                corporate = index % 10 == 0
                first_name = None if corporate else fake.first_name()
                last_name = None if corporate else fake.last_name()
                legal_name = fake.company() if corporate else None
                national_id = None if corporate else f"SYN{config.seed:06d}{index:08d}"
                tax_id = f"TAX{config.seed:06d}{index:08d}" if corporate else None
                birth_date = None if corporate else config.base_date - timedelta(
                    days=(20 + rng.randrange(50)) * 365 + rng.randrange(365)
                )
                created_at = base_time - timedelta(days=365 + rng.randrange(2500))
                cursor.execute(
                    """
                    INSERT INTO mms.customers (
                        customer_no, customer_type, first_name, last_name, legal_name,
                        national_id, tax_id, date_of_birth, segment_code, status_code,
                        home_branch_code, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'ACTIVE', %s, %s, %s)
                    RETURNING customer_id
                    """,
                    (
                        number, "CORPORATE" if corporate else "INDIVIDUAL", first_name,
                        last_name, legal_name, national_id, tax_id, birth_date,
                        "SME" if corporate else rng.choice(("RETAIL", "MASS_AFFLUENT", "PRIVATE")),
                        BRANCH_CODES[(index - 1) % len(BRANCH_CODES)], created_at, created_at,
                    ),
                )
                customer_id = cursor.fetchone()[0]
                customer_ids.append(customer_id)
                counters["customers"] += 1

                cursor.execute(
                    """
                    INSERT INTO mms.customer_addresses (
                        customer_id, address_type, address_line, district, city,
                        postal_code, is_primary, valid_from, created_at, updated_at
                    ) VALUES (%s, 'HOME', %s, %s, %s, %s, TRUE, %s, %s, %s)
                    """,
                    (
                        customer_id, fake.street_address(), fake.city_suffix(), fake.city(),
                        f"{34000 + index % 1000:05d}", created_at.date(), created_at, created_at,
                    ),
                )
                counters["addresses"] += 1

                contacts = (
                    ("EMAIL", f"{number.lower()}@example.invalid", True),
                    ("PHONE", f"+90555{index:07d}", index % 3 != 0),
                )
                for contact_type, contact_value, verified in contacts:
                    cursor.execute(
                        """
                        INSERT INTO mms.customer_contacts (
                            customer_id, contact_type, contact_value, is_primary,
                            is_verified, created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            customer_id, contact_type, contact_value, contact_type == "EMAIL",
                            verified, created_at, created_at,
                        ),
                    )
                    counters["contacts"] += 1

            for index in range(10, len(customer_ids), 20):
                cursor.execute(
                    """
                    INSERT INTO mms.customer_relations (
                        source_customer_id, target_customer_id, relation_type,
                        valid_from, created_at, updated_at
                    ) VALUES (%s, %s, 'AUTHORIZED_REPRESENTATIVE', %s, %s, %s)
                    """,
                    (
                        customer_ids[index - 1], customer_ids[index],
                        config.base_date - timedelta(days=180), base_time, base_time,
                    ),
                )
                counters["relations"] += 1

            for index in range(1, config.application_count + 1):
                number = application_no(config.seed, index)
                customer_number = customer_no(config.seed, ((index - 1) % config.customer_count) + 1)
                product_code = PRODUCT_CODES[(index - 1) % len(PRODUCT_CODES)]
                branch_code = BRANCH_CODES[(index - 1) % len(BRANCH_CODES)]
                term_months = PRODUCT_TERMS[product_code]
                requested_amount = money(Decimal(25000 + index * 2750))
                applied_at = base_time - timedelta(days=config.application_count - index + 30)
                status = "REJECTED" if index % 5 == 0 else "PENDING" if index % 4 == 0 else "APPROVED"
                decision_at = None if status == "PENDING" else applied_at + timedelta(days=1)
                cursor.execute(
                    """
                    INSERT INTO krd.loan_applications (
                        application_no, customer_no, product_code, branch_code,
                        requested_amount, currency_code, term_months, status_code,
                        applied_at, decision_at, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, 'TRY', %s, %s, %s, %s, %s, %s)
                    RETURNING application_id
                    """,
                    (
                        number, customer_number, product_code, branch_code, requested_amount,
                        term_months, status, applied_at, decision_at, applied_at, decision_at or applied_at,
                    ),
                )
                application_id = cursor.fetchone()[0]
                counters["applications"] += 1
                if status != "APPROVED":
                    continue

                disbursed_at = decision_at + timedelta(days=1)
                cursor.execute(
                    """
                    INSERT INTO krd.loans (
                        loan_no, application_id, customer_no, product_code, branch_code,
                        principal_amount, currency_code, annual_interest_rate, term_months,
                        status_code, disbursed_at, maturity_date, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, 'TRY', %s, %s,
                              'ACTIVE', %s, %s, %s, %s) RETURNING loan_id
                    """,
                    (
                        loan_no(config.seed, index), application_id, customer_number, product_code,
                        branch_code, requested_amount, PRODUCT_RATES[product_code], term_months,
                        disbursed_at, disbursed_at.date() + timedelta(days=30 * term_months),
                        disbursed_at, disbursed_at,
                    ),
                )
                loan_id = cursor.fetchone()[0]
                counters["loans"] += 1
                counters["installments"] += insert_installments(
                    cursor, loan_id, requested_amount, PRODUCT_RATES[product_code],
                    config.installments_per_loan, disbursed_at.date() + timedelta(days=30), disbursed_at,
                )
                if product_code != "CONSUMER_TRY":
                    cursor.execute(
                        """
                        INSERT INTO krd.collaterals (
                            loan_id, collateral_type, collateral_reference,
                            appraised_value, currency_code, status_code, created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, 'TRY', 'ACTIVE', %s, %s)
                        """,
                        (
                            loan_id,
                            "REAL_ESTATE" if product_code == "MORTGAGE_TRY" else "COMMERCIAL_GUARANTEE",
                            f"COL-{config.seed:06d}-{index:08d}",
                            money(requested_amount * Decimal("1.40")), disbursed_at, disbursed_at,
                        ),
                    )
                    counters["collaterals"] += 1
        return counters

    return action


def customer_change_action(config: WorkloadConfig) -> EventAction:
    def action(connection: Connection) -> dict[str, Any]:
        change_date = config.base_date + timedelta(days=1)
        change_time = utc_at(change_date, 10)
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT c.customer_id, c.customer_no FROM mms.customers c
                WHERE c.status_code = 'ACTIVE'
                  AND NOT EXISTS (
                    SELECT 1 FROM krd.loans l
                    WHERE l.customer_no = c.customer_no AND l.status_code <> 'CLOSED'
                ) ORDER BY c.customer_no DESC LIMIT 1 FOR UPDATE
                """
            )
            customer = cursor.fetchone()
            if not customer:
                raise RuntimeError("No customer is available for the profile change scenario")
            cursor.execute(
                "UPDATE mms.customers SET status_code = 'PASSIVE', "
                "segment_code = 'REVIEW_REQUIRED', updated_at = %s WHERE customer_id = %s",
                (change_time, customer["customer_id"]),
            )
            customer_updates = cursor.rowcount
            cursor.execute(
                """
                UPDATE mms.customer_addresses
                SET is_primary = FALSE, valid_to = %s, updated_at = %s
                WHERE customer_id = %s AND is_primary = TRUE AND valid_to IS NULL
                """,
                (change_date - timedelta(days=1), change_time, customer["customer_id"]),
            )
            closed_addresses = cursor.rowcount
            cursor.execute(
                """
                INSERT INTO mms.customer_addresses (
                    customer_id, address_type, address_line, district, city,
                    postal_code, is_primary, valid_from, created_at, updated_at
                ) VALUES (%s, 'HOME', 'Synthetic Change Address 1', 'Cankaya',
                          'Ankara', '06000', TRUE, %s, %s, %s)
                """,
                (customer["customer_id"], change_date, change_time, change_time),
            )
        return {
            "customer_no": customer["customer_no"], "customer_updates": customer_updates,
            "closed_addresses": closed_addresses, "inserted_addresses": 1,
        }
    return action


def contact_delete_action(config: WorkloadConfig) -> EventAction:
    del config

    def action(connection: Connection) -> dict[str, Any]:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT cc.contact_id, c.customer_no
                FROM mms.customer_contacts cc
                JOIN mms.customers c ON c.customer_id = cc.customer_id
                WHERE cc.contact_type = 'PHONE' AND cc.is_primary = FALSE
                ORDER BY c.customer_no DESC LIMIT 1 FOR UPDATE OF cc
                """
            )
            contact = cursor.fetchone()
            if not contact:
                raise RuntimeError("No secondary phone contact is available for deletion")
            cursor.execute("DELETE FROM mms.customer_contacts WHERE contact_id = %s", (contact["contact_id"],))
            deleted_contacts = cursor.rowcount
        return {"customer_no": contact["customer_no"], "deleted_contacts": deleted_contacts}
    return action


def loan_approval_action(config: WorkloadConfig) -> EventAction:
    def action(connection: Connection) -> dict[str, Any]:
        event_time = utc_at(config.base_date + timedelta(days=2), 11)
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT application_id, application_no, customer_no, product_code,
                       branch_code, requested_amount, currency_code, term_months
                FROM krd.loan_applications WHERE status_code = 'PENDING'
                ORDER BY application_no LIMIT 1 FOR UPDATE
                """
            )
            application = cursor.fetchone()
            if not application:
                raise RuntimeError("No pending loan application is available for approval")
            cursor.execute(
                "UPDATE krd.loan_applications SET status_code = 'APPROVED', "
                "decision_at = %s, updated_at = %s WHERE application_id = %s",
                (event_time, event_time, application["application_id"]),
            )
            cursor.execute(
                """
                SELECT annual_rate FROM prm.rate_parameters
                WHERE product_code = %s AND effective_from <= %s
                  AND (effective_to IS NULL OR effective_to >= %s)
                ORDER BY effective_from DESC LIMIT 1
                """,
                (application["product_code"], event_time.date(), event_time.date()),
            )
            rate_row = cursor.fetchone()
            if not rate_row:
                raise RuntimeError(f"No effective rate for {application['product_code']}")
            generated_loan_no = f"LC-{application['application_no']}"
            cursor.execute(
                """
                INSERT INTO krd.loans (
                    loan_no, application_id, customer_no, product_code, branch_code,
                    principal_amount, currency_code, annual_interest_rate, term_months,
                    status_code, disbursed_at, maturity_date, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                          'ACTIVE', %s, %s, %s, %s) RETURNING loan_id
                """,
                (
                    generated_loan_no, application["application_id"], application["customer_no"],
                    application["product_code"], application["branch_code"],
                    application["requested_amount"], application["currency_code"],
                    rate_row["annual_rate"], application["term_months"], event_time,
                    event_time.date() + timedelta(days=30 * application["term_months"]),
                    event_time, event_time,
                ),
            )
            loan_id = cursor.fetchone()["loan_id"]
            installment_count = insert_installments(
                cursor, loan_id, application["requested_amount"], rate_row["annual_rate"],
                config.installments_per_loan, event_time.date() + timedelta(days=30), event_time,
            )
        return {
            "application_no": application["application_no"], "approved_applications": 1,
            "inserted_loans": 1, "inserted_installments": installment_count,
            "loan_no": generated_loan_no,
        }
    return action


def installment_change_action(config: WorkloadConfig) -> EventAction:
    def action(connection: Connection) -> dict[str, Any]:
        event_time = utc_at(config.base_date + timedelta(days=3), 12)
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                "SELECT loan_id, loan_no FROM krd.loans WHERE status_code = 'ACTIVE' "
                "ORDER BY loan_no LIMIT 1 FOR UPDATE"
            )
            loan = cursor.fetchone()
            if not loan:
                raise RuntimeError("No active loan is available for installment changes")
            cursor.execute(
                """
                SELECT installment_id, installment_no FROM krd.installments
                WHERE loan_id = %s AND status_code = 'PENDING'
                ORDER BY installment_no LIMIT 2 FOR UPDATE
                """,
                (loan["loan_id"],),
            )
            installments = cursor.fetchall()
            if len(installments) < 2:
                raise RuntimeError("The selected loan needs at least two pending installments")
            cursor.execute(
                """
                UPDATE krd.installments
                SET paid_amount = principal_amount + interest_amount,
                    status_code = 'PAID', paid_at = %s, updated_at = %s
                WHERE installment_id = %s
                """,
                (event_time, event_time, installments[0]["installment_id"]),
            )
            paid_updates = cursor.rowcount
            cursor.execute(
                "UPDATE krd.installments SET status_code = 'OVERDUE', updated_at = %s "
                "WHERE installment_id = %s",
                (event_time, installments[1]["installment_id"]),
            )
            overdue_updates = cursor.rowcount
            cursor.execute(
                "UPDATE krd.loans SET status_code = 'DELINQUENT', updated_at = %s WHERE loan_id = %s",
                (event_time, loan["loan_id"]),
            )
            loan_updates = cursor.rowcount
        return {
            "loan_no": loan["loan_no"], "loan_updates": loan_updates,
            "overdue_installments": overdue_updates, "paid_installments": paid_updates,
        }
    return action


def rate_change_action(config: WorkloadConfig) -> EventAction:
    def action(connection: Connection) -> dict[str, Any]:
        event_time = utc_at(config.base_date + timedelta(days=4), 13)
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT rate_parameter_id, product_code, rate_type, annual_rate, effective_from
                FROM prm.rate_parameters
                WHERE product_code = 'CONSUMER_TRY' AND rate_type = 'BASE' AND effective_to IS NULL
                ORDER BY effective_from DESC LIMIT 1 FOR UPDATE
                """
            )
            current_rate = cursor.fetchone()
            if not current_rate:
                raise RuntimeError("No open CONSUMER_TRY base rate is available")
            next_effective_from = max(
                config.base_date + timedelta(days=30),
                current_rate["effective_from"] + timedelta(days=30),
            )
            next_rate = Decimal(current_rate["annual_rate"]) + Decimal("0.015000")
            cursor.execute(
                "UPDATE prm.rate_parameters SET effective_to = %s, updated_at = %s "
                "WHERE rate_parameter_id = %s",
                (next_effective_from - timedelta(days=1), event_time, current_rate["rate_parameter_id"]),
            )
            closed_rates = cursor.rowcount
            cursor.execute(
                """
                INSERT INTO prm.rate_parameters (
                    product_code, rate_type, annual_rate, effective_from, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    current_rate["product_code"], current_rate["rate_type"], next_rate,
                    next_effective_from, event_time, event_time,
                ),
            )
        return {
            "closed_rates": closed_rates, "inserted_rates": 1,
            "new_annual_rate": str(next_rate),
            "new_effective_from": next_effective_from.isoformat(),
            "product_code": current_rate["product_code"],
        }
    return action


def run_snapshot(connection: Connection, config: WorkloadConfig) -> None:
    execute_event(
        connection, config, "initial_snapshot",
        "Deterministic initial customer and lending snapshot",
        f"Insert {config.customer_count} customers and {config.application_count} "
        "referentially consistent loan applications",
        snapshot_action(config),
    )


def run_changes(connection: Connection, config: WorkloadConfig) -> None:
    events = (
        (
            "customer_address_status_change", "Customer address and status change",
            "Update one customer, close one address and insert its replacement",
            customer_change_action(config),
        ),
        (
            "secondary_contact_delete", "Customer contact deletion",
            "Delete one non-primary phone contact", contact_delete_action(config),
        ),
        (
            "loan_application_approval", "Loan application approval and disbursement",
            "Approve one pending application and insert its loan and installments",
            loan_approval_action(config),
        ),
        (
            "installment_payment_and_delinquency", "Installment payment and delinquency",
            "Mark one installment paid, one overdue and its loan delinquent",
            installment_change_action(config),
        ),
        (
            "base_rate_change", "Effective-dated lending rate change",
            "Close the current rate and insert one new effective rate", rate_change_action(config),
        ),
    )
    for event_key, scenario, expected_result, action in events:
        execute_event(connection, config, event_key, scenario, expected_result, action)


def parse_args() -> WorkloadConfig:
    parser = argparse.ArgumentParser(
        description="Generate deterministic transactions in the synthetic core-banking source."
    )
    parser.add_argument("mode", choices=("snapshot", "changes"))
    parser.add_argument("--run-id")
    parser.add_argument("--seed", type=int, default=int(os.getenv("WORKLOAD_SEED", "42")))
    parser.add_argument(
        "--base-date", type=date.fromisoformat,
        default=date.fromisoformat(os.getenv("WORKLOAD_BASE_DATE", "2026-01-15")),
    )
    parser.add_argument("--customer-count", type=int, default=100)
    parser.add_argument("--application-count", type=int, default=40)
    parser.add_argument("--installments-per-loan", type=int, default=12)
    args = parser.parse_args()
    run_id = args.run_id or f"{args.mode}-seed-{args.seed}-v1"
    if not RUN_ID_PATTERN.fullmatch(run_id):
        parser.error("--run-id must contain only letters, numbers, dot, underscore or dash (max 80)")
    if not 0 <= args.seed <= 999999:
        parser.error("--seed must be between 0 and 999999")
    if args.customer_count < 2:
        parser.error("--customer-count must be at least 2")
    if not 1 <= args.application_count <= args.customer_count:
        parser.error("--application-count must be between 1 and customer-count")
    if not 2 <= args.installments_per_loan <= 120:
        parser.error("--installments-per-loan must be between 2 and 120")
    return WorkloadConfig(
        mode=args.mode, run_id=run_id, seed=args.seed, base_date=args.base_date,
        customer_count=args.customer_count, application_count=args.application_count,
        installments_per_loan=args.installments_per_loan,
    )


def main() -> int:
    config = parse_args()
    connection: Connection | None = None
    try:
        connection = connect_with_retry()
        validate_control_schema(connection)
        if not start_run(connection, config):
            return 0
        if config.mode == "snapshot":
            run_snapshot(connection, config)
        else:
            run_changes(connection, config)
        finish_run(connection, config.run_id)
        return 0
    except Exception as error:
        if connection is not None:
            try:
                fail_run(connection, config.run_id, error)
            except Exception:
                connection.rollback()
        log("run_failed", error=str(error), run_id=config.run_id)
        return 1
    finally:
        if connection is not None:
            connection.close()


if __name__ == "__main__":
    sys.exit(main())
