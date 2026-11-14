from datetime import date, datetime, time, timedelta
from itertools import groupby
from statistics import median

from .applications import minor
from .assessment import finish_decision
from .calendar import ZONE, holidays, underwriting_deadline
from .store import ApiError, lock_application, one, require

CARD = """SELECT a.*,s.requested_amount_rub,s.requested_term_months
          FROM application a JOIN submission_snapshot s USING (application_id)
          WHERE a.status='UNDERWRITING'"""


def deadline(db, row):
    return row["underwriting_deadline_at"] or underwriting_deadline(row["underwriting_entered_at"], holidays(db))


def view(db, row):
    return {
        "application_id": row["application_id"], "status": row["status"], "submitted_at": row["submitted_at"],
        "requested_amount_minor": minor(row["requested_amount_rub"]), "requested_term_months": row["requested_term_months"],
        "sla_deadline_at": deadline(db, row), "escalated_at": row["underwriting_escalated_at"],
    }


def list_underwriting_queue(ctx):
    rows = ctx.db.execute(CARD + " ORDER BY underwriting_entered_at,application_id")
    return 200, [view(ctx.db, row) for row in rows]


def get_underwriting_application(ctx, application_id):
    return 200, view(ctx.db, require(one(ctx.db, CARD + " AND a.application_id=%s", (application_id,))))


def record_manual_decision(ctx, application_id):
    db = ctx.db
    app = lock_application(db, application_id)
    if app["status"] != "UNDERWRITING":
        raise ApiError(409, "UNDERWRITING_STATE_CONFLICT")
    command = ctx.body["command"]
    reason = ctx.body.get("rejection_reason_code")
    params = None
    if command == "APPROVE":
        if reason:
            raise ApiError(422, "REJECTION_REASON_NOT_ALLOWED")
        recommendation = one(db, "SELECT recommendation FROM evaluation WHERE application_id=%s", (application_id,))
        if not recommendation or not recommendation["recommendation"]:
            raise ApiError(503, "SERVICE_UNAVAILABLE")
        params = recommendation["recommendation"]
    receipt = finish_decision(db, app, ctx.clock, params, reason, ctx.actor["principal"], command)
    if receipt is None:
        # Ответ проходит штатный коммит, чтобы сохранить переход в SYSTEM_ERROR.
        return 503, ApiError(503, "SERVICE_UNAVAILABLE").body
    return 200, receipt


def escalate(db, app_id, clock):
    row = lock_application(db, app_id)
    at = clock()
    cutoff = deadline(db, row)
    exit_time = row["underwriting_exited_at"]
    if at > cutoff and (exit_time is None or exit_time > cutoff):
        db.execute("""UPDATE application SET underwriting_escalated_at=%s
                      WHERE application_id=%s AND underwriting_escalated_at IS NULL""", (at, app_id))


def list_underwriting_escalations(ctx):
    rows = ctx.db.execute("""SELECT * FROM application WHERE underwriting_escalated_at IS NOT NULL
                             ORDER BY underwriting_escalated_at,application_id""")
    return 200, [dict(application_id=row["application_id"], sla_deadline_at=deadline(ctx.db, row),
                      escalated_at=row["underwriting_escalated_at"], application_status=row["status"]) for row in rows]


def get_weekly_underwriting_report(ctx, week_start, as_of):
    start = date.fromisoformat(week_start)
    if start.weekday():
        raise ApiError(422, "INVALID_REPORT_WEEK")
    first = datetime.combine(start, time(), ZONE)
    end = first + timedelta(days=7)
    cutoff = datetime.fromisoformat(as_of.upper())
    if cutoff < end:
        raise ApiError(422, "INVALID_REPORT_CUTOFF")
    rows = ctx.db.execute("""
        SELECT d.underwriter_id,d.outcome,d.decided_at-a.submitted_at AS duration,
               CASE WHEN f.result='POSTED' AND f.recorded_at <= %(cutoff)s AND o.observation_ends_at <= %(cutoff)s
                    THEN o.is_overdue END AS overdue
        FROM final_decision d JOIN application a USING (application_id)
        LEFT JOIN contract c USING (application_id) LEFT JOIN loan_observation o USING (contract_id)
        LEFT JOIN disbursement_instruction i USING (contract_id) LEFT JOIN abs_final_result f USING (instruction_id)
        WHERE d.decision_source='MANUAL' AND d.decided_at >= %(first)s AND d.decided_at < %(end)s
        ORDER BY d.underwriter_id
        """, dict(first=first, end=end, cutoff=cutoff))
    metrics = []
    for operator, entries in groupby(rows, key=lambda row: row["underwriter_id"]):
        decisions = list(entries)
        observed = [row["overdue"] for row in decisions if row["overdue"] is not None]
        metrics.append(dict(underwriter_id=operator, decision_count=len(decisions),
                            median_time_to_decision_seconds=median(r["duration"].total_seconds() for r in decisions),
                            approval_rate=sum(r["outcome"] == "APPROVED" for r in decisions) / len(decisions),
                            observed_loan_count=len(observed), overdue_count=sum(observed),
                            overdue_rate=sum(observed) / len(observed) if observed else None))
    return 200, dict(week_start=week_start, time_zone=ZONE.key, as_of=cutoff, underwriters=metrics)
