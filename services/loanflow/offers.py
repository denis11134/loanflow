from .applications import NOTICE_VERSION, minor, notice_required, offer_row, own_application
from .store import ApiError, one, require

NOTICE_TEXT = "Долговая нагрузка превышает 50%. Оцените риск просрочки и штрафов по кредиту."


def cooling_hours(amount_rub):
    if amount_rub < 50_000:
        return 0
    return 4 if amount_rub <= 200_000 else 48


def get_offer(ctx, application_id):
    app = own_application(ctx, application_id)
    offer = offer_row(ctx.db, application_id)
    if not offer or (not offer["terms_id"] and app["status"] != "APPROVED"):
        raise ApiError(409, "OFFER_NOT_AVAILABLE")
    if not offer["terms_id"]:
        return 200, {"state": "PREPARING", "offer_id": offer["offer_id"]}
    hours = cooling_hours(offer["amount_rub"])
    result = {
        "state": "EXPIRED" if ctx.clock() >= offer["offer_expires_at"] else "AVAILABLE",
        "offer_id": offer["offer_id"], "terms_id": offer["terms_id"],
        "amount_minor": minor(offer["amount_rub"]), "term_months": offer["term_months"],
        "annual_rate_percent": format(offer["annual_rate_percent"], "f"),
        "monthly_payment_minor": minor(offer["monthly_payment_rub"]),
        "psk_annual_percent": format(offer["psk_annual_percent"], "f"), "psk_rub": format(offer["psk_rub"], ".2f"),
        "first_provided_at": offer["first_provided_at"], "offer_expires_at": offer["offer_expires_at"],
        "document_sha256": bytes(offer["document_sha256"]).hex(), "cooling_hours": hours,
        "withdrawal_available_during_cooling": hours > 0, "pdn_notice_required": notice_required(offer),
    }
    if result["pdn_notice_required"]:
        shown = offer["warning_first_shown_at"] if offer["warning_text_version"] == NOTICE_VERSION else None
        result["pdn_notice"] = {"version": NOTICE_VERSION, "text": NOTICE_TEXT, "shown_at": shown}
    return 200, result


def get_individual_terms_document(ctx, application_id, terms_id):
    own_application(ctx, application_id)
    row = require(one(ctx.db, """SELECT document_bytes FROM individual_terms WHERE application_id=%s AND terms_id=%s""",
                      (application_id, terms_id)))
    return 200, bytes(row["document_bytes"])


def acknowledge_pdn_notice(ctx, application_id, terms_id):
    own_application(ctx, application_id, lock=True)
    offer = offer_row(ctx.db, application_id)
    if not offer or offer["terms_id"] != terms_id:
        raise ApiError(404, "NOT_FOUND")
    if not notice_required(offer):
        raise ApiError(409, "PDN_NOTICE_NOT_REQUIRED")
    if ctx.body["notice_version"] != NOTICE_VERSION:
        raise ApiError(409, "NOTICE_VERSION_MISMATCH")
    at = ctx.clock()
    if offer["warning_text_version"] == NOTICE_VERSION:
        at = offer["warning_first_shown_at"]
    elif at >= offer["offer_expires_at"]:
        raise ApiError(409, "OFFER_EXPIRED")
    else:
        ctx.db.execute("UPDATE individual_terms SET warning_first_shown_at=%s,warning_text_version=%s WHERE terms_id=%s",
                       (at, NOTICE_VERSION, terms_id))
    return 200, {"notice_version": NOTICE_VERSION, "shown_at": at}
