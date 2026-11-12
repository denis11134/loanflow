from decimal import Decimal
from fractions import Fraction

import pytest

from services.loanflow import assessment, calculations


@pytest.mark.parametrize(("amount", "term", "rate", "regular", "payments", "interest", "psk"), [
    (100_000, 3, Fraction(0), 33_333, [33_333, 33_333, 33_334], 0, "0.000000000000"),
    (100_050, 1, calculations.MONTHLY_RATE, 101_718, [101_718], 1_668, "20.005997001499"),
    (5_000_000, 12, calculations.MONTHLY_RATE, 463_173, [463_173] * 11 + [463_167],
     558_070, "20.000006146107"),
])
def test_schedule_and_psk(amount, term, rate, regular, payments, interest, psk):
    assert calculations.annuity_schedule(amount, term, rate) == (regular, payments, interest)
    assert sum(payments) - interest == amount
    assert format(calculations.annual_psk_percent(amount, payments), "f") == psk


@pytest.mark.parametrize(("pdn", "term", "score", "result"), [
    ("35", 12, 80, "APPROVED"), ("35.01", 12, 60, "APPROVED"),
    ("50", 12, 60, "APPROVED"), ("50", 13, 50, "APPROVED"),
    ("50", 36, 50, "APPROVED"), ("50", 37, 40, "MANUAL_REVIEW"),
    ("50.01", 12, 50, "APPROVED"), ("50.01", 37, 30, "MANUAL_REVIEW"),
    ("65", 37, 30, "MANUAL_REVIEW"), ("65.01", 12, 20, "REJECTED"),
])
def test_scoring_boundaries(pdn, term, score, result):
    assert calculations.scoring_score(Fraction(pdn), term) == score
    assert calculations.scoring_result(score) == result


def test_pdf_rounds_stored_psk_half_up(monkeypatch):
    lines = []
    draw_string = assessment.canvas.Canvas.drawString

    def capture(doc, x, y, line):
        lines.append(line)
        return draw_string(doc, x, y, line)

    monkeypatch.setattr(assessment.canvas.Canvas, "drawString", capture)
    assert assessment.render_terms({
        "offer_id": "test", "amount_rub": Decimal("1600.00"), "term_months": 1,
        "annual_rate_percent": Decimal("20"), "monthly_payment_rub": Decimal("1626.67"),
        "psk_annual_percent": Decimal("20.002500000000"), "psk_rub": Decimal("26.67"),
    }).startswith(b"%PDF-")
    assert "ПСК годовых: 20.003%" in lines
