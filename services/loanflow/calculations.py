from decimal import Decimal
from fractions import Fraction


RULES_VERSION = "3.3"
MIN_AMOUNT_RUB = 1_000
MAX_AMOUNT_RUB = 1_000_000
MIN_TERM_MONTHS = 1
MAX_TERM_MONTHS = 60
ANNUAL_RATE_PERCENT = 20
PSK_LIMIT_PERCENT = 25
PSK_DECIMAL_PLACES = 12
MONTHLY_RATE = Fraction(ANNUAL_RATE_PERCENT, 12 * 100)


def round_half_up(value):
    # Денежные значения, которые здесь округляются, неотрицательны.
    shifted = value + Fraction(1, 2)
    return shifted.numerator // shifted.denominator


def rubles_from_cents(cents):
    return Decimal(cents).scaleb(-2)


def annuity_schedule(principal, term, monthly_rate):
    if monthly_rate:
        regular = principal * monthly_rate / (1 - (1 + monthly_rate) ** -term)
    else:
        regular = Fraction(principal, term)
    regular_payment = round_half_up(regular)
    remaining = principal
    payments = []
    total_interest = 0
    for month in range(1, term + 1):
        interest = round_half_up(remaining * monthly_rate)
        payment = regular_payment if month < term else remaining + interest
        remaining -= payment - interest
        payments.append(payment)
        total_interest += interest
    return regular_payment, payments, total_interest


def pdn_percent(income, existing, regular_payment):
    return (Fraction(existing) + Fraction(regular_payment)) * 100 / Fraction(income)


def scoring_score(pdn, term):
    if pdn <= 35:
        score = 60
    elif pdn <= 50:
        score = 40
    elif pdn <= 65:
        score = 30
    else:
        score = 0
    if term <= 12:
        score += 20
    elif term <= 36:
        score += 10
    return score


def scoring_result(score):
    if score >= 50:
        return "APPROVED"
    if score >= 30:
        return "MANUAL_REVIEW"
    return "REJECTED"


def present_value_difference(principal, payments, annual_percent):
    discount = 1 / (1 + Fraction(annual_percent) / (12 * 100))
    value = Fraction(0)
    for payment in reversed(payments):
        value = (value + payment) * discount
    return value - principal


def annual_psk_percent(principal, payments):
    scale = 10 ** PSK_DECIMAL_PLACES
    # Все платежи отстоят хотя бы на один период. Если дисконтировать их
    # как один платёж в первом месяце, получим верхнюю границу ставки.
    upper = Fraction(sum(payments) - principal, principal) * (12 * 100) * scale
    low, high = 0, round_half_up(upper)
    # Ищем первый результат, чья верхняя граница округления строго выше корня.
    # При точном равенстве выбирается следующий результат (ROUND_HALF_UP).
    while low < high:
        middle = (low + high) // 2
        upper_midpoint = Fraction(2 * middle + 1, 2 * scale)
        if present_value_difference(principal, payments, upper_midpoint) < 0:
            high = middle
        else:
            low = middle + 1
    return Decimal(low).scaleb(-PSK_DECIMAL_PLACES)
