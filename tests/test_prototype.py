"""Настоящий браузер, фиксированные часы демо, без ожиданий наугад."""
import os
import pytest
from playwright.sync_api import sync_playwright
from conftest import ROOT

pytestmark = pytest.mark.browser


@pytest.fixture(params=[320, 375], ids=['320px', '375px'])
def page(request):
    os.environ.setdefault('PLAYWRIGHT_BROWSERS_PATH', str(ROOT / '.cache/ms-playwright'))
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        context = browser.new_context(viewport={'width': request.param, 'height': 900},
                                      locale='ru-RU', timezone_id='Europe/Samara')
        page = context.new_page()
        errors = []
        page.on('pageerror', lambda error: errors.append(str(error)))
        page.goto((ROOT / 'docs/08-prototypes/loanflow-demo.html').as_uri() + f'#{request.param}')
        page.locator('#amount').wait_for()
        yield page
        assert not errors, errors
        context.close()
        browser.close()


def test_existing_journey(page):
    result = page.evaluate((ROOT / 'tests/prototype_journey.js').read_text())
    assert result['passed'] > 0
    assert result['width'] == page.viewport_size['width']
    print(f"\nМаршрут демо: {result['passed']} проверок, {result['width']} px")


def load(page, key, role='mobile'):
    page.locator(f'#roles [data-role="{role}"]').click()
    page.locator('#scenario').select_option(key)
    page.locator('#scenario-form button').click()


def event(page, prefix):
    page.locator('#lab-events button').filter(has_text=prefix).click()


def action(page, name):
    return page.locator(f'#role-content [data-action="{name}"]')


def test_contact_identification_deadline(page):
    load(page, 'cooling', 'contact')
    event(page, 'Имитировать успешную идентификацию')
    assert action(page, 'cancelCooling').count() == 1
    page.evaluate('advanceTo(startTime + 5 * minute - 1); render()')
    assert action(page, 'cancelCooling').count() == 1
    page.evaluate('advanceTo(startTime + 5 * minute); render()')
    assert action(page, 'cancelCooling').count() == 0
    page.evaluate('roleActions.contact.cancelCooling(); render()')
    assert page.evaluate('state.contract') == 'COOLING_OFF'
    event(page, 'Имитировать успешную идентификацию')
    action(page, 'cancelCooling').click()
    assert page.evaluate('state.contract') == 'CANCELED_BY_CLIENT'


def test_pdn_and_attempt_actions(page):
    load(page, 'lowerLimit', 'branch')
    assert page.evaluate('state.pdnWarning ?? null') is None
    page.evaluate('startSigning(); render()')
    assert page.evaluate('state.application') == 'APPROVED'
    page.locator('#roles [data-role="mobile"]').click()
    assert page.locator('#pdn-notice').is_visible()
    shown = page.evaluate('state.pdnWarning')
    action(page, 'showTerms').click()
    assert page.evaluate('state.pdnWarning') == shown
    action(page, 'startSigning').click()
    assert page.locator('#sms').is_visible()
    old_id = page.evaluate('state.attempt.id')
    event(page, 'К дедлайну попытки')
    assert page.locator('#sms').count() == action(page, 'resendSms').count() == 0
    action(page, 'startSigning').click()
    page.evaluate('(id) => { sign(id); render(); }', old_id)
    assert page.evaluate('state.application') == 'SIGNING_IN_PROGRESS'
    load(page, 'smsTimeout')
    page.locator('#sms').fill('123456')
    page.locator('#sms-form button[type="submit"]').click()
    assert page.locator('#sms').count() == action(page, 'resendSms').count() == action(page, 'startSigning').count() == 0
    event(page, 'Вернуть успешный результат')
    assert page.locator('#sms').count() == action(page, 'resendSms').count() == action(page, 'startSigning').count() == 0


def test_latest_registered_read_and_close_retry(page):
    load(page, 'pending', 'reconciliation')
    action(page, 'readInstruction').click()
    assert action(page, 'closeInstruction').count() == 1
    event(page, 'Задержать ответ следующего чтения')
    action(page, 'readInstruction').click()
    assert 'WAITING' in page.locator('#role-content').inner_text()
    assert action(page, 'closeInstruction').count() == 0
    event(page, 'Следующее чтение АБС завершится таймаутом')
    action(page, 'readInstruction').click()
    assert 'TIMEOUT' in page.locator('#role-content').inner_text()
    event(page, 'Получить ответ чтения № 2')
    assert page.evaluate('state.reads.at(-1).result') == 'TIMEOUT'
    assert action(page, 'closeInstruction').count() == 0
    load(page, 'lostClose', 'reconciliation')
    action(page, 'readInstruction').click()
    action(page, 'closeInstruction').click()
    event(page, 'Следующее чтение АБС завершится таймаутом')
    action(page, 'readInstruction').click()
    assert action(page, 'closeInstruction').inner_text() == 'Повторить закрытие'
    action(page, 'closeInstruction').click()
    assert page.evaluate('state.contract') == 'DISBURSEMENT_FAILED'
    assert page.evaluate('state.closeCommand.authorizedReadSeq') == 1


def test_roles_and_width(page):
    for role in ['mobile', 'contact', 'branch', 'underwriter', 'reconciliation']:
        for key in ['manual', 'lowerLimit', 'smsTimeout', 'cooling', 'pending']:
            load(page, key, role)
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
            if role in ['contact', 'branch']:
                assert not any(text in page.locator('#role-content').inner_text() for text in ['ПСК', 'ПДН', 'Сумма', 'SMS'])
            if role == 'branch':
                assert page.locator('#role-content button').count() == 0
    load(page, 'manual', 'underwriter')
    assert '120 000 ₽' in page.locator('#role-content').inner_text()
    assert 'Момент подачи' in page.locator('#role-content').inner_text()
    # Инъекция чужой команды в DOM проверяет существующий диспетчер роли, не авторизацию сервера.
    load(page, 'cooling', 'branch')
    page.evaluate('''() => { const b = document.createElement('button'); b.dataset.action = 'cancelCooling';
        document.querySelector('#role-content').append(b); b.click(); }''')
    assert page.evaluate('state.contract') == 'COOLING_OFF'


def test_late_terminal_read_and_later_nonterminal(page):
    load(page, 'posted', 'reconciliation')
    event(page, 'Задержать ответ следующего чтения')
    action(page, 'readInstruction').click()
    event(page, 'Следующее чтение АБС завершится таймаутом')
    action(page, 'readInstruction').click()
    assert action(page, 'closeInstruction').count() == 0
    event(page, 'Получить ответ чтения № 1')
    assert page.evaluate('state.contract') == 'DISBURSED'
    assert page.evaluate('state.reads.at(-1).result') == 'TIMEOUT'
    assert 'Зачисление подтверждено' in page.locator('#role-content').inner_text()
    # Оба чтения зарегистрированы до закрытия; поздний PENDING не скрывает CLOSED.
    load(page, 'pending', 'reconciliation')
    event(page, 'Задержать ответ следующего чтения')
    action(page, 'readInstruction').click()
    action(page, 'readInstruction').click()
    action(page, 'closeInstruction').click()
    event(page, 'Получить ответ чтения № 1')
    assert page.evaluate('state.contract') == 'DISBURSEMENT_FAILED'
    assert 'Распоряжение окончательно закрыто' in page.locator('#role-content').inner_text()
