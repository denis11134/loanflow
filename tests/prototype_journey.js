() => {
  const query = selector => document.querySelector(selector);
  const text = selector => query(selector).innerText;
  const current = () => text("#screen");
  const sandbox = () => text("#lab-state");
  const action = name => query('#role-content [data-action="' + name + '"]');
  const has = phrase => current().includes(phrase);
  let passed = 0;
  function assert(value, label) {
    if (!value) throw Error(label + "\n" + current());
    passed += 1;
  }
  const appStatus = code => assert(has(code), code);
  const reserve = value => assert(sandbox().includes(value), "Резерв: " + value);
  function click(element, label) {
    if (!element || element.disabled) throw Error("Нет доступного действия: " + label);
    element.click();
  }
  const act = name => click(action(name), name);
  const open = name => click(query('#roles [data-role="' + name + '"]'), name);
  const event = prefix => click(Array.from(document.querySelectorAll("#lab-events button"))
    .find(button => button.textContent.startsWith(prefix)), prefix);
  function input(id, value) {
    const field = query(id);
    field.value = value;
    field.dispatchEvent(new Event("input", { bubbles: true }));
  }
  function load(key, role = "mobile") {
    open(role);
    query("#scenario").value = key;
    click(query("#scenario-form button"), "Загрузить сценарий");
  }
  function sms(code = "123456") {
    input("#sms", code);
    click(query('#sms-form button[type="submit"]'), "Подтвердить подписание");
  }
  const sign = () => { act("startSigning"); sms(); };
  const submit = () => click(query('#draft-form button[value="submit"]'), "Подать заявку");
  const checkWidth = () => assert(document.documentElement.scrollWidth <= innerWidth, "Горизонтальный скролл");
  try {
    assert(innerWidth === Number(location.hash.slice(1)), "Фактическая ширина " + innerWidth);
    checkWidth();
    input("#amount", "125000");
    click(query('#draft-form button[value="save"]'), "Сохранить черновик");
    assert(has("Черновик сохранён"), "Сохранение");
    open("branch");
    assert(has("Активная заявка\nНет") && has("Действующий договор\nНет"), "Черновик не активен для отделения");
    assert(!query("#role-content button"), "Отделение только читает");
    open("mobile");
    assert(query("#amount").value === "125000", "Сумма в сохранённом черновике");
    submit();
    appStatus("SCORING");
    open("branch");
    assert(has("Активная заявка\nЕсть"), "Заявка активна после подачи");
    open("mobile");
    event("Завершить оценку");
    appStatus("APPROVED");
    const deadline = query("#role-content strong").textContent;
    act("showTerms");
    assert(has("ПСК") && has("20% годовых") && has("125 000 ₽") && has("охлаждение 4 ч"), "ИУ");
    checkWidth();
    act("hideTerms");
    act("showTerms");
    assert(query("#role-content strong").textContent === deadline, "Повторный просмотр ИУ");
    sign();
    appStatus("COOLING_OFF");
    assert(has("Запуск выдачи возможен с") && !has("Время зачисления"), "Окончание охлаждения");
    reserve("Удерживается");
    event("К окончанию охлаждения");
    appStatus("DISBURSEMENT_PROCESSING");
    assert(!action("cancelCooling"), "Отказ после запуска в МП недоступен");
    open("contact");
    assert(!action("cancelCooling"), "Отказ после запуска в КЦ недоступен");
    event("АБС подтвердила зачисление");
    appStatus("DISBURSED");
    assert(has("Время зачисления") && !has("125 000"), "КЦ показывает время без суммы");
    open("mobile");
    assert(has("125 000 ₽") && has("Время зачисления"), "Результат выдачи в МП");
    reserve("Списан");
    open("branch");
    assert(has("Активная заявка\nНет") && has("Действующий договор\nЕсть"), "Выданный договор в отделении");

    for (const [key, message] of [
      ["incomplete", "Укажите сумму"], ["duplicate", "уже есть активная заявка"],
      ["existing", "уже есть действующий договор"], ["nonPayroll", "зарплатным клиентам"],
    ]) {
      load(key);
      submit();
      appStatus("DRAFT");
      assert(has(message), message);
    }
    for (const [key, stage, message] of [
      ["rejected", "BANK_REJECTED", "кредит не одобрен"], ["selfBan", "BANK_REJECTED", "самозапрета"],
      ["noConsent", "BANK_REJECTED", "действующее согласие"],
      ["systemError", "SYSTEM_ERROR", "технического сбоя"], ["costLimit", "BANK_REJECTED", "кредит не одобрен"],
    ]) {
      load(key);
      event("Завершить оценку");
      appStatus(stage);
      assert(has(message) && !action("startSigning"), message);
      reserve("Отсутствует");
      open("contact");
      appStatus(stage);
      assert(!has(message) && !has("ПСК") && !action("cancelCooling"), "Ограничения КЦ");
    }
    load("manual", "underwriter");
    assert(!has("SLA нарушен"), "До дедлайна нет эскалации");
    event("Перейти за дедлайн SLA");
    appStatus("UNDERWRITING");
    assert(has("Заявка эскалирована руководителю"), "Эскалация");
    checkWidth();
    open("mobile");
    assert(!has("SLA"), "МП не показывает SLA");
    open("underwriter");
    act("approve");
    assert(has("Одобрение зафиксировано") && !action("reject"), "Ручное одобрение");
    open("mobile");
    appStatus("APPROVED");
    load("manual", "underwriter");
    act("reject");
    open("mobile");
    appStatus("BANK_REJECTED");
    load("quota", "underwriter");
    act("approve");
    open("mobile");
    appStatus("BANK_REJECTED");
    reserve("Отсутствует");
    load("manual");
    act("cancelApplication");
    open("underwriter");
    assert(!action("approve") && !action("reject"), "Отменённой заявки нет в очереди");

    load("beforeTerms");
    assert(!action("startSigning"), "Подписание до предоставления ИУ");
    event("Завершить предоставление ИУ");
    assert(action("startSigning"), "Подписание после предоставления ИУ");
    load("termsError");
    event("Завершить предоставление ИУ");
    appStatus("SYSTEM_ERROR");
    reserve("Освобождён");
    load("lowerLimit");
    assert(has("выше 50%") && has("охлаждение 4 ч"), "Предупреждение о ПДН до подписания");
    act("startSigning");
    const smsDeadline = query("#role-content strong").textContent;
    act("resendSms");
    act("resendSms");
    assert(action("resendSms").disabled, "Третья повторная отправка");
    assert(query("#role-content strong").textContent === smsDeadline, "Дедлайн SMS сохраняется");
    sms("111111");
    appStatus("APPROVED");
    assert(has("Код неверный"), "Неверный код");
    act("startSigning");
    event("Имитировать сбой доставки");
    appStatus("APPROVED");
    assert(has("Не удалось доставить SMS"), "Сбой доставки");
    act("startSigning");
    event("К дедлайну попытки");
    appStatus("APPROVED");
    assert(has("Время ввода кода истекло"), "Истечение попытки");
    reserve("Удерживается");
    act("cancelApplication");
    appStatus("CLIENT_CANCELED");
    reserve("Освобождён");
    load("smsTimeout");
    sms();
    assert(has("Проверяем код") && !query("#sms") && !action("resendSms") && !action("startSigning"), "Неизвестный исход ПЭП");
    act("toggleVerification");
    assert(has("Результат появится в статусе заявки") && !query("#sms"), "Сворачивание проверки");
    open("branch");
    open("mobile");
    assert(has("Результат появится в статусе заявки"), "Смена роли сохраняет экран");
    act("toggleVerification");
    assert(has("Проверяем код") && !query("#sms"), "Возврат на экран");
    event("Вернуть успешный результат");
    appStatus("COOLING_OFF");
    load("smsTimeout");
    sms();
    event("К дедлайну попытки");
    appStatus("APPROVED");
    load("offerDeadline");
    event("К дедлайну предложения");
    appStatus("OFFER_EXPIRED");
    assert(!action("startSigning") && !query("#sms"), "Истёкшее предложение не подписывается");
    reserve("Освобождён");

    for (const [key, outcome, duration] of [
      ["noCooling", "DISBURSEMENT_PROCESSING", null], ["lowerLimit", "COOLING_OFF", "4 ч"],
      ["upperLimit", "COOLING_OFF", "4 ч"], ["longCooling", "COOLING_OFF", "48 ч"],
    ]) {
      load(key);
      if (duration) assert(has("охлаждение " + duration), "Порог охлаждения " + key);
      sign();
      appStatus(outcome);
    }
    load("cooling");
    act("cancelCooling");
    appStatus("CANCELED_BY_CLIENT");
    reserve("Освобождён");
    load("cooling", "contact");
    assert(!action("cancelCooling") && has("идентифицируйте клиента"), "КЦ без идентификации");
    assert(!has("ПСК") && !has("ПДН") && !has("Код учебной SMS"), "Минимум данных КЦ");
    event("Имитировать успешную идентификацию");
    act("cancelCooling");
    appStatus("CANCELED_BY_CLIENT");
    open("mobile");
    appStatus("CANCELED_BY_CLIENT");
    reserve("Освобождён");
    load("cooling", "contact");
    event("Имитировать успешную идентификацию");
    event("К окончанию охлаждения");
    assert(!action("cancelCooling"), "Идентификация не сохраняет право отказа после запуска");
    event("АБС подтвердила окончательное закрытие");
    open("mobile");
    appStatus("DISBURSEMENT_FAILED");
    reserve("Освобождён");

    for (const [key, result] of [["missing", "NOT_FOUND"], ["pending", "PENDING"]]) {
      load(key, "reconciliation");
      checkWidth();
      assert(has("INST-DEMO-001") && !action("closeInstruction"), "Закрытие только после чтения");
      act("readInstruction");
      assert(has(result) && action("closeInstruction"), "Ручное закрытие после " + result);
      reserve("Удерживается");
      open("mobile");
      appStatus("DISBURSEMENT_PROCESSING");
      open("reconciliation");
      for (const [prefix, code] of [
        ["Следующее чтение АБС завершится таймаутом", "TIMEOUT"], ["Следующее чтение АБС вернёт конфликт", "CONFLICT"],
      ]) {
        event(prefix);
        act("readInstruction");
        assert(has(code) && !action("closeInstruction"), "После " + code + " нужно новое чтение");
        act("readInstruction");
      }
      act("closeInstruction");
      assert(has("CLOSED") && !action("closeInstruction"), "Окончательное закрытие");
      open("mobile");
      appStatus("DISBURSEMENT_FAILED");
      reserve("Освобождён");
    }
    for (const [key, outcome, contract] of [
      ["posted", "POSTED", "DISBURSED"], ["closed", "CLOSED", "DISBURSEMENT_FAILED"],
    ]) {
      load(key, "reconciliation");
      act("readInstruction");
      assert(has(outcome) && !action("closeInstruction"), "Окончательный ответ " + outcome);
      open("mobile");
      appStatus(contract);
    }
    for (const followUp of ["closeInstruction", "readInstruction"]) {
      load("lostClose", "reconciliation");
      act("readInstruction");
      act("closeInstruction");
      assert(has("CLOSE_TIMEOUT") && has("INST-DEMO-001"), "Потеря ответа на закрытие");
      reserve("Удерживается");
      act(followUp);
      assert(has("CLOSED") && has("INST-DEMO-001"), "Повтор по исходному ID");
    }
    load("pending", "reconciliation");
    act("readInstruction");
    event("АБС провела зачисление");
    act("closeInstruction");
    assert(has("POSTED"), "Проводка, зафиксированная первой, побеждает закрытие");
    open("mobile");
    appStatus("DISBURSED");

    load("unavailable", "contact");
    assert(has("получить статус невозможно") && !has("APPROVED"), "КЦ при недоступности данных");
    open("branch");
    assert(has("выполнить проверку невозможно") && !has("Действующий договор"), "Отделение при недоступности данных");
    load("journey");
    checkWidth();
    return { passed, width: innerWidth };
  } catch (error) {
    throw new Error("LOANFLOW QA FAIL: " + error.message);
  }
}
