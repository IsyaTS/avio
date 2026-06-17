 Цель goal:
  Сделать ответы Avio Bot заметно лучше на реальных и eval-сценариях, сохранив текущую архитектуру проекта и не ломая production flow.

  Рабочая директория:
   /opt/avio-dev

  Контекст:
  Проект Avio — SaaS автоответчик для Авито. Текущий стабильный pipeline ответа уже существует. Его не надо переписывать с нуля, не надо добавлять новый AgentRuntimeV2, новый seller brain или отдельную LLM-архитектуру.
  Нужно улучшить качество ответов внутри текущих точек управления: response_pipeline, sales_core, retrieval/context, fallback, guards, learning examples, evals.

  Главная задача:
  Бот должен отвечать коротко, по делу, как нормальный менеджер в Авито-чате:
  - не задавать вопрос, если клиент уже дал ответ;
  - не повторять одно и то же;
  - не давать неподходящую категорию/цену;
  - не уходить в общий шаблон на конкретный вопрос;
  - не гнать клиента в мессенджер слишком рано;
  - не выдумывать цену, наличие, адрес, доставку;
  - учитывать историю диалога;
  - учитывать размер, город, контакт, отказ, каталог, цену, наличие;
  - если данных не хватает — задавать один самый важный уточняющий вопрос.

  Ограничения:
  1. Не переписывать весь pipeline.
  2. Не добавлять новую крупную архитектуру.
  3. Не хардкодить двери в runtime logic.
  4. Нишевые примеры можно держать только в eval cases/tests.
  5. Не ломать публичные API и webhook flow.
  6. Не менять отправку сообщений в Авито без необходимости.
  7. Все изменения должны быть покрыты тестами/evals.
  8. Prod не трогать, работа dev-only в /opt/avio-dev.

  Что нужно сделать по шагам:

  1. Сначала зафиксировать baseline:
     - pwd
     - hostname -I
     - git status --short
     - определить dirty files
     - не откатывать чужие изменения

  2. Найти текущий runtime flow ответов:
     - где входящее Avito-сообщение попадает в worker/API
     - где вызывается run_response_pipeline
     - где формируется fallback
     - где берётся catalog/context/history/examples
     - где принимается решение smart reply/static reply/catalog/price reply
     - где применяются guards/sanitize

  3. Запустить текущий offline eval:
     python -m tools.run_reply_evals --tenant-id 101 --cases evals/reply_cases.jsonl --out evals/reports/baseline.json

  4. Составить список худших проблем по report:
     - top violations
     - 10 худших кейсов
     - для каждого: user_text, reply_text, source, fallback_used, violations

  5. Исправлять только минимальными точечными изменениями:
     - context/history interpretation
     - guard против повторного вопроса о размере/городе/контакте
     - fallback selection
     - prompt instructions внутри существующего pipeline, если это уже текущая точка настройки
     - quality guard, который не даёт плохой ответ уйти без fallback/handoff
     - catalog/category mismatch guard
     - close/contact/catalog intent handling

  6. Обязательные продуктовые правила:
     - если клиент указал размер, бот не спрашивает размер снова;
     - если клиент указал город, бот не спрашивает город снова;
     - если клиент оставил телефон, бот реагирует на контакт, а не спрашивает “что подбираете”;
     - если клиент отказался/уже решил, бот закрывает диалог коротко;
     - если клиент спрашивает цену, бот не подставляет цену другой категории;
     - если цены нет, бот не выдумывает цену;
     - если наличия нет в подтверждённом контексте, бот не пишет “есть в наличии”;
     - если клиент просит каталог здесь, бот не требует WhatsApp/Telegram/MAX;
     - если клиент задал конкретный вопрос, бот отвечает на него, а не шлёт общий шаблон.

  7. После каждого набора изменений прогонять:
     - pytest по изменённым тестам
     - pytest tests/test_response_pipeline_contextual_cases.py -q
     - pytest tests/test_worker_smart_reply_runtime.py -q
     - python -m tools.run_reply_evals --tenant-id 101 --cases evals/reply_cases.jsonl --out evals/reports/latest.json

  8. Критерий успеха:
     - pass rate eval вырос относительно baseline;
     - violations ignored_size, ignored_city, repeated_question, wrong_category_price, generic_fallback, catalog_push_without_answer стали меньше;
     - на 10 худших кейсах ответы стали человеческими и по делу;
     - production behavior не сломан;
     - нет door-specific hardcode в runtime;
     - все niche-specific ожидания остались в eval cases/tests.

  9. В финале показать:
     - baseline pass rate и latest pass rate;
     - top violations before/after;
     - 10 примеров: user_text -> old_reply -> new_reply -> source -> violations;
     - список изменённых файлов;
     - команды проверок и результат;
     - что не проверялось;
     - prod не проверялся, если prod не трогали.

  Важно:
  Не ограничивайся “улучшить prompt”. Сначала найди реальные причины плохих ответов по trace/eval, потом исправляй самую дешёвую и точную точку: slot detection, history use, fallback guard, catalog mismatch, repeated
  question guard, или prompt только если это действительно причина.

  Работай до состояния, где можно показать пользователю большую выборку ответов и понятно доказать, что бот стал отвечать лучше.