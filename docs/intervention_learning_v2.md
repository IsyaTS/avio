# Intervention Learning V2

## Что это

`learning v2` — это per-tenant самообучение бота на вмешательствах менеджера.
Система учится не на `question -> answer`, а на `state -> preferred action`.

Основные сущности:
- `dialogue_state_snapshots`
- `intervention_episodes`
- `episode_labels`
- `policy_candidates`
- `policy_candidate_evidence`
- `tenant_policy_rules`
- `policy_decisions`
- `policy_outcomes`

## Как работает

1. Когда менеджер вмешивается в диалог, worker сохраняет `intervention episode`.
2. Для эпизода сохраняются два snapshot'а: до bot-path и до manager-path.
3. После следующего пользовательского хода или по достижении horizon система считает outcome.
4. Повторяющиеся positive episodes поднимаются в `policy_candidates`.
5. После достаточного evidence policy попадает в `tenant_policy_rules`.
6. Runtime читает active rules только как soft hint.
7. По умолчанию runtime работает в `shadow mode`: решения логируются, но в prompt не подмешиваются.

## Guardrails

Система не должна:
- обучаться на `Q/A` парах
- копировать raw manager reply как ответ бота
- ломать answer-user-first
- forcing qualification loop, если вопрос уже answerable
- override confirmed facts, catalog truth, price truth
- смешивать policy между tenant

## Конфигурация tenant

В `tenant.json` или tenant config:

```json
{
  "learning": {
    "enabled": true,
    "intervention_policy": {
      "enabled": true,
      "capture_enabled": true,
      "runtime_enabled": true,
      "shadow_mode": true,
      "apply_mode": false,
      "kill_switch": false,
      "min_similarity": 0.64,
      "min_confidence": 0.72,
      "min_evidence": 3,
      "min_distinct_leads": 2,
      "min_reward_delta": 0.15,
      "max_negative_evidence": 2
    }
  }
}
```

По умолчанию `intervention_policy.enabled=false`, поэтому новая логика не влияет на старых tenant.

## Shadow mode

Когда `shadow_mode=true` и `apply_mode=false`:
- runtime строит snapshot
- ищет подходящее rule
- пишет `policy_decision`
- логирует `learning_v2_policy_decision`
- НЕ добавляет policy hint в system prompt

## Apply mode

Когда `apply_mode=true`:
- runtime делает всё то же самое
- при прохождении gating добавляет advisory policy hint в system prompt
- LLM всё равно генерирует ответ заново
- hint не содержит raw manager reply

## Откат

Самый быстрый rollback:
- поставить `learning.intervention_policy.kill_switch=true`
- либо `enabled=false`

Это отключает capture/runtime use без отката миграций.

## Что смотреть в БД

- active rules: `tenant_policy_rules`
- сырые episodes: `intervention_episodes`
- shadow/apply decisions: `policy_decisions`
- outcome / agreement: `policy_outcomes`

## Остаточные ограничения

- taxonomy пока rule-based, без LLM labeler
- outcome model использует lightweight heuristics
- runtime apply-mode intentionally conservative
- UI для ручного управления policy пока не добавлен
