# niche-brain-v2 manual check

Runtime was not changed by this document. Use it after enabling the code in a dev or test tenant.

## Enable for a test tenant

Add this tenant config section for the test tenant only:

```json
{
  "behavior": {
    "niche_brain_v2": {
      "enabled": true,
      "apply_mode": true,
      "tenant_allowlist": [101],
      "allowed_channels": ["avito"]
    }
  }
}
```

Disable by setting `enabled` or `apply_mode` to `false`, removing the tenant from
`tenant_allowlist`, removing `avito` from `allowed_channels`, or setting
`NICHE_BRAIN_V2_DISABLED=1`.

## Avito message types to replay

1. `каталог есть?`
2. `скиньте каталог`
3. `фото есть?`
4. `сколько стоит?`
5. `цена какая`
6. `есть в наличии?`
7. `можно сегодня забрать?`
8. `где посмотреть`
9. `адрес магазина`
10. `доставка есть?`
11. `с установкой сколько?`
12. `замер делаете?`
13. `размер 2050 на 900`
14. `нужна дверь в квартиру`
15. `в дом нужна`
16. `дорого`
17. `есть дешевле?`
18. `а чем отличаются`
19. `это жестянка?`
20. `почему адрес нужен?`

## Compare old vs v2

Run the same cases with v2 disabled and enabled. Prefer `tools/run_reply_evals.py`
if the current environment has the needed tenant config and model access. Otherwise,
use the same real-dialog replay path manually and save the reply text, source,
fallback flag, latency, and quality violations.

Check old and v2 replies against the same criteria:

- answers the current client meaning before asking;
- asks no more than one follow-up question;
- does not ask for facts already present in the message or recent history;
- does not push PDF, phone, Telegram, or manager handoff too early;
- uses only grounded catalog/price/address facts;
- sounds like a concise Avito seller, not support automation.

## Bad reply signs

- starts with empty acknowledgement: `Понял`, `Принято`, `Уточните`;
- asks `Чем могу помочь?` after the client already stated the need;
- asks several qualifiers at once;
- ignores size, city, object type, or model already provided;
- answers with generic fallback when the user asked a concrete price/catalog/location question;
- invents price, availability, address, discount, or model details.

## Good reply signs

- short and concrete;
- first sentence handles the current question or makes a useful grounded guess;
- one next question at most;
- keeps the conversation inside Avito unless the client asks otherwise;
- uses seller language: practical, direct, and specific to doors;
- moves to the next sales step without sounding scripted.
