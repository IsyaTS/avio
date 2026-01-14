import statistics

from libs.core.services import avito_analytics_report as report


def test_calc_sla_unanswered_and_breach():
    # two chats: one unanswered, one slow response > SLA
    chats = [
        {
            "_messages": [
                {"ts": "2024-01-01T10:00:00+00:00", "direction": "incoming"},
            ]
        },
        {
            "_messages": [
                {"ts": "2024-01-01T10:00:00+00:00", "direction": "incoming"},
                {"ts": "2024-01-01T10:30:00+00:00", "direction": "outgoing"},
            ]
        },
    ]
    sla = report._calc_sla(chats, None, 15)
    assert sla.unanswered == 1
    assert sla.slow_buckets.get("breach") == 1
    assert statistics.median(sla.first_response_seconds) == 1800


def test_calc_losses_with_params():
    sla_stats = report.SLAStats(first_response_seconds=[100], unanswered=2, chats_total=2, slow_buckets={"breach": 1})
    losses = report._calc_losses(sla_stats, {"avg_check": 1000, "close_rate_chat": 0.2, "loss_factor_slow_response": 0.5, "gross_margin": 50})
    # revenue_at_risk_unanswered = 2 * 1000 * 0.2 = 400
    assert losses["revenue_at_risk_unanswered"] == 400
    # revenue_at_risk_slow = 1 * 1000 * 0.2 * 0.5 = 100
    assert losses["revenue_at_risk_slow"] == 100
    # profit_at_risk_unanswered = 400 * 0.5 = 200
    assert losses["profit_at_risk_unanswered"] == 200
