# Hybrid Production Repair v2

v1は `update_prediction_history_v2.py` に存在しない `generate_v7_predictions*` を探して停止しました。

実Repositoryを確認した結果:
- app.py -> generate_v7_predictions_cached
- update_prediction_history_v2.py -> generate_predictions
- build_sim_numbers_v7.py -> generate_v7_predictions

v2はこの実構造に合わせて個別に配線します。

Production:
- N3 = V8 Repeat Penalty + BOX-class Top5
- N4 = V7 Fixed Rank維持

実行:
run_numbers_ai_v8_hybrid_production_and_push_v2.bat

v1は再実行しないでください。
