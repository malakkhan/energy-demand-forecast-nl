import json
import glob
import os

files = glob.glob("/projects/prjs2061/energy-demand-forecast-nl/src/models/cmat/results/best_config_full_h*.json")
results = []
for f in files:
    with open(f, 'r') as fp:
        d = json.load(fp)
        results.append(d)

results.sort(key=lambda x: x.get('horizon_days', 0))

print("Performance of CMAT-Full from Optuna best configs:")
print(f"{'Horizon':<10} | {'Val Loss':<10} | {'Test RMSE':<10} | {'Test MAE':<10} | {'Test MAPE':<10} | {'Test PCC':<10}")
print("-" * 75)
for r in results:
    h = r.get('horizon_days', 'N/A')
    vl = r.get('best_val_loss', 0)
    rmse = r.get('test_rmse', 0)
    mae = r.get('test_mae', 0)
    mape = r.get('test_mape', 0)
    pcc = r.get('test_pcc', 0)
    print(f"{h:<10} | {vl:<10.4f} | {rmse:<10.1f} | {mae:<10.1f} | {mape:<10.2f} | {pcc:<10.4f}")

