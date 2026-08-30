import re

def patch_naive(filepath):
    with open(filepath, "r") as f:
        code = f.read()
    
    code = re.sub(
        r'(elapsed = time\.time\(\) - t0)',
        r'infer_time_s = time.time() - t0\n    elapsed = infer_time_s\n    train_time_s = 0.0',
        code
    )
    
    code = re.sub(
        r'("n_test": [^,]+,)',
        r'\1\n        "train_time_s": round(train_time_s, 2),\n        "infer_time_s": round(infer_time_s, 2),',
        code
    )
    
    code = re.sub(
        r'("n_test": result\.get\("n_test", 0\),| "n_test": result\["n_test"\],)',
        r'\1\n                    "n_val": 0,\n                    "train_time_s": result.get("train_time_s", 0.0),\n                    "infer_time_s": result.get("infer_time_s", 0.0),\n                    "seed": "N/A",',
        code
    )
    
    with open(filepath, "w") as f:
        f.write(code)

patch_naive("src/models/baselines/train_naive_baselines.py")
patch_naive("src/models/baselines/train_seasonal_naive.py")
