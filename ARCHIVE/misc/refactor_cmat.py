import re

def patch_cmat(filepath):
    with open(filepath, "r") as f:
        code = f.read()

    # CMAT train_one_fold signature
    code = re.sub(
        r'("n_train": len\(train_df\),\n\s+"n_test": len\(test_df\),)',
        r'"n_train_rows": len(train_df),\n        "n_val_rows": len(val_df),\n        "n_test_rows": len(test_df),\n        "n_train": len(train_dataset) if "train_dataset" in locals() else len(train_df),\n        "n_val": len(val_dataset) if "val_dataset" in locals() else len(val_df),\n        "n_test": len(test_dataset) if "test_dataset" in locals() else len(test_df),\n        "train_time_s": round(train_time_s, 2) if "train_time_s" in locals() else 0.0,\n        "infer_time_s": round(infer_time_s, 2) if "infer_time_s" in locals() else 0.0,',
        code
    )

    code = re.sub(
        r'(for epoch in range\(.*?:\n[\s\S]*?)(model\.eval\(\))',
        r't_train_start = time.time()\n    \1train_time_s = time.time() - t_train_start\n\n    t_infer_start = time.time()\n    \2',
        code
    )

    code = re.sub(
        r'(metrics = compute_metrics\(y_true_mw, y_pred_mw\))',
        r'infer_time_s = time.time() - t_infer_start if "t_infer_start" in locals() else 0.0\n    \1',
        code
    )

    code = re.sub(
        r'(\*\*metrics\.to_dict\(\),)',
        r'\1\n            "n_val": fold_result.get("n_val", 0),\n            "train_time_s": fold_result.get("train_time_s", 0.0),\n            "infer_time_s": fold_result.get("infer_time_s", 0.0),',
        code
    )

    # MASE is already printed/saved by compute_metrics (inside metrics.to_dict())
    
    with open(filepath, "w") as f:
        f.write(code)

patch_cmat("src/models/cmat/train.py")
