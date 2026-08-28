import re

def patch_dl_script(filepath, model_name):
    with open(filepath, "r") as f:
        code = f.read()

    # 1. Add seeds import if needed
    if "RANDOM_SEEDS" not in code:
        code = code.replace("from src.models.baselines.config import ROCVConfig", "from src.models.baselines.config import ROCVConfig, RANDOM_SEEDS")

    # 2. Modify fold function signature to accept seed
    code = re.sub(
        r'(def run_.*?_fold\([\s\S]*?horizon_days: int,)\n',
        r'\1\n    seed: int = 42,\n',
        code
    )

    # 3. Add sequence counting inside the fold (n_train_seq, n_val_seq, n_test_seq)
    code = re.sub(
        r'("n_train": len\(train_df\),\n\s+"n_test": len\(test_df\),)',
        r'"n_train_rows": len(train_df),\n        "n_val_rows": len(val_df),\n        "n_test_rows": len(test_df),\n        "n_train": len(train_dataset) if "train_dataset" in locals() else len(train_df),\n        "n_val": len(val_dataset) if "val_dataset" in locals() else len(val_df),\n        "n_test": len(test_dataset) if "test_dataset" in locals() else len(test_df),\n        "train_time_s": round(train_time_s, 2) if "train_time_s" in locals() else 0.0,\n        "infer_time_s": round(infer_time_s, 2) if "infer_time_s" in locals() else 0.0,',
        code
    )
    # Handle y_true_mw case (Seq2Seq uses y_true_mw for len)
    code = re.sub(
        r'("n_train": len\(train_df\),\n\s+"n_test": len\(y_true_mw\),)',
        r'"n_train_rows": len(train_df),\n        "n_val_rows": len(val_df),\n        "n_test_rows": len(test_df),\n        "n_train": len(train_dataset) if "train_dataset" in locals() else len(train_df),\n        "n_val": len(val_dataset) if "val_dataset" in locals() else len(val_df),\n        "n_test": len(test_dataset) if "test_dataset" in locals() else len(y_true_mw),\n        "train_time_s": round(train_time_s, 2) if "train_time_s" in locals() else 0.0,\n        "infer_time_s": round(infer_time_s, 2) if "infer_time_s" in locals() else 0.0,',
        code
    )

    # 4. Wrap model training loop with t_train
    code = re.sub(
        r'(model\.train\(\)\n\s+for epoch in range\(.*?:\n[\s\S]*?)(model\.eval\(\))',
        r't_train_start = time.time()\n    \1train_time_s = time.time() - t_train_start\n\n    t_infer_start = time.time()\n    \2',
        code
    )
    # For Prophet (it has no PyTorch train epoch loop, it fits directly)
    if "model.train()" not in code and "model = Prophet" in code:
        code = re.sub(
            r'(model\.fit\(train_prophet\))',
            r't_train_start = time.time()\n    \1\n    train_time_s = time.time() - t_train_start\n    t_infer_start = time.time()',
            code
        )
    # For SARIMAX (Stage 1 is SARIMAX, Stage 2 is LSTM)
    if "SARIMAX" in model_name:
        code = re.sub(
            r'(model = SARIMAX\([\s\S]*?)\n\s+sarimax_res = model\.fit\([\s\S]*?\)',
            r't_train_start = time.time()\n    \1\n        sarimax_res = model.fit(disp=False)\n        train_time_s = time.time() - t_train_start\n        t_infer_start = time.time()',
            code
        )

    # 5. Measure infer_time_s right before compute_metrics
    code = re.sub(
        r'(metrics = compute_metrics\(y_true_mw, y_pred_mw\))',
        r'infer_time_s = time.time() - t_infer_start if "t_infer_start" in locals() else 0.0\n    \1',
        code
    )
    # MLR / generic y_true, y_pred case
    code = re.sub(
        r'(metrics = compute_metrics\(y_true, y_pred\))',
        r'infer_time_s = time.time() - t_infer_start if "t_infer_start" in locals() else 0.0\n    \1',
        code
    )

    # 6. Main loop: Add seed looping
    if "for h_days in horizons:" in code and "for seed in RANDOM_SEEDS:" not in code:
        old_loop = re.search(r'(for h_days in horizons:\n(?:[ \t]+.*?\n)+)', code)
        if old_loop:
            indent = re.search(r'^([ \t]+)', old_loop.group(1), re.MULTILINE).group(1)
            # Indent the whole block
            block_lines = old_loop.group(1).split('\n')
            new_block = []
            for line in block_lines:
                if line.startswith(indent):
                    new_block.append(indent + line)
                else:
                    new_block.append(line)
            
            new_code = f"{indent}for seed in RANDOM_SEEDS:\n{indent}    import torch, random, numpy\n{indent}    torch.manual_seed(seed)\n{indent}    random.seed(seed)\n{indent}    numpy.random.seed(seed)\n{indent}    logger.info(\"=' * 40)\")\n{indent}    logger.info(\"SEED = %d\", seed)\n{indent}    logger.info(\"=' * 40)\")\n" + "\n".join(new_block)
            # Replace the signature of run_xxx_fold inside the loop to pass seed=seed
            new_code = re.sub(r'(run_.*?_fold\([\s\S]*?h_days)', r'\1, seed=seed', new_code)
            code = code.replace(old_loop.group(1), new_code)

    # 7. Add seed, mase, times to result_row
    code = re.sub(
        r'("fold": fold_idx,)',
        r'\1\n                    "seed": seed if "seed" in locals() else "N/A",',
        code
    )
    code = re.sub(
        r'(\*\*metrics\.to_dict\(\),)',
        r'\1\n                    "n_val": fold_result.get("n_val", 0),\n                    "train_time_s": fold_result.get("train_time_s", 0.0),\n                    "infer_time_s": fold_result.get("infer_time_s", 0.0),',
        code
    )

    with open(filepath, "w") as f:
        f.write(code)
    print(f"Patched {filepath}")

patch_dl_script("src/models/baselines/train_seq2seq_v2.py", "seq2seq")
patch_dl_script("src/models/baselines/train_prophet_lstm_v2.py", "prophet_lstm")
patch_dl_script("src/models/baselines/train_sarimax_lstm_v2.py", "sarimax_lstm")
