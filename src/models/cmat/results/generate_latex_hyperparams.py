import json
import glob
import os

def load_configs(variant):
    files = glob.glob(f"/projects/prjs2061/energy-demand-forecast-nl/src/models/cmat/results/best_config_{variant}_h*.json")
    results = []
    for f in files:
        with open(f, 'r') as fp:
            d = json.load(fp)
            results.append(d)
    results.sort(key=lambda x: x['horizon_days'])
    return results

def print_latex(variant, results):
    print(r"\begin{table}[h]")
    print(r"\centering")
    print(r"\footnotesize")
    print(r"\caption{Winning hyperparameters for CMAT-" + variant.upper() + r" across horizons.}")
    print(r"\label{tab:hyperparams_" + variant + r"}")
    print(r"\begin{tabular}{r r r r r r r r r}")
    print(r"\toprule")
    print(r"\textbf{H (d)} & \textbf{CW (h)} & \textbf{Dim} & \textbf{Heads} & \textbf{Depth} & \textbf{Drop} & \textbf{BS} & \textbf{LR} & \textbf{WD} \\")
    print(r"\midrule")
    for r in results:
        h = r['horizon_days']
        cw = r['context_window_hours']
        dim = r['embed_dim']
        heads = r['self_attn_heads']
        depth = r['transformer_depth']
        drop = r['dropout']
        bs = r['batch_size']
        lr = r['learning_rate']
        wd = r['weight_decay']
        
        # format LR and WD in scientific notation if they are small
        def fmt_sci(val):
            return f"{val:.1e}".replace('e-0', 'e-').replace('e+0', 'e+')
            
        print(f"{h} & {cw} & {dim} & {heads} & {depth} & {drop:.1f} & {bs} & {fmt_sci(lr)} & {fmt_sci(wd)} \\\\")
    print(r"\bottomrule")
    print(r"\end{tabular}")
    print(r"\end{table}")
    print()

tab_results = load_configs("tab")
ntl_results = load_configs("ntl")

print_latex("tab", tab_results)
print_latex("ntl", ntl_results)

