import json
import glob

files = glob.glob("/projects/prjs2061/energy-demand-forecast-nl/src/models/cmat/results/best_config_full_h*.json")
results = []
for f in files:
    with open(f, 'r') as fp:
        d = json.load(fp)
        results.append(d)

results.sort(key=lambda x: x.get('horizon_days', 0))

def fmt_sci(val):
    return f"{val:.1e}".replace('e-0', 'e-').replace('e+0', 'e+')

print("| Horizon | CW (h) | Dim | Heads | Depth | Drop | BS | LR | WD | PCC (Performance) |")
print("|---------|--------|-----|-------|-------|------|----|----|----|-------------------|")
for r in results:
    h = r.get('horizon_days', 'N/A')
    cw = r.get('context_window_hours', 'N/A')
    dim = r.get('embed_dim', 'N/A')
    heads = r.get('self_attn_heads', 'N/A')
    depth = r.get('transformer_depth', 'N/A')
    drop = r.get('dropout', 0)
    bs = r.get('batch_size', 'N/A')
    lr = r.get('learning_rate', 0)
    wd = r.get('weight_decay', 0)
    pcc = r.get('test_pcc', 0)
    
    status = "🌟 AMAZING" if pcc > 0.9 else "❌ FAILED"
    
    print(f"| {h} | {cw} | {dim} | {heads} | {depth} | {drop:.1f} | {bs} | {fmt_sci(lr)} | {fmt_sci(wd)} | {pcc:.4f} ({status}) |")

