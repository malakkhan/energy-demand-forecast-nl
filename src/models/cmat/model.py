"""Cross-Modal Attention Transformer (CMAT) — PyTorch model.

Implements all four ablation variants:
    - CMAT-Tab   : Tabular self-attention only (spatial branch disabled)
    - CMAT-NTL   : Tabular + scalar NTL aggregate (no spatial branch)
    - CMAT-Early : Both branches, early fusion via concatenation + shared SA
    - CMAT-Full  : Both branches, dedicated cross-modal attention

Architecture follows the thesis appendix specification exactly:
    Phase 1: Modality-specific feature encoding
    Phase 2: Temporal self-attention + cross-modal fusion
    Phase 3: Factorised probabilistic decoder
"""

import math
from typing import Optional

import torch
import torch.nn as nn
import torch.nn.functional as F

from . import config as C
from .config import CMATConfig, CMATVariant


# ═══════════════════════════════════════════════════════════════════════════
# Positional Encodings
# ═══════════════════════════════════════════════════════════════════════════

class LearnedPositionalEncoding(nn.Module):
    """Learned positional encoding for temporal tokens."""

    def __init__(self, max_len: int, d_model: int):
        super().__init__()
        self.pe = nn.Embedding(max_len, d_model)

    def forward(self, seq_len: int) -> torch.Tensor:
        """Returns (seq_len, d_model)."""
        positions = torch.arange(seq_len, device=self.pe.weight.device)
        return self.pe(positions)


class SpatialPositionalEncoding(nn.Module):
    """Learned 2D spatial positional encoding for image patches (SPE)."""

    def __init__(self, n_patches_h: int, n_patches_w: int, d_model: int):
        super().__init__()
        self.row_embed = nn.Embedding(n_patches_h, d_model)
        self.col_embed = nn.Embedding(n_patches_w, d_model)
        self.n_patches_h = n_patches_h
        self.n_patches_w = n_patches_w

    def forward(self) -> torch.Tensor:
        """Returns (N_p, d_model) where N_p = n_patches_h * n_patches_w."""
        device = self.row_embed.weight.device
        rows = torch.arange(self.n_patches_h, device=device)
        cols = torch.arange(self.n_patches_w, device=device)
        row_enc = self.row_embed(rows).unsqueeze(1).expand(
            -1, self.n_patches_w, -1
        )  # (H, W, d)
        col_enc = self.col_embed(cols).unsqueeze(0).expand(
            self.n_patches_h, -1, -1
        )  # (H, W, d)
        return (row_enc + col_enc).reshape(-1, row_enc.shape[-1])  # (N_p, d)


class DayPositionalEncoding(nn.Module):
    """Learned day positional encoding (DPE) for multi-day image stacks."""

    def __init__(self, max_days: int, d_model: int):
        super().__init__()
        self.day_embed = nn.Embedding(max_days, d_model)

    def forward(self, day_idx: int) -> torch.Tensor:
        """Returns (d_model,) for day index k."""
        device = self.day_embed.weight.device
        return self.day_embed(torch.tensor(day_idx, device=device))


# ═══════════════════════════════════════════════════════════════════════════
# Phase 1: Tabular Feature Encoder
# ═══════════════════════════════════════════════════════════════════════════

class TabularEncoder(nn.Module):
    """Encodes 32 continuous + 4 categorical features → d-dimensional tokens.

    z_t^(0) = W_c * x_cont_t + b_c + Σ_j E_j[x_j_t] + PE(t)
    """

    def __init__(self, cfg: CMATConfig):
        super().__init__()
        d = cfg.embed_dim

        # Determine number of continuous features
        n_cont = C.N_CONTINUOUS
        if cfg.uses_ntl_scalar:
            n_cont += 1  # add ntl_a2_all_mean for CMAT-NTL variant

        # Continuous projection: W_c * x_cont + b_c
        self.cont_proj = nn.Linear(n_cont, d)

        # Categorical embeddings: E_j for each categorical feature
        self.cat_embeddings = nn.ModuleDict()
        for feat_name in C.CATEGORICAL_FEATURES:
            cardinality = C.CATEGORICAL_CARDINALITIES[feat_name]
            self.cat_embeddings[feat_name] = nn.Embedding(cardinality, d)

        # Temporal positional encoding PE(t)
        max_window = max(C.SEARCH_SPACE["context_window_hours"]) + 1
        self.temporal_pe = LearnedPositionalEncoding(max_window, d)

    def forward(
        self,
        x_cont: torch.Tensor,
        x_cat: torch.Tensor,
    ) -> torch.Tensor:
        """
        Parameters
        ----------
        x_cont : (B, W, N_cont) — continuous features, float
        x_cat  : (B, W, N_cat) — categorical features, long

        Returns
        -------
        Z_tab^(0) : (B, W, d)
        """
        B, W, _ = x_cont.shape

        # h_cont = W_c * x_cont + b_c
        h_cont = self.cont_proj(x_cont)  # (B, W, d)

        # h_cat = Σ_j E_j[x_j]
        h_cat = torch.zeros_like(h_cont)
        for j, feat_name in enumerate(C.CATEGORICAL_FEATURES):
            cat_idx = x_cat[:, :, j].long()  # (B, W)
            h_cat = h_cat + self.cat_embeddings[feat_name](cat_idx)

        # z_t^(0) = h_cont + h_cat + PE(t)
        pe = self.temporal_pe(W)  # (W, d)
        z = h_cont + h_cat + pe.unsqueeze(0)  # (B, W, d)
        return z


# ═══════════════════════════════════════════════════════════════════════════
# Phase 1: Spatial Patch Encoder
# ═══════════════════════════════════════════════════════════════════════════

class SpatialPatchEncoder(nn.Module):
    """Encodes D_W daily NTL images via linear patch embedding + SPE + DPE.

    z_img^(k,i) = W_patch * flatten(p_i^(k)) + b_patch + SPE(i) + DPE(k)
    """

    def __init__(self, cfg: CMATConfig):
        super().__init__()
        P = cfg.ntl_patch_size
        d = cfg.embed_dim

        # Linear patch embedding via Conv2d
        self.patch_embed = nn.Conv2d(
            in_channels=1,
            out_channels=d,
            kernel_size=P,
            stride=P,
            bias=True,
        )

        # Compute patch grid dimensions
        self.n_patches_h = C.NTL_IMG_H // P
        self.n_patches_w = C.NTL_IMG_W // P
        self.n_patches = self.n_patches_h * self.n_patches_w

        # Spatial positional encoding (per-patch position within image)
        self.spe = SpatialPositionalEncoding(
            self.n_patches_h, self.n_patches_w, d
        )

        # Day positional encoding (which day in the lookback window)
        max_days = max(
            max(1, math.ceil(w / 24))
            for w in C.SEARCH_SPACE["context_window_hours"]
        ) + 1
        self.dpe = DayPositionalEncoding(max_days, d)

        # Learnable [MASK] token for dynamic image masking
        self.mask_token = nn.Parameter(torch.randn(d) * 0.02)

    def forward(
        self,
        images: torch.Tensor,
        mask_ratio: float = 0.0,
    ) -> torch.Tensor:
        """
        Parameters
        ----------
        images : (B, D_W, 1, H, W) — daily NTL images
        mask_ratio : float — fraction of patches to mask (training only)

        Returns
        -------
        Z_img : (B, D_W * N_p, d) — or fewer tokens if masked
        """
        B, D_W = images.shape[0], images.shape[1]
        d = self.patch_embed.out_channels
        device = images.device

        all_tokens = []

        spe = self.spe()  # (N_p, d)

        for k in range(D_W):
            # Patch embed: (B, 1, H, W) → (B, d, n_h, n_w) → (B, N_p, d)
            img_k = images[:, k]  # (B, 1, H, W)
            patches = self.patch_embed(img_k)  # (B, d, n_h, n_w)
            patches = patches.flatten(2).transpose(1, 2)  # (B, N_p, d)

            # Add SPE and DPE
            dpe_k = self.dpe(k)  # (d,)
            tokens = patches + spe.unsqueeze(0) + dpe_k  # (B, N_p, d)

            # Dynamic image masking during training
            if mask_ratio > 0 and self.training:
                n_mask = int(self.n_patches * mask_ratio)
                n_keep = self.n_patches - n_mask
                # Per-batch random mask
                noise = torch.rand(B, self.n_patches, device=device)
                ids_sorted = noise.argsort(dim=1)
                ids_keep = ids_sorted[:, :n_keep]  # (B, n_keep)
                ids_mask = ids_sorted[:, n_keep:]  # (B, n_mask)

                # Gather kept tokens
                ids_keep_exp = ids_keep.unsqueeze(-1).expand(-1, -1, d)
                kept = torch.gather(tokens, 1, ids_keep_exp)

                # Replace masked tokens with [MASK]
                mask_tokens = self.mask_token.unsqueeze(0).unsqueeze(0).expand(
                    B, n_mask, -1
                )

                # Reconstruct full sequence with mask tokens in-place
                full = tokens.clone()
                ids_mask_exp = ids_mask.unsqueeze(-1).expand(-1, -1, d)
                full.scatter_(1, ids_mask_exp, mask_tokens)
                tokens = full

            all_tokens.append(tokens)

        # Concatenate across days: (B, D_W * N_p, d)
        Z_img = torch.cat(all_tokens, dim=1)
        return Z_img


# ═══════════════════════════════════════════════════════════════════════════
# Phase 2: Transformer Encoder Layer
# ═══════════════════════════════════════════════════════════════════════════

class TransformerEncoderLayer(nn.Module):
    """Standard pre-norm transformer encoder layer.

    Z' = LN(Z + MSA(Z))
    Z_out = LN(Z' + FFN(Z'))
    """

    def __init__(self, d_model: int, n_heads: int, dropout: float):
        super().__init__()
        self.norm1 = nn.LayerNorm(d_model)
        self.attn = nn.MultiheadAttention(
            d_model, n_heads, dropout=dropout, batch_first=True
        )
        self.norm2 = nn.LayerNorm(d_model)
        self.ffn = nn.Sequential(
            nn.Linear(d_model, d_model * C.FFN_EXPANSION),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(d_model * C.FFN_EXPANSION, d_model),
            nn.Dropout(dropout),
        )
        self.dropout = nn.Dropout(dropout)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # Self-attention with residual
        x_norm = self.norm1(x)
        attn_out, _ = self.attn(x_norm, x_norm, x_norm)
        x = x + self.dropout(attn_out)
        # FFN with residual
        x_norm = self.norm2(x)
        x = x + self.ffn(x_norm)
        return x


# ═══════════════════════════════════════════════════════════════════════════
# Phase 2: Cross-Modal Attention Layer
# ═══════════════════════════════════════════════════════════════════════════

class CrossModalAttention(nn.Module):
    """Cross-attention: Q from temporal, K/V from spatial.

    Z_cross = softmax(Q K^T / sqrt(d_k)) V
    Z_fused = LN(Z_tab + Z_cross)
    """

    def __init__(self, d_model: int, n_heads: int, dropout: float):
        super().__init__()
        self.norm_q = nn.LayerNorm(d_model)
        self.norm_kv = nn.LayerNorm(d_model)
        self.cross_attn = nn.MultiheadAttention(
            d_model, n_heads, dropout=dropout, batch_first=True
        )
        self.norm_out = nn.LayerNorm(d_model)
        self.dropout = nn.Dropout(dropout)

    def forward(
        self,
        z_tab: torch.Tensor,
        z_img: torch.Tensor,
    ) -> torch.Tensor:
        """
        Parameters
        ----------
        z_tab : (B, W, d) — temporal query tokens
        z_img : (B, D_W * N_p, d) — spatial key-value tokens

        Returns
        -------
        z_fused : (B, W, d)
        """
        q = self.norm_q(z_tab)
        kv = self.norm_kv(z_img)
        cross_out, _ = self.cross_attn(q, kv, kv)
        z_fused = self.norm_out(z_tab + self.dropout(cross_out))
        return z_fused


# ═══════════════════════════════════════════════════════════════════════════
# Phase 3: Factorised Probabilistic Decoder
# ═══════════════════════════════════════════════════════════════════════════

class FactorisedDecoder(nn.Module):
    """Factorised bilinear decoder.

    Ŷ = W_H^T · Z_fused · W_τ  ∈ R^{H_pred × 3}

    where W_H ∈ R^{W × H_pred} and W_τ ∈ R^{d × 3}.
    """

    def __init__(self, d_model: int, max_window: int, n_quantiles: int = 3):
        super().__init__()
        self.d_model = d_model
        self.n_quantiles = n_quantiles

        # W_τ: maps d → n_quantiles
        self.W_tau = nn.Linear(d_model, n_quantiles, bias=False)

        # W_H will be set dynamically based on (W, H_pred)
        # We use a parameter that maps from context length to horizon
        # Since W and H_pred vary, we use a flexible approach:
        # project each token to a scalar weight, then weighted-sum → horizon
        self._W_H = None  # lazily initialised

    def set_horizon(self, W: int, H_pred: int):
        """Set or update the W_H projection for current (W, H_pred)."""
        device = self.W_tau.weight.device
        # W_H ∈ R^{W × H_pred}: maps W input timesteps → H_pred output steps
        if (self._W_H is None or
                self._W_H.shape != (W, H_pred)):
            self._W_H = nn.Parameter(
                torch.randn(W, H_pred, device=device) * 0.02
            )

    def forward(
        self,
        z_fused: torch.Tensor,
        H_pred: int,
    ) -> torch.Tensor:
        """
        Parameters
        ----------
        z_fused : (B, W, d) — fused latent representation
        H_pred  : int — forecast horizon in hours

        Returns
        -------
        y_hat : (B, H_pred, n_quantiles)
        """
        B, W, d = z_fused.shape

        # Ensure W_H is properly sized
        if self._W_H is None or self._W_H.shape != (W, H_pred):
            self._W_H = nn.Parameter(
                torch.randn(W, H_pred, device=z_fused.device) * 0.02
            )

        # Ŷ = W_H^T · Z_fused · W_τ
        # Step 1: Z_fused · W_τ → (B, W, n_quantiles)
        z_q = self.W_tau(z_fused)  # (B, W, 3)

        # Step 2: W_H^T · (result) → (B, H_pred, n_quantiles)
        # W_H^T is (H_pred, W), z_q is (B, W, 3)
        y_hat = torch.einsum("hw,bwq->bhq", self._W_H.T, z_q)

        return y_hat


# ═══════════════════════════════════════════════════════════════════════════
# Full CMAT Model
# ═══════════════════════════════════════════════════════════════════════════

class CMAT(nn.Module):
    """Cross-Modal Attention Transformer.

    Supports four variants via ``cfg.variant``.
    """

    def __init__(self, cfg: CMATConfig):
        super().__init__()
        self.cfg = cfg
        d = cfg.embed_dim

        # ── Phase 1: Tabular encoder ──
        self.tabular_encoder = TabularEncoder(cfg)

        # ── Phase 1: Spatial encoder (for EARLY_FUSION and FULL) ──
        if cfg.uses_spatial:
            self.spatial_encoder = SpatialPatchEncoder(cfg)
        else:
            self.spatial_encoder = None

        # ── Phase 2: Self-attention layers ──
        self.sa_layers = nn.ModuleList([
            TransformerEncoderLayer(d, cfg.self_attn_heads, cfg.dropout)
            for _ in range(cfg.transformer_depth)
        ])

        # ── Phase 2: Cross-modal attention (FULL variant only) ──
        if cfg.variant == CMATVariant.FULL:
            self.cross_attn = CrossModalAttention(
                d, cfg.cross_attn_heads, cfg.dropout
            )
        else:
            self.cross_attn = None

        # ── Phase 2: Shared SA layers for early fusion ──
        if cfg.variant == CMATVariant.EARLY_FUSION:
            self.early_fusion_layers = nn.ModuleList([
                TransformerEncoderLayer(d, cfg.self_attn_heads, cfg.dropout)
                for _ in range(cfg.transformer_depth)
            ])
        else:
            self.early_fusion_layers = None

        # ── Phase 3: Decoder ──
        max_window = max(C.SEARCH_SPACE["context_window_hours"]) + 1
        self.decoder = FactorisedDecoder(d, max_window, C.N_QUANTILES)

    def forward(
        self,
        x_cont: torch.Tensor,
        x_cat: torch.Tensor,
        images: Optional[torch.Tensor] = None,
        H_pred: Optional[int] = None,
    ) -> torch.Tensor:
        """
        Parameters
        ----------
        x_cont : (B, W, N_cont) — continuous features
        x_cat  : (B, W, N_cat) — categorical features (long)
        images : (B, D_W, 1, H_img, W_img) — NTL images (optional)
        H_pred : int — forecast horizon in hours

        Returns
        -------
        y_hat : (B, H_pred, 3) — quantile forecasts [q05, q50, q95]
        """
        if H_pred is None:
            H_pred = self.cfg.horizon_hours

        # ── Phase 1: Encode tabular ──
        Z_tab = self.tabular_encoder(x_cont, x_cat)  # (B, W, d)

        # ── Phase 2: Self-attention ──
        for layer in self.sa_layers:
            Z_tab = layer(Z_tab)
        # Z_tab is now Z_tab^(L)

        # ── Phase 2: Fusion ──
        if self.cfg.variant == CMATVariant.FULL and images is not None:
            # Spatial encoding
            Z_img = self.spatial_encoder(
                images, mask_ratio=self.cfg.image_mask_ratio
            )
            # Cross-modal attention: Q from Z_tab, K/V from Z_img
            Z_fused = self.cross_attn(Z_tab, Z_img)

        elif self.cfg.variant == CMATVariant.EARLY_FUSION and images is not None:
            # Spatial encoding
            Z_img = self.spatial_encoder(
                images, mask_ratio=self.cfg.image_mask_ratio
            )
            # Concatenate along sequence axis
            Z_concat = torch.cat([Z_tab, Z_img], dim=1)  # (B, W + D_W*N_p, d)
            # Shared self-attention
            for layer in self.early_fusion_layers:
                Z_concat = layer(Z_concat)
            # Extract tabular tokens (first W)
            Z_fused = Z_concat[:, :Z_tab.shape[1], :]

        else:
            # TAB_ONLY or TAB_NTL: no spatial fusion
            Z_fused = Z_tab

        # ── Phase 3: Decode ──
        y_hat = self.decoder(Z_fused, H_pred)  # (B, H_pred, 3)

        return y_hat

    def count_parameters(self) -> int:
        """Count total trainable parameters."""
        return sum(p.numel() for p in self.parameters() if p.requires_grad)
