import itertools
import math
import random

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from Home import configure_page, password_gate, render_sidebar

configure_page(page_title="Aircraft Cargo Fit Checker")
password_gate()
render_sidebar()


# ============================================================
# Config / Title
# ============================================================
st.title("Aircraft Cargo Fit Checker")

st.markdown(
    "⚠️ **Disclaimer:** This tool provides an *estimated* packing feasibility only. "
    "Actual results may vary depending on actual baggage size, materials, fullness, and shape. "
    )

# ============================================================
# Predefined Containers
# ============================================================
containers = {
    "CJ": {
        "door": {"width_min": 24, "width_max": 26, "height": 20, "diag": 31},
        "interior": {
            "height": 22,         # z
            "depth": 45,          # x (front -> back)
            "width": 84,          # y (left -> right)
            "restricted": {"width": 20, "depth": 20},   # near the door, right side (y high, x near 0)
            # Long tunnel: against the BACK WALL (x near cargo_L), spans full width (y 0->84)
            "tunnel": {"depth": 24, "width": 84}
        }
    },
    "Legacy": {
        "door": {"width": 34, "height": 22, "diag": 38},
        "interior": {
            "height": 36,
            "depth": 89,
            "width_min": 36,
            "width_max": 54
        }
    }
}

# ============================================================
# Standard Baggage Presets (with Flexibility)
# ============================================================
standard_baggage = {
    "Small Carry-on": {"dims": (22, 14, 9), "flex": 1.0},
    "Standard Suitcase": {"dims": (26, 18, 10), "flex": 1.0},
    "Large Suitcase": {"dims": (30, 19, 11), "flex": 1.0},
    "Golf Clubs (Soft Bag)": {"dims": (55, 13, 13), "flex": 0.85},
    "Ski Bag (Soft)": {"dims": (70, 12, 7), "flex": 0.9},
    "Custom": {"dims": None, "flex": 1.0}
}

# ============================================================
# Helper Functions
# ============================================================
def fits_through_door(box_dims, door, flex=1.0):
    dims_flex = apply_flex(box_dims, flex)
    for dims in itertools.permutations(dims_flex):
        bw, bh = dims[0], dims[1]
        diag = math.hypot(bw, bh)
        if "width_min" in door:  # CJ style door with narrowest span
            width_ok_min = bw <= door["width_min"]
            width_ok_max = bw <= door["width_max"]
            height_ok = bh <= door["height"]
            if width_ok_min and height_ok:
                return True
            if width_ok_min and width_ok_max and diag <= door["diag"]:
                return True
        else:  # Legacy style door (single width)
            width_ok = bw <= door["width"]
            height_ok = bh <= door["height"]
            if width_ok and height_ok:
                return True
            if width_ok and diag <= door["diag"]:
                return True
    return False

def legacy_width_at_height(interior, z):
    """Linear interpolation of width at height z (Legacy taper)."""
    h = interior["height"]
    wmin, wmax = interior["width_min"], interior["width_max"]
    return wmin + (wmax - wmin) * (z / h)

def apply_flex(dims, flex):
    """Apply flexibility/squish factor (for fit checks only)."""
    l, w, h = dims
    return (l * flex, w * flex, h * flex)

def fits_in_space(box_dims, space_dims):
    """Return an oriented (l,w,h) that fits inside space_dims (L,W,H), else None."""
    l, w, h = box_dims
    for dims in itertools.permutations([l, w, h]):
        bl, bw, bh = dims
        if bl <= space_dims[0] and bw <= space_dims[1] and bh <= space_dims[2]:
            return dims
    return None

def can_fit_rotated_in_plane(item_length, item_width, space_length, space_width, step_degrees=1.0):
    """Check if a rectangle can fit in a rectangle via in-plane rotation."""
    for angle_deg in [i * step_degrees for i in range(int(90 / step_degrees) + 1)]:
        angle = math.radians(angle_deg)
        cos_a = abs(math.cos(angle))
        sin_a = abs(math.sin(angle))
        proj_length = item_length * cos_a + item_width * sin_a
        proj_width = item_length * sin_a + item_width * cos_a
        if proj_length <= space_length and proj_width <= space_width:
            return True
    return False

def fits_inside(box_dims, interior, container_type, flex=1.0, allow_diagonal=False):
    """Check if a single box can fit somewhere in the empty hold (not a packing check)."""
    dims_flex = apply_flex(box_dims, flex)
    for dims in itertools.permutations(dims_flex):
        bl, bw, bh = dims
        if container_type == "CJ":
            if bh <= interior["height"] and bl <= interior["depth"] and bw <= interior["width"]:
                r = interior["restricted"]
                main_depth = max(0.0, interior["depth"] - r["depth"])
                main_width = max(0.0, interior["width"] - r["width"])
                fits_outside_restricted = (
                    bl > r["depth"]
                    or bw > r["width"]
                    or bl <= main_depth
                    or bw <= main_width
                )
                if fits_outside_restricted:
                    return True, False
            if allow_diagonal and bh <= interior["height"]:
                r = interior["restricted"]
                main_depth = max(0.0, interior["depth"] - r["depth"])
                main_width = max(0.0, interior["width"] - r["width"])
                diagonal_spaces = [
                    (r["depth"], main_width),
                    (main_depth, interior["width"]),
                    (interior["depth"], interior["width"]),
                ]
                for space_depth, space_width in diagonal_spaces:
                    if can_fit_rotated_in_plane(bl, bw, space_depth, space_width):
                        return True, True
        elif container_type == "Legacy":
            width_limit = min(
                legacy_width_at_height(interior, 0),
                legacy_width_at_height(interior, bh)
            )
            if bl <= interior["depth"] and bh <= interior["height"]:
                if bw <= width_limit:
                    return True, False
            if allow_diagonal and bh <= interior["height"]:
                if can_fit_rotated_in_plane(bl, bw, interior["depth"], width_limit):
                    return True, True
    return False, False

def bag_volume(dims):
    l, w, h = dims
    return l * w * h

def cargo_volume(interior, container_type):
    if container_type == "CJ":
        main_vol = interior["depth"] * interior["width"] * interior["height"]
        restricted = interior["restricted"]
        restricted_vol = restricted["depth"] * restricted["width"] * interior["height"]
        return max(0.0, main_vol - restricted_vol)
    else:
        # approximate trapezoidal cross-section (average width)
        return interior["depth"] * ((interior["width_min"] + interior["width_max"]) / 2) * interior["height"]

# ============================================================
# Free-Space 3D Packing (with CJ Tunnel specialization)
# ============================================================
def space_volume(space):
    return space["L"] * space["W"] * space["H"]

def prune_spaces(spaces):
    pruned = []
    for i, space in enumerate(spaces):
        contained = False
        for j, other in enumerate(spaces):
            if i == j:
                continue
            if (
                space["x"] >= other["x"]
                and space["y"] >= other["y"]
                and space["z"] >= other["z"]
                and space["x"] + space["L"] <= other["x"] + other["L"]
                and space["y"] + space["W"] <= other["y"] + other["W"]
                and space["z"] + space["H"] <= other["z"] + other["H"]
                and space["Section"] == other["Section"]
            ):
                contained = True
                break
        if not contained:
            pruned.append(space)
    return pruned

def initial_spaces(container_type, interior):
    if container_type == "CJ":
        cargo_L = interior["depth"]
        cargo_W = interior["width"]
        cargo_H = interior["height"]
        r = interior["restricted"]
        t = interior["tunnel"]

        spaces = []
        # Space near door, left side (exclude restricted block)
        spaces.append({
            "x": 0.0,
            "y": 0.0,
            "z": 0.0,
            "L": r["depth"],
            "W": max(0.0, cargo_W - r["width"]),
            "H": cargo_H,
            "Section": "Main"
        })
        # Space behind restricted block, before tunnel
        spaces.append({
            "x": r["depth"],
            "y": 0.0,
            "z": 0.0,
            "L": max(0.0, cargo_L - r["depth"] - t["depth"]),
            "W": cargo_W,
            "H": cargo_H,
            "Section": "Main"
        })
        # Space beside tunnel at back wall
        spaces.append({
            "x": max(0.0, cargo_L - t["depth"]),
            "y": t["width"],
            "z": 0.0,
            "L": t["depth"],
            "W": max(0.0, cargo_W - t["width"]),
            "H": cargo_H,
            "Section": "Main"
        })
        # Tunnel space
        spaces.append({
            "x": max(0.0, cargo_L - t["depth"]),
            "y": 0.0,
            "z": 0.0,
            "L": t["depth"],
            "W": t["width"],
            "H": cargo_H,
            "Section": "Tunnel"
        })
        return [s for s in spaces if s["L"] > 0 and s["W"] > 0 and s["H"] > 0]

    # Legacy (tapered) - use max width with taper checks on placement
    return [{
        "x": 0.0,
        "y": 0.0,
        "z": 0.0,
        "L": interior["depth"],
        "W": interior["width_max"],
        "H": interior["height"],
        "Section": "Main"
    }]

def legacy_width_ok(interior, y1, z0, z1):
    from_bottom = legacy_width_at_height(interior, z0)
    to_top = legacy_width_at_height(interior, z1)
    w_avail = min(from_bottom, to_top)
    return y1 <= w_avail

def choose_oriented_fit(box_dims, space_dims, prefer_long_axis=False):
    permutations = list(itertools.permutations(box_dims))
    if prefer_long_axis:
        longest = max(box_dims)
        preferred = [dims for dims in permutations if dims[1] == longest]
        permutations = preferred + [dims for dims in permutations if dims not in preferred]
    for dims in permutations:
        bl, bw, bh = dims
        if bl <= space_dims[0] and bw <= space_dims[1] and bh <= space_dims[2]:
            return dims
    return None

def split_space(space, dims):
    l, w, h = dims
    spaces = []
    if space["L"] - l > 1e-6:
        spaces.append({
            "x": space["x"] + l,
            "y": space["y"],
            "z": space["z"],
            "L": space["L"] - l,
            "W": space["W"],
            "H": space["H"],
            "Section": space["Section"]
        })
    if space["W"] - w > 1e-6:
        spaces.append({
            "x": space["x"],
            "y": space["y"] + w,
            "z": space["z"],
            "L": l,
            "W": space["W"] - w,
            "H": space["H"],
            "Section": space["Section"]
        })
    if space["H"] - h > 1e-6:
        spaces.append({
            "x": space["x"],
            "y": space["y"],
            "z": space["z"] + h,
            "L": l,
            "W": w,
            "H": space["H"] - h,
            "Section": space["Section"]
        })
    return spaces

def greedy_3d_packing(
    baggage_list,
    container_type,
    interior,
    force_tunnel_for_long=True,
    long_threshold=50,
    allow_tunnel_for_short=False
):
    """
    Returns (success: bool, placements: list[dict]).
    Each placement: {Item, Type, Dims: (x,y,z) oriented, Position: (x0,y0,z0), Section: "Tunnel"/"Main"}
    """
    placements = []
    spaces = initial_spaces(container_type, interior)

    for i, item in enumerate(baggage_list):
        dims_flex = apply_flex(item["Dims"], item.get("Flex", 1.0))
        placed = False
        is_long = max(dims_flex) >= long_threshold

        tunnel_first = container_type == "CJ" and is_long and force_tunnel_for_long
        allowed_tunnel = container_type == "CJ" and (allow_tunnel_for_short or is_long)

        space_order = []
        if tunnel_first:
            space_order.extend([s for s in spaces if s["Section"] == "Tunnel"])
            space_order.extend([s for s in spaces if s["Section"] == "Main"])
        elif allowed_tunnel:
            space_order.extend(spaces)
        else:
            space_order.extend([s for s in spaces if s["Section"] == "Main"])

        best_choice = None
        best_space = None
        best_dims = None

        for space in space_order:
            if space["Section"] == "Tunnel" and not allowed_tunnel:
                continue
            oriented = choose_oriented_fit(
                dims_flex,
                (space["L"], space["W"], space["H"]),
                prefer_long_axis=(
                    container_type == "CJ"
                    and space["Section"] == "Tunnel"
                    and is_long
                    and force_tunnel_for_long
                )
            )
            if not oriented:
                continue
            l, w, h = oriented
            x0, y0, z0 = space["x"], space["y"], space["z"]
            x1, y1, z1 = x0 + l, y0 + w, z0 + h

            if container_type == "Legacy" and not legacy_width_ok(interior, y1, z0, z1):
                continue

            leftover = space_volume(space) - (l * w * h)
            if best_choice is None or leftover < best_choice:
                best_choice = leftover
                best_space = space
                best_dims = (l, w, h)

        if best_space and best_dims:
            l, w, h = best_dims
            placements.append({
                "Item": i + 1,
                "Type": item["Type"],
                "Dims": best_dims,
                "Position": (best_space["x"], best_space["y"], best_space["z"]),
                "Section": best_space["Section"]
            })
            spaces.remove(best_space)
            spaces.extend(split_space(best_space, best_dims))
            spaces = prune_spaces([s for s in spaces if s["L"] > 1e-6 and s["W"] > 1e-6 and s["H"] > 1e-6])
            placed = True

        if not placed:
            return False, placements

    return True, placements

# ============================================================
# Multi-Strategy Packing Wrapper
# ============================================================
def multi_strategy_packing(baggage_list, container_type, interior):
    # Different orderings can impact greedy results
    strategies = {
        "Original Order": baggage_list,
        "Largest Volume First": sorted(baggage_list, key=lambda x: bag_volume(x["Dims"]), reverse=True),
        "Largest Dimension First": sorted(baggage_list, key=lambda x: max(x["Dims"]), reverse=True),
        "Largest Footprint First": sorted(baggage_list, key=lambda x: (x["Dims"][0] * x["Dims"][1]), reverse=True),
        "Smallest First": sorted(baggage_list, key=lambda x: bag_volume(x["Dims"]))
    }

    for seed in (7, 23, 91):
        shuffled = baggage_list[:]
        random.Random(seed).shuffle(shuffled)
        strategies[f"Random Shuffle {seed}"] = shuffled

    if container_type == "CJ":
        def is_long(item):
            return max(item["Dims"]) >= 50

        tunnel_priority = sorted(
            baggage_list,
            key=lambda x: (
                not is_long(x),
                -bag_volume(x["Dims"])
            )
        )
        strategies["Tunnel Priority Ordering"] = tunnel_priority

    if container_type == "CJ":
        variant_settings = [
            {
                "force_tunnel_for_long": True,
                "long_threshold": 50,
                "allow_tunnel_for_short": False,
                "label": "Prefer tunnel ≥50\""
            },
            {
                "force_tunnel_for_long": True,
                "long_threshold": 60,
                "allow_tunnel_for_short": False,
                "label": "Prefer tunnel ≥60\""
            },
            {
                "force_tunnel_for_long": False,
                "long_threshold": 50,
                "allow_tunnel_for_short": True,
                "label": "Tunnel allowed all"
            },
        ]
    else:
        variant_settings = [
            {"force_tunnel_for_long": True, "long_threshold": 50, "allow_tunnel_for_short": False, "label": None}
        ]

    best_result = {"success": False, "placements": [], "strategy": None, "fit_count": 0}

    for name, bags in strategies.items():
        for variant in variant_settings:
            success, placements = greedy_3d_packing(
                bags,
                container_type,
                interior,
                force_tunnel_for_long=variant["force_tunnel_for_long"],
                long_threshold=variant["long_threshold"],
                allow_tunnel_for_short=variant["allow_tunnel_for_short"]
            )
            strategy_label = name
            if container_type == "CJ" and variant["label"]:
                strategy_label = f"{name} / {variant['label']}"
            if success:
                return {
                    "success": True,
                    "placements": placements,
                    "strategy": strategy_label,
                    "fit_count": len(placements)
                }
            if len(placements) > best_result["fit_count"]:
                best_result = {
                    "success": False,
                    "placements": placements,
                    "strategy": strategy_label,
                    "fit_count": len(placements)
                }

    return best_result

# ============================================================
# Visualization
# ============================================================
def plot_cargo(cargo_dims, placements, container_type=None, interior=None):
    cargo_L, cargo_W, cargo_H = cargo_dims
    fig = go.Figure()

    # ---- CJ: main hold wireframe + restricted block + tunnel ----
    if container_type == "CJ":
        # Main hold wireframe (lines only, no legend spam)
        corners = [
            (0,0,0), (cargo_L,0,0), (cargo_L,cargo_W,0), (0,cargo_W,0),
            (0,0,cargo_H), (cargo_L,0,cargo_H), (cargo_L,cargo_W,cargo_H), (0,cargo_W,cargo_H)
        ]
        edges = [(0,1),(1,2),(2,3),(3,0),(4,5),(5,6),(6,7),(7,4),(0,4),(1,5),(2,6),(3,7)]
        for e in edges:
            x = [corners[e[0]][0], corners[e[1]][0]]
            y = [corners[e[0]][1], corners[e[1]][1]]
            z = [corners[e[0]][2], corners[e[1]][2]]
            fig.add_trace(go.Scatter3d(x=x, y=y, z=z, mode='lines',
                                       line=dict(color='black', width=3),
                                       showlegend=False))

        # Restricted block (near door, right side)
        r = interior["restricted"]
        x0, y0, z0 = 0, cargo_W - r["width"], 0
        x1, y1, z1 = r["depth"], cargo_W, cargo_H
        vertices = [
            [x0, y0, z0], [x1, y0, z0], [x1, y1, z0], [x0, y1, z0],
            [x0, y0, z1], [x1, y0, z1], [x1, y1, z1], [x0, y1, z1]
        ]
        x, y, z = zip(*vertices)
        faces = [(0,1,2),(0,2,3),(4,5,6),(4,6,7),
                 (0,1,5),(0,5,4),(1,2,6),(1,6,5),
                 (2,3,7),(2,7,6),(3,0,4),(3,4,7)]
        i, j, k = zip(*faces)
        fig.add_trace(go.Mesh3d(x=x, y=y, z=z, i=i, j=j, k=k,
                                color='gray', opacity=0.35,
                                name='Restricted Area'))

        # Tunnel: shallow box at the back wall spanning the width
        t_depth = interior["tunnel"]["depth"]
        t_width = interior["tunnel"]["width"]
        t_x0 = cargo_L - t_depth
        t_y0 = 0
        vertices = [
            [t_x0,         t_y0,          0],
            [t_x0+t_depth, t_y0,          0],
            [t_x0+t_depth, t_y0+t_width,  0],
            [t_x0,         t_y0+t_width,  0],
            [t_x0,         t_y0,          cargo_H],
            [t_x0+t_depth, t_y0,          cargo_H],
            [t_x0+t_depth, t_y0+t_width,  cargo_H],
            [t_x0,         t_y0+t_width,  cargo_H]
        ]
        x, y, z = zip(*vertices)
        faces = [(0,1,2),(0,2,3),(4,5,6),(4,6,7),
                 (0,1,5),(0,5,4),(1,2,6),(1,6,5),
                 (2,3,7),(2,7,6),(3,0,4),(3,4,7)]
        i, j, k = zip(*faces)
        fig.add_trace(go.Mesh3d(x=x, y=y, z=z, i=i, j=j, k=k,
                                color='rgba(0,0,0,0)', opacity=0.0,
                                name='Long Tunnel'))
        edges = [(0,1),(1,2),(2,3),(3,0),(4,5),(5,6),(6,7),(7,4),(0,4),(1,5),(2,6),(3,7)]
        for e in edges:
            fig.add_trace(go.Scatter3d(
                x=[x[e[0]], x[e[1]]],
                y=[y[e[0]], y[e[1]]],
                z=[z[e[0]], z[e[1]]],
                mode='lines',
                line=dict(color='rgba(0,0,0,0.4)', width=2),
                showlegend=False
            ))

    # ---- Legacy: tapered hold ----
    if container_type == "Legacy":
        d = interior["depth"]
        wmin, wmax = interior["width_min"], interior["width_max"]
        h = interior["height"]
        vertices = [
            [0,0,0],[d,0,0],[d,wmin,0],[0,wmin,0],
            [0,0,h],[d,0,h],[d,wmax,h],[0,wmax,h]
        ]
        x, y, z = zip(*vertices)
        faces = [(0,1,2),(0,2,3),(4,5,6),(4,6,7),
                 (0,1,5),(0,5,4),(1,2,6),(1,6,5),
                 (2,3,7),(2,7,6),(3,0,4),(3,4,7)]
        i, j, k = zip(*faces)
        fig.add_trace(go.Mesh3d(x=x, y=y, z=z, i=i, j=j, k=k,
                                color='lightblue', opacity=0.15,
                                name='Legacy Hold'))

    # ---- Baggage meshes ----
    colors = ['red','green','blue','orange','purple','cyan','magenta','yellow','lime','pink']
    for idx, item in enumerate(placements):
        l, w, h = item["Dims"]  # already oriented if in Tunnel
        x0, y0, z0 = item["Position"]
        color = colors[idx % len(colors)]
        x = [x0, x0+l, x0+l, x0, x0, x0+l, x0+l, x0]
        y = [y0, y0, y0+w, y0+w, y0, y0, y0+w, y0+w]
        z = [z0, z0, z0, z0, z0+h, z0+h, z0+h, z0+h]
        fig.add_trace(go.Mesh3d(x=x, y=y, z=z,
                                color=color, opacity=0.5,
                                flatshading=True,
                                lighting=dict(ambient=0.4, diffuse=0.6, specular=0.2, roughness=0.9),
                                name=f"{item['Type']} ({item.get('Section','Main')})"))
        edges = [(0,1),(1,2),(2,3),(3,0),(4,5),(5,6),(6,7),(7,4),(0,4),(1,5),(2,6),(3,7)]
        for e in edges:
            fig.add_trace(go.Scatter3d(
                x=[x[e[0]], x[e[1]]],
                y=[y[e[0]], y[e[1]]],
                z=[z[e[0]], z[e[1]]],
                mode='lines',
                line=dict(color='rgba(0,0,0,0.6)', width=2),
                showlegend=False
            ))

    fig.update_layout(
        scene=dict(
            xaxis_title='Depth (in)',
            yaxis_title='Width (in)',
            zaxis_title='Height (in)',
            aspectmode='data'
        ),
        margin=dict(l=0, r=0, b=0, t=0)
    )
    return fig

# ============================================================
# Streamlit UI
# ============================================================
# State init
if "baggage_list" not in st.session_state:
    st.session_state["baggage_list"] = []

# Controls
colA, colB = st.columns([1, 1])
with colA:
    container_choice = st.selectbox("Select Aircraft Cargo Hold", list(containers.keys()))
with colB:
    if st.button("Clear Baggage List"):
        st.session_state["baggage_list"] = []
        st.success("✅ Baggage list cleared.")

container = containers[container_choice]
allow_diagonal_fit = st.checkbox(
    "Allow diagonal floor placement for single-item fit checks",
    value=False,
    help=(
        "Uses a diagonal/angled placement check for individual items (CJ and Legacy). "
        "Packing/visualization remains axis-aligned."
    )
)

st.write("### Add Baggage")
col1, col2, col3, col4 = st.columns([1,1,1,1])
with col1:
    baggage_type = st.selectbox("Baggage Type", list(standard_baggage.keys()))
with col2:
    if baggage_type == "Custom":
        length = st.number_input("Length (in)", min_value=1)
        width  = st.number_input("Width (in)",  min_value=1)
        height = st.number_input("Height (in)", min_value=1)
        dims = (length, width, height)
        flex = 1.0
    else:
        dims = standard_baggage[baggage_type]["dims"]
        flex = standard_baggage[baggage_type]["flex"]
with col3:
    qty = st.number_input("Quantity", min_value=1, value=1)
with col4:
    if st.button("Add Item"):
        if dims is None:
            st.warning("Please enter dimensions for custom item.")
        else:
            for _ in range(qty):
                st.session_state["baggage_list"].append({
                    "Type": baggage_type,
                    "Dims": dims,
                    "Flex": flex
                })
            st.success(f"Added {qty} × {baggage_type}")

# Current Load Table
if st.session_state["baggage_list"]:
    st.write("### Current Baggage Load")

    # Show each item with a remove button
    for idx, bag in enumerate(st.session_state["baggage_list"], start=1):
        col1, col2, col3, col4, col5 = st.columns([2, 3, 3, 3, 1])
        with col1:
            st.write(f"**{idx}**")
        with col2:
            st.write(bag["Type"])
        with col3:
            st.write(f"{bag['Dims'][0]} × {bag['Dims'][1]} × {bag['Dims'][2]}")
        with col4:
            st.write(f"Flex: {bag['Flex']}")
        with col5:
            if st.button("❌", key=f"remove_{idx}"):
                st.session_state["baggage_list"].pop(idx-1)
                st.rerun()  # refresh immediately



    # Fit checks + Packing
    if st.button("Check Fit / Pack"):
        # Per-item simple fit
        results = []
        door_fail_items = []
        diagonal_fit_items = []
        for i, item in enumerate(st.session_state["baggage_list"], 1):
            box_dims = item["Dims"]
            door_fit = fits_through_door(box_dims, container["door"], item.get("Flex", 1.0))
            interior_fit, used_diagonal = fits_inside(
                box_dims,
                container["interior"],
                container_choice,
                item.get("Flex", 1.0),
                allow_diagonal=allow_diagonal_fit
            )
            if door_fit and interior_fit:
                status = "✅ Fits (Diagonal)" if used_diagonal else "✅ Fits"
            else:
                status = "❌ Door Fail" if not door_fit else "❌ Interior Fail"
            if not door_fit:
                door_fail_items.append(i)
            if used_diagonal:
                diagonal_fit_items.append(i)
            results.append({"Type": item["Type"], "Dims": box_dims, "Result": status})

        results_df = pd.DataFrame(results).reset_index(drop=True)
        results_df.index = results_df.index + 1
        results_df.index.name = "Item"
        st.write("### Fit Results")
        st.table(results_df)

        if door_fail_items:
            fail_descriptions = [
                f"{idx} ({results[idx-1]['Type']})" for idx in door_fail_items
            ]
            fail_summary = ", ".join(fail_descriptions)
            st.error(
                "🚫 Door fit failed. Remove the following item(s) before finishing calculations: "
                f"{fail_summary}."
            )
            st.info("Door failures must be resolved before packing calculations can continue.")
            st.stop()
        if diagonal_fit_items:
            diagonal_descriptions = [
                f"{idx} ({results[idx-1]['Type']})" for idx in diagonal_fit_items
            ]
            diagonal_summary = ", ".join(diagonal_descriptions)
            st.info(
                "📐 Diagonal fit assumed for the following item(s): "
                f"{diagonal_summary}. Packing/visualization remains axis-aligned."
            )

        # Packing multi-strategy
        result = multi_strategy_packing(
            st.session_state["baggage_list"], container_choice, container["interior"]
        )

        st.write("### Overall Cargo Packing Feasibility")
        if result["success"]:
            st.success(f"✅ Packing possible using **{result['strategy']}** strategy.")
        else:
            st.warning(
                f"⚠️ Full packing failed. Best strategy was **{result['strategy']}**, "
                f"which fit {result['fit_count']} out of {len(st.session_state['baggage_list'])} items."
            )

        placements = result["placements"]

        if placements:
            # Human-friendly placements table
            nice_rows = []
            for p in placements:
                (x0,y0,z0) = p["Position"]
                (lx,ly,lz) = p["Dims"]
                nice_rows.append({
                    "Item": p["Item"],
                    "Type": p["Type"],
                    "Section": p.get("Section", "Main"),
                    "Dims (x,y,z)": f"{lx:.1f}×{ly:.1f}×{lz:.1f}",
                    "Position (x,y,z)": f"{x0:.1f}, {y0:.1f}, {z0:.1f}"
                })
            placements_df = pd.DataFrame(nice_rows).reset_index(drop=True)
            placements_df.index = placements_df.index + 1
            placements_df.index.name = "Placed"
            st.write("### Suggested Placement Positions (oriented)")
            st.table(placements_df)

            # Utilization
            total_bag_vol = sum(bag_volume(item["Dims"]) for item in st.session_state["baggage_list"])
            hold_vol = cargo_volume(container["interior"], container_choice)
            utilization = (total_bag_vol / hold_vol) * 100 if hold_vol > 0 else 0.0
            st.info(f"📦 Estimated Volume Utilization: {utilization:.1f}% (bags / usable hold volume)")

            # Visualization
            st.write("### Cargo Load Visualization")
            if container_choice == "CJ":
                cargo_dims = (container["interior"]["depth"],
                              container["interior"]["width"],
                              container["interior"]["height"])
            else:
                cargo_dims = (container["interior"]["depth"],
                              container["interior"]["width_max"],
                              container["interior"]["height"])
            fig = plot_cargo(cargo_dims, placements, container_choice, container["interior"])
            st.plotly_chart(fig, width="stretch")

            # Debug expander (optional)
            with st.expander("🔎 Debug data (raw placements)"):
                st.json(placements)
